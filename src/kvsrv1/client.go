package kvsrv

import (
	"6.5840/chr"
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	"6.5840/tester1"
	"6.5840/vclock"
)

type Clerk struct {
	clnt *tester.Clnt
	ring *chr.ConsistentHashRing
}

func MakeClerk(clnt *tester.Clnt, ring *chr.ConsistentHashRing) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt,
		ring: ring,
	}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Context, rpc.Err) {
	return ck.CoordGet(key)
}

func (ck *Clerk) Put(key, value string, context rpc.Context) rpc.Err {
	return ck.CoordPut(key, value, context)
}

func (ck *Clerk) CoordGet(key string) (string, rpc.Context, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	const maxRetry = 3
	ok := false
	for retry := 0; retry < maxRetry; retry++ {
		coordinator := ck.ring.GetCoordinator(key)
		ok = ck.clnt.Call(coordinator, "KVServer.CoordGet", &args, &reply) // retry
		if ok && reply.Err != rpc.ErrNotCoordinator {
			break
		}
	}
	if !ok {
		return "", rpc.ZeroContext(), rpc.ErrRPCFailure
	}
	if reply.Err != rpc.OK {
		return "", rpc.ZeroContext(), reply.Err // return corresponding error
	}
	if len(reply.Objects) == 0 {
		return "", rpc.ZeroContext(), rpc.ErrNoKey
	}
	latest := pickLatestObject(reply.Objects)
	return latest.Value, latest.Context, reply.Err // single-value compatibility for kvtest
}


func (ck *Clerk) CoordPut(key, value string, context rpc.Context) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Context: context}
	reply := rpc.PutReply{}

	const maxRetry = 3
	hadRPCFailure := false
	ok := false
	for retry := 0; retry < maxRetry; retry++ {
		coordinator := ck.ring.GetCoordinator(key)
		ok = ck.clnt.Call(coordinator, "KVServer.CoordPut", &args, &reply) // retry
		if !ok {
			hadRPCFailure = true
		}
		if ok && reply.Err != rpc.ErrNotCoordinator {
			break
		}
	}
	if !ok {
		return rpc.ErrMaybe
	}
	if reply.Err == rpc.ErrVersion && hadRPCFailure {
		return rpc.ErrMaybe
	}

	return reply.Err // return OK or ErrNokey
}

func pickLatestObject(objects []rpc.Object) rpc.Object {
	best := objects[0]
	for _, obj := range objects[1:] {
		cmp := obj.Context.Compare(best.Context)
		if cmp == vclock.After {
			best = obj
			continue
		}
		// If causally concurrent/equal, resolve by latest timestamp.
		if (cmp == vclock.Concurrent || cmp == vclock.Equal) && obj.Context.Timestamp > best.Context.Timestamp {
			best = obj
		}
	}
	return best
}
