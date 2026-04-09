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

var maxRetry = 3

func (ck *Clerk) Get(key string) (string, rpc.Context, rpc.Err) {
	// return ck.CoordGet(key)
	siblings, err := ck.CoordGet(key)

	if siblings == nil {
		return "", rpc.NewContext(), err
	} else if len(siblings) == 1 {
		return siblings[0].Value, siblings[0].Context, err
	} else {
		obj := clientBasedResolution(siblings) // TODO: implement client-based resolution
		return obj.Value, obj.Context, err
	}
}

func (ck *Clerk) Put(key, value string, context rpc.Context) rpc.Err {
	return ck.CoordPut(key, value, context)
}

func (ck *Clerk) CoordGet(key string) ([]rpc.Object, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	ok := false
	for retry := 0; retry < maxRetry; retry++ {
		reply := rpc.GetReply{}
		coordinator := ck.ring.GetCoordinator(key)
		ok = ck.clnt.Call(coordinator, "KVServer.CoordGet", &args, &reply)
		if ok && reply.Err != rpc.ErrNotCoordinator {
			if len(reply.Objects) == 0 {
				return nil, rpc.ErrNoKey
			}
			return reply.Objects, rpc.OK
		}
	}
	if !ok {
		return nil, rpc.ErrRPCFailure
	}
	return nil, rpc.ErrNotCoordinator
}


func (ck *Clerk) CoordPut(key, value string, context rpc.Context) rpc.Err {
	args := rpc.PutArgs{Key: key, Object: rpc.Object{Value: value, Context: context}, BaseContext: context}

	hadRPCFailure := false
	ok := false
	for retry := 0; retry < maxRetry; retry++ {
		reply := rpc.PutReply{}
		coordinator := ck.ring.GetCoordinator(key)
		ok = ck.clnt.Call(coordinator, "KVServer.CoordPut", &args, &reply) // retry
		if !ok {
			hadRPCFailure = true
		}
		if ok && reply.Err != rpc.ErrNotCoordinator {
			if reply.Err == rpc.ErrVersion && hadRPCFailure {
				return rpc.ErrMaybe
			}
			return reply.Err // return OK or ErrNokey
		}
	}
	if !ok {
		return rpc.ErrMaybe
	}
	return rpc.ErrNotCoordinator
}

func clientBasedResolution(siblings []rpc.Object) rpc.Object {
	return pickLatestObject(siblings) // TODO: implement client-based resolution
}

func pickLatestObject(siblings []rpc.Object) rpc.Object {
	latest := siblings[0]
	for _, sibling := range siblings[1:] {
		cmp := sibling.Context.Compare(latest.Context)
		if cmp == vclock.After {
			latest = sibling
			continue
		}
		// If causally concurrent/equal, resolve by latest timestamp.
		if (cmp == vclock.Concurrent || cmp == vclock.Equal) && sibling.Context.Timestamp > latest.Context.Timestamp {
			latest = sibling
		}
	}
	return latest
}
