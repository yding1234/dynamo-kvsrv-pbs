package kvsrv

import (
	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
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

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	return ck.CoordGet(key)
}

func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	return ck.CoordPut(key, value, version)
}

func (ck *Clerk) CoordGet(key string) (string, rpc.Tversion, rpc.Err) {
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
		return "", 0, rpc.ErrReadQuorumNotMet
	}
	if reply.Err == rpc.ErrNoKey {
		return "", 0, reply.Err
	}

	return reply.Value, reply.Version, reply.Err // success, return OK
}

func (ck *Clerk) CoordPut(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
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
