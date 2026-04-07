package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"6.5840/chr"
)


type Clerk struct {
	clnt   *tester.Clnt
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
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	coordinator := ck.ring.GetCoordinator(key)
	ok := ck.clnt.Call(coordinator, "KVServer.CoordGet", &args, &reply) // first try

	// for !ok || (ok && reply.Err != rpc.OK) {
	// 	if ok && reply.Err == rpc.ErrNoKey {
	// 		return "", 0, reply.Err
	// 	}
	// 	ok = ck.clnt.Call(coordinator, "KVServer.CoordGet", &args, &reply) // retry
	// }
	for !ok || (ok && reply.Err == rpc.ErrNotCoordinator) {
		coordinator := ck.ring.GetCoordinator(key)
		ok = ck.clnt.Call(coordinator, "KVServer.CoordGet", &args, &reply) // retry
	}
	if reply.Err == rpc.ErrNoKey {
		return "", 0, reply.Err
	}

	return reply.Value, reply.Version, reply.Err // success, return OK
}


func (ck *Clerk) CoordPut(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	resent := false
	coordinator := ck.ring.GetCoordinator(key)
	ok := ck.clnt.Call(coordinator, "KVServer.CoordPut", &args, &reply) // first try

	// for !ok || (ok && reply.Err == rpc.ErrVersion){
	// 	if ok && !resent {return reply.Err}
	// 	if ok && resent {return rpc.ErrMaybe}
	// 	resent = true
	// 	ok = ck.clnt.Call(coordinator, "KVServer.CoordPut", &args, &reply) // retry
	// }

	for !ok || (ok && reply.Err == rpc.ErrNotCoordinator) {
		coordinator := ck.ring.GetCoordinator(key)
		ok = ck.clnt.Call(coordinator, "KVServer.CoordPut", &args, &reply) // retry
	}
	if reply.Err == rpc.ErrVersion {
		if !resent {
			return reply.Err
		}
		return rpc.ErrMaybe
	}

	return reply.Err // return OK or ErrNokey
}

