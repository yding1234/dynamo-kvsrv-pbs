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
	ck := &Clerk{clnt: clnt, ring: ring}
	// You may add code here.
	return ck
}


func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	serverID := ck.ring.FindCoordinator(key)
	ok := ck.clnt.Call(serverID, "KVServer.Get", &args, &reply) // first try

	for !ok || (ok && reply.Err != rpc.OK) {
		if ok && reply.Err == rpc.ErrNoKey {
			return "", 0, reply.Err
		}
		ok = ck.clnt.Call(serverID, "KVServer.Get", &args, &reply) // retry
	}

	return reply.Value, reply.Version, reply.Err // success, return OK
}


func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	serverID := ck.ring.FindCoordinator(key)
	resent := false
	ok := ck.clnt.Call(serverID, "KVServer.Put", &args, &reply) // first try

	for !ok || (ok && reply.Err == rpc.ErrVersion){
		if ok && !resent {return reply.Err}
		if ok && resent {return rpc.ErrMaybe}
		resent = true
		ok = ck.clnt.Call(serverID, "KVServer.Put", &args, &reply) // retry
	}
	return reply.Err
}
