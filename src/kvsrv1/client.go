package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


type Clerk struct {
	clnt   *tester.Clnt
	server string // server name for current operation
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply) // first try

	for !ok || (ok && reply.Err != rpc.OK) {
		if reply.Err == rpc.ErrNoKey {
			return "", 0, reply.Err
		}
		ok = ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply) // retry
	}

	return reply.Value, reply.Version, reply.Err // success, return OK
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	resent := false
	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply) // first try

	for !ok || (ok && reply.Err == rpc.ErrVersion){
		if ok && !resent {return reply.Err}
		if ok && resent {return rpc.ErrMaybe}
		resent = true
		ok = ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply) // retry
	}
	return reply.Err
}
