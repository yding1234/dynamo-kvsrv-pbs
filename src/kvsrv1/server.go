package kvsrv

import (
	"log"
	"sync"

	"6.5840/tester1"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	id string
	kv map[string]string
	versions map[string]rpc.Tversion // version of the key
}

func MakeKVServer() *KVServer {
	kv := &KVServer{id: tester.Randstring(20), 
		kv: make(map[string]string), 
		versions: make(map[string]rpc.Tversion),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.kv[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = value
	reply.Version = kv.versions[args.Key]
	reply.Err = rpc.OK
	return
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.kv[args.Key]
	//if the key doesn't exist and the version is not 0, return ErrNoKey
	if !ok && args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		return
	}
	// if the key exists and the version doesn't match, return ErrVersion
	if ok && args.Version != kv.versions[args.Key] {
		reply.Err = rpc.ErrVersion
		return
	}
	// otherwise, install the value
	kv.kv[args.Key] = args.Value
	kv.versions[args.Key] = args.Version + 1
	reply.Err = rpc.OK
	return
}



// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
