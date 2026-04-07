package kvsrv

import (
	"log"
	"sync"

	"6.5840/tester1"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/chr"
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

	id string
	kv map[string]string
	versions map[string]rpc.Tversion // version of the key

	ring *chr.ConsistentHashRing
	writeQuorum int
	readQuorum int
	ends map[string]*labrpc.ClientEnd
}

func MakeKVServer(serverID string, ring *chr.ConsistentHashRing, writeQuorum int, readQuorum int) *KVServer {
	kv := &KVServer{id: serverID, 
		kv: make(map[string]string), 
		versions: make(map[string]rpc.Tversion),
		ring: ring,
		writeQuorum: writeQuorum,
		readQuorum: readQuorum,
		ends: make(map[string]*labrpc.ClientEnd)
	}
	return kv
}


// Read operation
//
// The get(key) operation locates the object replicas associated 
// with the key in the storage system and returns a single object 
// or a list of objects with conflicting versions along with a context.
//
// The context encodes system metadata about the object that is opaque to the caller 
// and includes information such as the version of the object.
// TODO: sort the final reply by vector clock (a list of (node, counter) pairs)
// TODO: handle the case where the read quorum is not met
// TODO: implement context
func (kv *KVServer) CoordGet(args *rpc.GetArgs, reply *rpc.GetReply) {
	// check if myself is the coordinator
	if kv.ring.GetCoordinator(args.Key) != kv.id {
		reply.Err = rpc.ErrNotCoordinator
		return
	}
	// get the value and version from the replicas
	prefList := kv.ring.GetPreferenceList(args.Key)
	ch := make(chan *rpc.GetReply, len(prefList))
	replies := make([]*rpc.GetReply, len(prefList))

	for _, serverID := range prefList {
		go func(serverID string) {
			args := &rpc.GetArgs{Key: args.Key}
			reply := &rpc.GetReply{}
			ok := kv.ends[serverID].Call("KVServer.ReplicaGet", args, reply)
			for !ok {
				ok = kv.ends[serverID].Call("KVServer.ReplicaGet", args, reply) // retry
			}
			ch <- reply
		}(serverID)
	}

	successCount := 0
	for i := 0; i < len(prefList); i++ { // TODO: check if this is correct wait for N replies
		replies[i] = <- ch
		if replies[i].Err == rpc.OK {
			successCount++
			if replies[i].Version > reply.Version {
				reply.Value = replies[i].Value
				reply.Version = replies[i].Version
			}
		} else {
			reply.Err = replies[i].Err
		}
	}
	if successCount >= kv.readQuorum {
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrReadQuorumNotMet // TODO: figure out the best error message
	}
}

// Write operation
//
// The put(key, object, context) operation determines where the replicas of 
// the object should be placed based on the associated key, and writes the replicas to disk.
// TODO: handle the case where the write quorum is not met
func (kv *KVServer) CoordPut(args *rpc.PutArgs, reply *rpc.PutReply) {
	// check if myself is the coordinator
	if kv.ring.GetCoordinator(args.Key) != kv.id {
		reply.Err = rpc.ErrNotCoordinator
		return
	}
	// get the preference list
	prefList := kv.ring.GetPreferenceList(args.Key)
	ch := make(chan *rpc.PutReply, len(prefList))
	replies := make([]*rpc.PutReply, len(prefList))

	for _, serverID := range prefList {
		go func(serverID string) {
			args := &rpc.PutArgs{Key: args.Key, Value: args.Value, Version: args.Version}
			reply := &rpc.PutReply{}
			ok := kv.ends[serverID].Call("KVServer.ReplicaPut", args, reply)
			for !ok {
				ok = kv.ends[serverID].Call("KVServer.ReplicaPut", args, reply) // retry
			}
			ch <- reply
		}(serverID)
	}

	successCount := 0
	for i := 0; i < len(prefList); i++ { // TODO: check if this is correct wait for N replies
		replies[i] = <- ch
		if replies[i].Err == rpc.OK {
			successCount++
		}
	}
	if successCount >= kv.writeQuorum {
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrWriteQuorumNotMet // TODO: figure out the best error message
	}
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) ReplicaGet(args *rpc.GetArgs, reply *rpc.GetReply) {
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
func (kv *KVServer) ReplicaPut(args *rpc.PutArgs, reply *rpc.PutReply) {
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

// // for replicated KVservers
// func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
// 	kv := MakeKVServer(tester.ServerName(gid, srv), tc.Ring, WriteQuorum, ReadQuorum)
// 	return []any{kv}
// }
