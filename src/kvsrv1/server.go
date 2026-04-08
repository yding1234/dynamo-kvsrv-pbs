package kvsrv

import (
	"log"
	"sync"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	coordMu sync.Mutex

	id       string
	kv       map[string]string
	versions map[string]rpc.Tversion // version of the key

	// for consistent hashing
	ring        *chr.ConsistentHashRing
	writeQuorum int
	readQuorum  int

	// for forwarding requests to the replicas
	ends map[string]*labrpc.ClientEnd
}

func MakeKVServer(serverID string, ring *chr.ConsistentHashRing,
	writeQuorum int, readQuorum int, ends map[string]*labrpc.ClientEnd) *KVServer {
	kv := &KVServer{id: serverID,
		kv:          make(map[string]string),
		versions:    make(map[string]rpc.Tversion),
		ring:        ring,
		writeQuorum: writeQuorum,
		readQuorum:  readQuorum,
		ends:        ends,
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
	type getResult struct {
		ok    bool
		reply rpc.GetReply
	}

	// Query replicas in the preference list and apply read-quorum semantics.
	prefList := kv.ring.GetPreferenceList(args.Key)
	ch := make(chan getResult, len(prefList))

	for _, serverID := range prefList {
		go func(serverID string) {
			repArgs := &rpc.GetArgs{Key: args.Key}
			repReply := rpc.GetReply{}
			ok := kv.ends[serverID].Call("KVServer.ReplicaGet", repArgs, &repReply)
			ch <- getResult{ok: ok, reply: repReply}
		}(serverID)
	}

	okCount := 0
	noKeyCount := 0
	for i := 0; i < len(prefList); i++ {
		res := <-ch
		if !res.ok {
			continue
		}
		if res.reply.Err == rpc.OK {
			okCount++
			if reply.Err != rpc.OK || res.reply.Version > reply.Version {
				reply.Value = res.reply.Value
				reply.Version = res.reply.Version
			}
		} else if res.reply.Err == rpc.ErrNoKey {
			noKeyCount++
		}
	}
	if okCount >= kv.readQuorum {
		reply.Err = rpc.OK
	} else if noKeyCount >= kv.readQuorum {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Err = rpc.ErrReadQuorumNotMet
	}
}

// Write operation
//
// The put(key, object, context) operation determines where the replicas of
// the object should be placed based on the associated key, and writes the replicas to disk.
// TODO: handle the case where the write quorum is not met
func (kv *KVServer) CoordPut(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Serialize coordinator writes so concurrent puts with the same expected
	// version don't interleave across replicas and break linearizability.
	kv.coordMu.Lock()
	defer kv.coordMu.Unlock()

	// check if myself is the coordinator
	if kv.ring.GetCoordinator(args.Key) != kv.id {
		reply.Err = rpc.ErrNotCoordinator
		return
	}
	type putResult struct {
		ok  bool
		err rpc.Err
	}

	prefList := kv.ring.GetPreferenceList(args.Key)
	ch := make(chan putResult, len(prefList))

	for _, serverID := range prefList {
		go func(serverID string) {
			repArgs := &rpc.PutArgs{Key: args.Key, Value: args.Value, Version: args.Version}
			repReply := rpc.PutReply{}
			ok := kv.ends[serverID].Call("KVServer.ReplicaPut", repArgs, &repReply)
			if !ok {
				ch <- putResult{ok: false}
				return
			}
			ch <- putResult{ok: true, err: repReply.Err}
		}(serverID)
	}

	successCount := 0
	versionErrCount := 0
	noKeyErrCount := 0
	for i := 0; i < len(prefList); i++ {
		res := <-ch
		if !res.ok {
			continue
		}
		if res.err == rpc.OK {
			successCount++
		} else if res.err == rpc.ErrVersion {
			versionErrCount++
		} else if res.err == rpc.ErrNoKey {
			noKeyErrCount++
		}
	}

	// Dynamo semantics: not enough ACKs means client-visible write failure,
	// while partial replica writes are still possible and intentionally kept.
	if successCount >= kv.writeQuorum {
		reply.Err = rpc.OK
	} else if successCount == 0 && versionErrCount > 0 {
		reply.Err = rpc.ErrVersion
	} else if successCount == 0 && noKeyErrCount > 0 {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Err = rpc.ErrWriteQuorumNotMet
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

// StartKVServer matches tester.FstartServer. Ring and R/W quorum use the same package-level
// parameters as test.go (numSectors, numReplicas, readQuorum, writeQuorum) and len(ends) for cluster size.
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd,
	gid tester.Tgid, srv int, persister *tester.Persister) []any {
	_ = tc
	_ = persister

	endsMap := make(map[string]*labrpc.ClientEnd, len(ends))
	nodeIDs := make([]string, len(ends))
	for i := 0; i < len(ends); i++ {
		name := tester.ServerName(gid, i)
		endsMap[name] = ends[i]
		nodeIDs[i] = name
	}
	ring := chr.MakeConsistentHashRing(numReplicas, numSectors, len(ends), nodeIDs)
	kv := MakeKVServer(tester.ServerName(gid, srv), ring, writeQuorum, readQuorum, endsMap)
	return []any{kv}
}
