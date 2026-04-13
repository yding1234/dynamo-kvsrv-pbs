package kvsrv

import (
	"log"
	"sync"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
	"time"
)

const Debug = false

var defaultAntiEntropyInterval = 500 * time.Millisecond

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
	kv       map[string][]rpc.Object // key -> list of objects

	// for consistent hashing
	ring        *chr.ConsistentHashRing
	writeQuorum int
	readQuorum  int

	// for forwarding requests to the replicas
	ends map[string]*labrpc.ClientEnd

	// for anti-entropy
	// merkleRoots map[int]rpc.TreeSummary // sector ID -> merkle tree summary
	merkleRoots map[int]*MerkleNode // sector ID -> merkle root
	keysInBuckets [][][]string // sector ID -> bucket ID -> keys
	antiEntropyInterval time.Duration
	stopCh chan struct{}

	// membership
}

func MakeKVServer(serverID string, ring *chr.ConsistentHashRing,
	writeQuorum int, readQuorum int, ends map[string]*labrpc.ClientEnd) *KVServer {
	kv := &KVServer{id: serverID,
		kv:          make(map[string][]rpc.Object),
		ring:        ring,
		writeQuorum: writeQuorum,
		readQuorum:  readQuorum,
		ends:        ends,
		merkleRoots: make(map[int]*MerkleNode, len(ring.GetSectors(serverID))),
		keysInBuckets: make([][][]string, ring.NumSectors()),
		antiEntropyInterval: defaultAntiEntropyInterval,
		stopCh: make(chan struct{}),
	}

	for i := 0; i < ring.NumSectors(); i++ {
		kv.keysInBuckets[i] = make([][]string, ring.BucketsPerSector())
		for j := 0; j < ring.BucketsPerSector(); j++ {
			kv.keysInBuckets[i][j] = make([]string, 0)
		}
	}
	for _, sector := range ring.GetSectors(serverID) {
		kv.merkleRoots[sector] = kv.BuildMerkleTree(sector)
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
func (kv *KVServer) CoordGet(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.coordMu.Lock()
	defer kv.coordMu.Unlock()

	// check if myself is the coordinator
	if kv.ring.GetCoordinator(args.Key) != kv.id {
		reply.Err = rpc.ErrNotCoordinator
		return
	}

	// forward the get request to the replicas

	prefList := kv.ring.GetPreferenceList(args.Key)
	ch := make(chan rpc.ForwardGetResult, len(prefList))

	for _, serverID := range prefList {
		go func(serverID string) {
			forwardArgs := rpc.GetArgs{Key: args.Key}
			forwardReply := rpc.GetReply{}
			ok := kv.ends[serverID].Call("KVServer.ReplicaGet", &forwardArgs, &forwardReply)
			ch <- rpc.ForwardGetResult{ServerID: serverID, OK: ok, Reply: forwardReply}
		}(serverID)
	}

	successCount := 0
	noKeyCount := 0
	siblings := make([]rpc.Object, 0)
	results := make([]rpc.ForwardGetResult, len(prefList))

	for i := 0; i < len(prefList); i++ {
		results[i] = <-ch
		if !results[i].OK {
			continue
		}
		if results[i].Reply.Err == rpc.OK {
			successCount++
			for _, obj := range results[i].Reply.Objects {
				if obj.CanBeAddedTo(siblings) {
					siblings = rpc.AddObject(siblings, obj, nil) // nil means no specify sort function
				}
			}
		} else if results[i].Reply.Err == rpc.ErrNoKey {
			noKeyCount++
		}
	}

	if successCount >= kv.readQuorum {
		// read repair
		canonicalSiblings := rpc.CopyObjects(siblings)
		staleReplicas := findStaleReplicas(canonicalSiblings, results)
		key := args.Key
		go kv.repairReplicas(key, canonicalSiblings, staleReplicas)

		// return the siblings
		reply.Objects = siblings
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

	// forward the put request to the replicas
	prefList := kv.ring.GetPreferenceList(args.Key)
	ch := make(chan rpc.ForwardPutResult, len(prefList))

	writeObject := args.Object
	writeObject.Context = args.BaseContext
	writeObject.Context.Update(kv.id, writeObject.Value)
	
	for _, serverID := range prefList {
		go func(serverID string) {
			forwardArgs := rpc.PutArgs{Key: args.Key, Object: writeObject, BaseContext: args.BaseContext}
			forwardReply := rpc.PutReply{}
			ok := kv.ends[serverID].Call("KVServer.ReplicaPut", &forwardArgs, &forwardReply)
			if !ok {
				ch <- rpc.ForwardPutResult{OK: false}
				return
			}
			ch <- rpc.ForwardPutResult{OK: true, Err: forwardReply.Err}
		}(serverID)
	}

	// check the results from the replicas
	successCount := 0
	versionErrCount := 0
	noKeyCount := 0
	for i := 0; i < len(prefList); i++ {
		res := <-ch
		if !res.OK {
			continue
		}
		if res.Err == rpc.OK {
			successCount++
			if successCount >= kv.writeQuorum {
				reply.Err = rpc.OK
				return
			}
		} else if res.Err == rpc.ErrVersion {
			versionErrCount++
		} else if res.Err == rpc.ErrNoKey {
			noKeyCount++
		}
	}

	if versionErrCount >= kv.writeQuorum {
		reply.Err = rpc.ErrVersion
	} else if noKeyCount >= kv.writeQuorum {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Err = rpc.ErrWriteQuorumNotMet
	}
}

// Get returns the value and context for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) ReplicaGet(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	siblings, ok := kv.kv[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Objects = siblings
	reply.Err = rpc.OK
	return
}


// Update the value for a key if args.Context matches the context of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Context is zero, and returns ErrNoKey otherwise.
func (kv *KVServer) ReplicaPut(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	siblings, ok := kv.kv[args.Key]
	// if the key doesn't exist and the context is not initial, return ErrNoKey
	if !ok && !args.BaseContext.IsInitial() {
		reply.Err = rpc.ErrNoKey
		return
	}

	baseObject := rpc.Object{Value: args.Object.Value, Context: args.BaseContext}
	canAdd := baseObject.CanBeAddedTo(siblings)
	if !canAdd {
		reply.Err = rpc.ErrVersion
		return
	}
	// otherwise, install the siblings
	kv.kv[args.Key] = rpc.AddObject(siblings, args.Object, nil) // nil means no specify sort function

	// If this is the first version for the key, add it to the bucket index too.
	if args.BaseContext.IsInitial() {
		sector, bucket := kv.ring.GetLocation(args.Key)
		kv.keysInBuckets[sector][bucket] = appendUniqueKey(kv.keysInBuckets[sector][bucket], args.Key)
	}
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
	kv.StartAntiEntropy() // start anti-entropy process
	return []any{kv}
}

func (kv *KVServer) CopyKV() map[string][]rpc.Object {
    kv.mu.Lock()

    kvCopy := make(map[string][]rpc.Object, len(kv.kv))
    for k, objs := range kv.kv {
        kvCopy[k] = rpc.CopyObjects(objs)
    }

    kv.mu.Unlock()
	return kvCopy
}

func (kv *KVServer) CopySectorKeys() map[int][]string {
    kv.mu.Lock()
	defer kv.mu.Unlock()

    sectorKeysCopy := make(map[int][]string, len(kv.keysInBuckets))
    for sectorID, buckets := range kv.keysInBuckets {
        keys := make([]string, 0)
        for _, bucketKeys := range buckets {
            keys = append(keys, bucketKeys...)
        }
        sectorKeysCopy[sectorID] = keys
    }

	return sectorKeysCopy
}

// get sectors from sector-keys map
func (kv *KVServer) CopyKeysInSector(sector int) [][]string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	keysInSector := make([][]string, len(kv.keysInBuckets[sector]))
	for i := 0; i < len(kv.keysInBuckets[sector]); i++ {
		keysInSector[i] = make([]string, len(kv.keysInBuckets[sector][i]))
		copy(keysInSector[i], kv.keysInBuckets[sector][i])
	}
	return keysInSector
}

// get the sectors which are responsible for this server
func (kv *KVServer) GetResponsibleSectors() []int {
	return kv.ring.GetSectors(kv.id)
}

func (kv *KVServer) CopyPreferenceList(key string) []string {
	kv.coordMu.Lock()
	defer kv.coordMu.Unlock()
	prefList := kv.ring.GetPreferenceList(key)
	prefListCopy := make([]string, len(prefList))
	copy(prefListCopy, prefList)
	return prefListCopy
}

func (kv *KVServer) GetClientEnd(serverID string) *labrpc.ClientEnd {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.ends[serverID]
}

func (kv *KVServer) GetKeysFromSector(sector int) []string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	keys := make([]string, 0)
	for _, bucketKeys := range kv.keysInBuckets[sector] {
		keys = append(keys, bucketKeys...)
	}
	return keys
}

func (kv *KVServer) GetSiblings(key string) []rpc.Object {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	siblings := rpc.CopyObjects(kv.kv[key])
	return siblings
}

func (kv *KVServer) GetKeysFromBucket(sector int, bucket int) []string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	keys := make([]string, len(kv.keysInBuckets[sector][bucket]))
	copy(keys, kv.keysInBuckets[sector][bucket])
	return keys
}

func (kv *KVServer) GetBucketsFromSector(sector int) []int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	buckets := make([]int, 0, len(kv.keysInBuckets[sector]))
	for i := 0; i < len(kv.keysInBuckets[sector]); i++ {
		if len(kv.keysInBuckets[sector][i]) > 0 {
			buckets = append(buckets, i)
		}
	}
	return buckets
}

func appendUniqueKey(keys []string, key string) []string {
	for _, existing := range keys {
		if existing == key {
			return keys
		}
	}
	return append(keys, key)
}

func removeKey(keys []string, key string) []string {
	for i, existing := range keys {
		if existing == key {
			return append(keys[:i], keys[i+1:]...)
		}
	}
	return keys
}

func (kv *KVServer) refreshMerkleTreeForSector(sector int) {
	newRoot := kv.BuildMerkleTree(sector)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.merkleRoots[sector] = newRoot
}

func (kv *KVServer) installObjects(key string, objects []rpc.Object) {
	sector, bucket := kv.ring.GetLocation(key)

	kv.mu.Lock()
	if len(objects) == 0 {
		delete(kv.kv, key)
		kv.keysInBuckets[sector][bucket] = removeKey(kv.keysInBuckets[sector][bucket], key)
		kv.mu.Unlock()
		kv.refreshMerkleTreeForSector(sector)
		return
	}

	_, existed := kv.kv[key]
	kv.kv[key] = rpc.CopyObjects(objects)
	if !existed {
		kv.keysInBuckets[sector][bucket] = appendUniqueKey(kv.keysInBuckets[sector][bucket], key)
	}
	kv.mu.Unlock()

	kv.refreshMerkleTreeForSector(sector)
}