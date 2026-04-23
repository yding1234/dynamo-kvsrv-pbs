package kvsrv

import (
	"log"
	"sync"
	"time"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvsrv_eval"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

var defaultAntiEntropyInterval = 500 * time.Millisecond
var defaultGossipInterval = 100 * time.Millisecond
var defaultHeartbeatTimeout = 100 * time.Millisecond
var defaultFailureTimeout = 500 * time.Millisecond
var defaultCleanupTimeout = 1500 * time.Millisecond
var defaultHintedHandoffInterval = 100 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	coordMu sync.Mutex

	id string
	kv map[string][]rpc.Object // key -> list of objects

	// consistent hashing
	ring        *chr.ConsistentHashRing
	writeQuorum int
	readQuorum  int

	// forwarding requests to the replicas
	ends map[string]*labrpc.ClientEnd

	// anti-entropy
	// merkleRoots map[int]rpc.TreeSummary // sector ID -> merkle tree summary
	merkleRoots         map[int]*MerkleNode // sector ID -> merkle root
	keysInBuckets       [][][]string        // sector ID -> bucket ID -> keys
	antiEntropyInterval time.Duration
	stopCh              chan struct{}

	// membership
	members           map[string]rpc.MemberInfo // server ID -> member info
	memberLastUpdated map[string]time.Time
	numNeighbors      int    // number of neighbors to gossip with
	heartbeatCounter  uint64 // heartbeat counter
	heartbeatTimeout  time.Duration
	gossipInterval    time.Duration
	failureTimeout    time.Duration // time to consider a node as suspect
	cleanupTimeout    time.Duration // time to consider a node as dead

	// hinted handoff
	hints                 map[string][]rpc.PutArgs // original target -> pending put requests
	hintedHandoffInterval time.Duration

	// tracing
	collector *kvsrv_eval.PBSCollector
}

func MakeKVServer(serverID string, ring *chr.ConsistentHashRing,
	writeQuorum int, readQuorum int, ends map[string]*labrpc.ClientEnd) *KVServer {
	kv := &KVServer{id: serverID,
		kv:                    make(map[string][]rpc.Object),
		ring:                  ring,
		writeQuorum:           writeQuorum,
		readQuorum:            readQuorum,
		ends:                  ends,
		merkleRoots:           make(map[int]*MerkleNode, len(ring.GetSectors(serverID))),
		keysInBuckets:         make([][][]string, ring.NumSectors()),
		antiEntropyInterval:   defaultAntiEntropyInterval,
		stopCh:                make(chan struct{}),
		members:               make(map[string]rpc.MemberInfo, len(ends)),
		memberLastUpdated:     make(map[string]time.Time, len(ends)),
		numNeighbors:          2,
		heartbeatTimeout:      defaultHeartbeatTimeout,
		gossipInterval:        defaultGossipInterval,
		failureTimeout:        defaultFailureTimeout,
		cleanupTimeout:        defaultCleanupTimeout,
		hints:                 make(map[string][]rpc.PutArgs),
		hintedHandoffInterval: defaultHintedHandoffInterval,
		collector:             kvsrv_eval.NewPBSCollector(),
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
	now := time.Now()
	for nodeID := range ends {
		kv.members[nodeID] = rpc.MemberInfo{
			ServerID:  nodeID,
			Heartbeat: 0,
			Status:    rpc.Alive,
		}
		kv.memberLastUpdated[nodeID] = now
	}
	if _, ok := kv.members[serverID]; !ok {
		kv.members[serverID] = rpc.MemberInfo{
			ServerID:  serverID,
			Heartbeat: 0,
			Status:    rpc.Alive,
		}
		kv.memberLastUpdated[serverID] = now
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
// TODO: handle the case where the read quorum is not met
func (kv *KVServer) CoordGet(args *rpc.GetArgs, reply *rpc.GetReply) {
	startedAt := time.Now()
	// kv.coordMu.Lock()
	// defer kv.coordMu.Unlock()

	// check if myself is the coordinator
	if kv.ring.GetCoordinator(args.Key) != kv.id {
		reply.Err = rpc.ErrNotCoordinator
		return
	}

	// forward the get request to the replicas
	prefList := kv.filterDeadMembers(kv.ring.GetPreferenceList(args.Key))
	if len(prefList) < kv.readQuorum {
		reply.Err = rpc.ErrReadQuorumNotMet
		return
	}
	ch := make(chan rpc.ForwardGetResult, len(prefList))

	for _, serverID := range prefList {
		go func(serverID string) {
			forwardArgs := rpc.GetArgs{Key: args.Key}
			forwardReply := rpc.GetReply{}

			sentAt := time.Now()
			ok := kv.ends[serverID].Call("KVServer.ReplicaGet", &forwardArgs, &forwardReply)
			receivedAt := time.Now()

			if ok && kv.collector != nil {
				arrivedAt := forwardReply.ArrivedAt
				respondedAt := forwardReply.RespondedAt
				_ = kv.collector.ObserveReadLatency(kvsrv_eval.NewMessageTrace(sentAt, arrivedAt, respondedAt, receivedAt))
			}

			ch <- rpc.ForwardGetResult{ServerID: serverID, OK: ok, Reply: forwardReply}
		}(serverID)
	}

	successCount := 0
	noKeyCount := 0
	siblings := make([]rpc.Object, 0)
	results := make([]rpc.ForwardGetResult, 0, len(prefList))

	for len(results) < len(prefList) {
		res := <-ch

		results = append(results, res)
		if !res.OK {
			continue
		}
		if res.Reply.Err == rpc.OK {
			successCount++
			for _, obj := range res.Reply.Objects {
				if obj.CanBeAddedTo(siblings) {
					siblings = rpc.AddObject(siblings, obj, nil) // nil means no specify sort function
				}
			}
			if successCount >= kv.readQuorum {
				canonicalSiblings := rpc.CopyObjects(siblings)
				collectedResults := append([]rpc.ForwardGetResult(nil), results...)
				remaining := len(prefList) - len(results)
				go kv.finishCoordGetReadRepair(args.Key, canonicalSiblings, collectedResults, ch, remaining)

				reply.Objects = rpc.CopyObjects(siblings)
				reply.Err = rpc.OK
				if kv.collector != nil {
					kv.collector.ObserveCompletedRead(kvsrv_eval.NewCompletedRead(args.Key, startedAt, time.Now(), rpc.CopyObjects(siblings)))
				}
				return
			}
		} else if res.Reply.Err == rpc.ErrNoKey {
			noKeyCount++
			if noKeyCount >= kv.readQuorum {
				collectedResults := append([]rpc.ForwardGetResult(nil), results...)
				remaining := len(prefList) - len(results)
				go kv.finishCoordGetReadRepair(args.Key, nil, collectedResults, ch, remaining)
				reply.Err = rpc.ErrNoKey
				return
			}
		}
	}

	reply.Err = rpc.ErrReadQuorumNotMet
}

// Write operation
//
// The put(key, object, context) operation determines where the replicas of
// the object should be placed based on the associated key, and writes the replicas to disk.
// TODO: handle the case where the write quorum is not met
func (kv *KVServer) CoordPut(args *rpc.PutArgs, reply *rpc.PutReply) {
	startedAt := time.Now()
	// kv.coordMu.Lock()
	// defer kv.coordMu.Unlock()

	// check if myself is the coordinator
	if kv.ring.GetCoordinator(args.Key) != kv.id {
		reply.Err = rpc.ErrNotCoordinator
		return
	}

	// update the context with the new node and value
	args.Object.Context.Update(kv.id, args.Object.Value)

	// get put plans
	prefList := kv.ring.GetPreferenceList(args.Key)

	type putPlan struct {
		targetServer  string
		handoffServer string // "" means no handoff is needed
	}

	usedServers := make(map[string]bool, len(prefList))
	for _, serverID := range prefList {
		usedServers[serverID] = true
	}

	plans := make([]putPlan, 0, len(prefList))
	for _, serverID := range prefList {
		// if the server is dead, choose a handoff node from the remaining servers
		if kv.isDead(serverID) {
			handoffServer, ok := kv.chooseHandoffNode(usedServers)
			if ok {
				usedServers[handoffServer] = true
				plans = append(plans, putPlan{targetServer: serverID, handoffServer: handoffServer})
			}
			continue
		}
		plans = append(plans, putPlan{targetServer: serverID})
	}

	if len(plans) < kv.writeQuorum {
		reply.Err = rpc.ErrWriteQuorumNotMet
		return
	}

	// forward the put request to the replicas or hinted-handoff targets according to the plans
	ch := make(chan rpc.ForwardPutResult, len(plans))
	for _, plan := range plans {
		go func(plan putPlan) {
			if plan.handoffServer != "" {
				// send the put request to the handoff server
				hintedArgs := rpc.HintedPutArgs{TargetServer: plan.targetServer, PutArgs: args.Copy()}
				hintedReply := rpc.HintedPutReply{}

				ok := kv.ends[plan.handoffServer].Call("KVServer.HintedPut", &hintedArgs, &hintedReply)

				if !ok {
					ch <- rpc.ForwardPutResult{OK: false}
				} else {
					ch <- rpc.ForwardPutResult{OK: true, Err: hintedReply.Err}
				}
			} else {
				// send the put request to the target server
				forwardArgs := args.Copy()
				forwardReply := rpc.PutReply{}

				sentAt := time.Now()
				ok := kv.ends[plan.targetServer].Call("KVServer.ReplicaPut", &forwardArgs, &forwardReply)
				receivedAt := time.Now()

				if !ok {
					ch <- rpc.ForwardPutResult{OK: false}
					return
				}
				if kv.collector != nil {
					arrivedAt := forwardReply.ArrivedAt
					respondedAt := forwardReply.RespondedAt
					_ = kv.collector.ObserveWriteLatency(kvsrv_eval.NewMessageTrace(sentAt, arrivedAt, respondedAt, receivedAt))
				}

				ch <- rpc.ForwardPutResult{OK: true, Err: forwardReply.Err}
			}
		}(plan)
	}

	// check the results from the replicas
	successCount := 0
	versionErrCount := 0
	noKeyCount := 0
	for i := 0; i < len(plans); i++ {
		res := <-ch
		if !res.OK {
			continue
		}
		if res.Err == rpc.OK {
			successCount++
			if successCount >= kv.writeQuorum {
				go drainForwardPutResults(ch, len(plans)-i-1)
				reply.Err = rpc.OK // reply OK immediately when the W quorum is met
				if kv.collector != nil {
					kv.collector.ObserveCompletedWrite(kvsrv_eval.NewCompletedWrite(args.Key, startedAt, time.Now(), args.Object))
				}
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

func (kv *KVServer) finishCoordGetReadRepair(key string, canonicalSiblings []rpc.Object,
	results []rpc.ForwardGetResult, ch <-chan rpc.ForwardGetResult, remaining int) {
	for i := 0; i < remaining; i++ {
		res := <-ch
		results = append(results, res)
		if !res.OK {
			continue
		}
		if res.Reply.Err == rpc.OK {
			for _, obj := range res.Reply.Objects {
				if obj.CanBeAddedTo(canonicalSiblings) {
					canonicalSiblings = rpc.AddObject(canonicalSiblings, obj, nil)
				}
			}
		}
	}

	hasCanonical := len(canonicalSiblings) > 0
	if !hasCanonical {
		return
	}

	staleReplicas := findStaleReplicas(canonicalSiblings, results)
	kv.repairReplicas(key, canonicalSiblings, staleReplicas)
}

func drainForwardGetResults(ch <-chan rpc.ForwardGetResult, remaining int) {
	for i := 0; i < remaining; i++ {
		<-ch
	}
}

func drainForwardPutResults(ch <-chan rpc.ForwardPutResult, remaining int) {
	for i := 0; i < remaining; i++ {
		<-ch
	}
}

// Get returns the value and context for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) ReplicaGet(args *rpc.GetArgs, reply *rpc.GetReply) {
	reply.ArrivedAt = time.Now()
	defer func() {
		reply.RespondedAt = time.Now()
	}()

	kv.mu.Lock()
	defer kv.mu.Unlock()

	siblings, ok := kv.kv[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Objects = rpc.CopyObjects(siblings)
	reply.Err = rpc.OK
	return
}

// Update the value for a key if args.Context matches the context of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Context is zero, and returns ErrNoKey otherwise.
func (kv *KVServer) ReplicaPut(args *rpc.PutArgs, reply *rpc.PutReply) {
	reply.ArrivedAt = time.Now()
	defer func() {
		reply.RespondedAt = time.Now()
	}()

	kv.mu.Lock()
	defer kv.mu.Unlock()

	siblings, ok := kv.kv[args.Key]
	// if the key doesn't exist and the context is not initial, return ErrNoKey
	if !ok && !args.BaseContext.IsInitial() {
		reply.Err = rpc.ErrNoKey
		return
	}

	// if the object cannot be added to the siblings, return ErrVersion
	baseObject := rpc.Object{Value: args.Object.Value, Context: args.BaseContext}
	canAdd := baseObject.CanBeAddedTo(siblings)
	if !canAdd {
		reply.Err = rpc.ErrVersion
		return
	}
	// otherwise, install the siblings
	// TODO: check if need change to install func
	kv.kv[args.Key] = rpc.AddObject(siblings, args.Object, nil) // nil means no specify sort function

	// if this is the first version for the key, add it to the bucket index too
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
	// start background processes
	kv.StartAntiEntropy() // start anti-entropy process
	kv.StartSyncMembers()
	kv.StartMembershipFailureDetector()
	kv.StartHintedHandoff()
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
