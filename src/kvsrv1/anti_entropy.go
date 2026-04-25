package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"math/rand"
	"time"
)

func (kv *KVServer) StartAntiEntropy() {
	rand.Seed(int64(time.Now().UnixNano()))
	go func() {
		ticker := time.NewTicker(kv.antiEntropyInterval)
		defer ticker.Stop()
		for {
			select {
				case <-ticker.C:
					// choose a random sector from the sectors managed by the server
					sectors := kv.GetResponsibleSectors()
					if len(sectors) == 0 {
						continue
					}
					randSector := sectors[rand.Intn(len(sectors))]
					// choose a random neighbor sector of the sector, excluding the sector itself
					_, neighborSectors := kv.ring.GetNeighbors(randSector)
					if len(neighborSectors) <= 1 {
						continue
					}
					randChosen := rand.Intn(len(neighborSectors)-1) + 1 // +1 because the sector itself is not a neighbor

					kv.Reconcile(randSector, neighborSectors[randChosen])
				case <-kv.stopCh:
					return
				}
			}
		}()
}

// reconcile with the neighbor sector
func (kv *KVServer) Reconcile(sector int, neighborSector int) {
	// send anti-entropy request to the neighbor
	neighborNode := kv.ring.GetNodeID(neighborSector)
	root, ok := kv.GetMerkleRoot(sector)
	if !ok || root == nil {
		return
	}

	summary := root.ToSummary()
	

	repairGetDiffArgs := rpc.RepairGetDiffArgs{Sector: sector, Summary: summary}
	repairGetDiffReply := rpc.RepairGetDiffReply{}
	
	ok = kv.ends[neighborNode].Call("KVServer.RepairGetDiff", &repairGetDiffArgs, &repairGetDiffReply)
	if !ok {
		// network error
		return
	}
	if repairGetDiffReply.Err == rpc.ErrNoHashValue {
		// no hash value found -> no key is stored in the neighbor sector
		// the reason can be disk failure, network failure, etc.
		// we can't do anything about it for now, so we just return
		return
	}
	if repairGetDiffReply.Err == rpc.OK && len(repairGetDiffReply.DiffKeyInfos) == 0 {
		// no difference found
		return
	}

	diffKeyInfos := repairGetDiffReply.DiffKeyInfos
	// apply the diff to the current sector
	kv.ApplyDiff(sector, diffKeyInfos, neighborNode)
}


func (kv *KVServer) RepairGetDiff(args *rpc.RepairGetDiffArgs, reply *rpc.RepairGetDiffReply) {
	mySector := args.Sector
	
	myRoot, ok := kv.GetMerkleRoot(mySector)
	if !ok || myRoot == nil {
		reply.Err = rpc.ErrNoHashValue
		return
	}
	if myRoot.Hash == args.Summary.Hashes[0] {
		reply.Err = rpc.OK
		return
	}

	// find the different leaves
	diffBuckets := findDiffBuckets(myRoot.ToSummary(), args.Summary)

	// collect the difference key infos
	for _, bucket := range diffBuckets {
		keys := kv.GetKeysFromBucket(mySector, bucket)
		// add the key and its siblings in the current bucket to the reply
		for _, key := range keys {
			reply.DiffKeyInfos = append(reply.DiffKeyInfos, rpc.KeyInfo{
				Key: key,
				Objects: kv.GetSiblings(key),
			})
		}
	}
	reply.Err = rpc.OK
}

func findDiffBuckets(mySummary, neighborSummary rpc.TreeSummary) []int {
	myHashes := mySummary.Hashes
	neighborHashes := neighborSummary.Hashes

	diffBuckets := make([]int, 0)
	// use BFS to find the different buckets
	queue := []int{0}
	for len(queue) > 0 {
		currPos := queue[0]
		queue = queue[1:]
		
		if myHashes[currPos] != neighborHashes[currPos] {
			// since each sectors has same number of buckets, 
			// we can directly check one merkle tree position to find if it's internal or leaf
			// and there is no chance that one is internal and the other is leaf
			if currPos < len(myHashes)/2 { // internal node
				// check the left child
				if !IsEmptyHash(myHashes[currPos*2+1]) {
					queue = append(queue, currPos*2+1)
				}
				// check the right child
				if !IsEmptyHash(myHashes[currPos*2+2]) {
					queue = append(queue, currPos*2+2)
				}
			} else { // leaf node
				// convert leaf node position to bucket index
				bucketIndex := currPos - len(myHashes)/2
				diffBuckets = append(diffBuckets, bucketIndex) // found a different bucket
			}
		}
	}
	return diffBuckets
}

func (kv *KVServer) ApplyDiff(sector int, diffKeyInfos []rpc.KeyInfo, neighborNode string) {
	for _, keyInfo := range diffKeyInfos {
		key := keyInfo.Key
		merged := kv.mergeObjects(key, keyInfo.Objects)
		if len(merged) == 0 {
			continue
		}

		// repair put the merged view to the neighbor; 
		// repairPut also uses merge semantics so the neighbor will not roll back any concurrent writes
		kv.ends[neighborNode].Call("KVServer.RepairPut",
			&rpc.RepairArgs{Key: key, Objects: merged, Delete: false}, &rpc.RepairReply{})
	}
}