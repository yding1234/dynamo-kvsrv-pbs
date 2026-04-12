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
					// TODO: choose a random sector from the sectors owned by the server, do we need to do this?
					sectors := kv.GetResponsibleSectors()
					randSector := sectors[rand.Intn(len(sectors))]
					// choose a random neighbor sector of the sector, excluding the sector itself
					_, neighborSectors := kv.ring.GetNeighbors(randSector)
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
	summary := kv.GetMerkleRoot(sector).ToSummary()
	
	repairGetDiffArgs := rpc.RepairGetDiffArgs{Sector: neighborSector, Summary: summary}
	repairGetDiffReply := rpc.RepairGetDiffReply{}
	
	ok := kv.ends[neighborNode].Call("KVServer.RepairGetDiff", &repairGetDiffArgs, &repairGetDiffReply)
	if !ok {
		return
	}
	if repairGetDiffReply.Err == rpc.OK{
		return
	}
	if repairGetDiffReply.Err == rpc.ErrNoHashValue {
		// no hash value found -> no key is stored in the neighbor sector
		// the reason can be disk failure, network failure, etc.
		// we can't do anything about it for now, so we just return
		return
	}

	diffKeyInfos := repairGetDiffReply.DiffKeyInfos
	// apply the diff to the current sector
	kv.ApplyDiff(sector, diffKeyInfos, neighborNode)
}

func (kv *KVServer) RepairGetDiff(args *rpc.RepairGetDiffArgs, reply *rpc.RepairGetDiffReply) {
	mySector := args.Sector
	neighborSector := args.Summary.Sector
	
	myRoot, ok := kv.GetMerkleRoot(mySector)
	if !ok {
		reply.Err = rpc.ErrNoHashValue
		return
	}
	if myRoot.Hash == args.Summary.Hashes[0] {
		reply.Err = rpc.OK
		return
	}

	// find the different leaves
	diffLeaves := findDiffLeaves(myRoot.ToSummary(), args.Summary)

	// collect the difference key infos
	for _, leaf := range diffLeaves {
		keys := kv.GetKeysFromBucket(sector, leaf)
		for _, key := range keys {
			reply.DiffKeyInfos = append(reply.DiffKeyInfos, KeyInfo{
				Key: key,
				Objects: kv.GetSiblings(key),
			})
		}
	}
}

func findDiffLeaves(mySummary, neighborSummary rpc.TreeSummary) []int {
	myHashes := mySummary.Hashes
	neighborHashes := neighborSummary.Hashes

	diffLeaves := make([]int, 0)
	// use BFS to find the difference paths
	diffPaths := make([][]int, 0)
	queue := [][]int{{0}}
	for len(queue) > 0 {
		currPath := queue[0]
		currPos := currPath[len(currPath)-1]
		
		if myHashes[currPos] != neighborHashes[currPos] {
			// since each sectors has same number of buckets, 
			// we can directly check one merkle tree position to find if it's internal or leaf
			// and there is no chance that one is internal and the other is leaf
			if currPos < len(myHashes)/2{ // internal node
				// check the left child
				if !IsEmptyHash(myHashes[currPos+1]) { // not empty hash value
					diffPaths = append(diffPaths, append(currPath, currPos+1))
				}
				// check the right child
				if !IsEmptyHash(myHashes[currPos+2]) {
					diffPaths = append(diffPaths, append(currPath, currPos+2))
				}
			} else { // leaf node
				diffLeaves = append(diffLeaves, currPos) // found a different leaf
			}
		}
		queue = queue[1:]
	}
	return diffLeaves
}

func (kv *KVServer) ApplyDiff(sector int, diffKeyInfos []rpc.KeyInfo, neighborNode string) {
	// check validity of each key info by vclock
	for key, siblings := range diffKeyInfos {
		mySiblings := kv.GetSiblings(key)
		for _, sibling := range siblings {
			if !sibling.CanBeAddedTo(mySiblings) {
				continue
			}
			mySiblings = rpc.AddObject(mySiblings, sibling, nil) 
		}
		// update the key in the current sector
		kv.mu.Lock()
		kv.kv[key] = mySiblings
		kv.mu.Unlock()

		// send repair request to the neighbor node
		kv.ends[neighborNode].Call("KVServer.RepairPut", 
			&rpc.RepairArgs{Key: key, Objects: mySiblings, Delete: false}, &rpc.RepairReply{})
	}
}