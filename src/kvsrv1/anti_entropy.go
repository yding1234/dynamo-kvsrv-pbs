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
					// choose a random sector from the sectors owned by the server
					sectors := kv.GetSectors()
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
	sectorHash := kv.BuildMerkleNodeForSector(sector).GetDigest()
	repairGetDiffArgs := rpc.RepairGetDiffArgs{SectorID: neighborSector, Hash: sectorHash}
	repairGetDiffReply := rpc.RepairGetDiffReply{}
	
	ok := kv.ends[neighborNode].Call("KVServer.RepairGetDiff", &repairGetDiffArgs, &repairGetDiffReply)
	if !ok {
		return
	}
	if repairGetDiffReply.Err != rpc.OK {
		return
	}
	diff := repairGetDiffReply.Diff
	// apply the diff to the current sector
	kv.ApplyDiff(sector, diff)
}