package chr

import (
	"fmt"
	"hash/crc32"
	"sync"
)

// use strategy 3 in the paper, a sector is a contiguous range of the ring space
// the ring space is [0, 2^32-1]
// the sectors are [0, Q-1]
type ConsistentHashRing struct {
	rwMutex sync.RWMutex

	numReplicas int // N in the paper
	numBackups  int // the number of backups excepted the top N replicas in the preference list
	numSectors  int // Q in the paper
	numServers  int // S in the paper

	bucketsPerSector int // used for building merkle tree

	hashFunc  func(string) uint32
	nodeIDs   []string
	nodes     map[string][]int // keep track of current nodes and their sectors
	sectorMap map[int]string   // sector to servers
}

// func (chr *ConsistentHashRing) KeyToSector(key string) int {
// 	hash := chr.hashFunc(key)
// 	// Map [0, 2^32-1] uniformly into [0, numSectors-1].
// 	// sector = sumSectors * hash / 2^32 = hash / 2^(32-9) = hash >> (32-9)
// 	SectorBits := bits.Len32(uint32(chr.numSectors)) - 1 // log2(numSectors)
// 	return int((uint64(hash)) >> (32-SectorBits))
// }

// func (chr *ConsistentHashRing) GetBucket(key string, sectorID int) int {
// 	hash := chr.hashFunc(key)
// 	SectorBits := bits.Len32(uint32(chr.numSectors)) - 1 
// 	BucketBits := bits.Len32(uint32(chr.bucketsPerSector)) - 1
// 	return int(((hash << SectorBits)>> (32-BucketBits)))
// }

func (chr *ConsistentHashRing) GetLocation(key string) (int, int) {
	hash := chr.hashFunc(key)
	totalBuckets := chr.numSectors * chr.bucketsPerSector
	position := int((uint64(totalBuckets) * uint64(hash)) >> 32)
	sectorID := position / chr.bucketsPerSector
	bucketID := position % chr.bucketsPerSector
	return sectorID, bucketID
}

// Hash function for consistent hashing
func Hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func MakeConsistentHashRing(numReplicas int, numSectors int, numServers int, nodeIDs []string) *ConsistentHashRing {
	chr := &ConsistentHashRing{numReplicas: numReplicas,
		numBackups: 0, // TODO: figure out the best value later
		numSectors: numSectors,
		numServers: numServers,
		bucketsPerSector: 256,
		hashFunc:   Hash,
		nodeIDs:    nodeIDs,
		nodes:      make(map[string][]int, 0),
		sectorMap:  make(map[int]string)}

	for _, nodeID := range nodeIDs {
		chr.nodes[nodeID] = make([]int, 0)
	}

	// distribute the sectors to the nodes evenly
	for i := 0; i < numSectors; i++ {
		curNodeID := chr.nodeIDs[i%numServers]
		chr.nodes[curNodeID] = append(chr.nodes[curNodeID], i)
		chr.sectorMap[i] = curNodeID
	}

	return chr
}

// return the preference list for the key
func (chr *ConsistentHashRing) GetPreferenceList(key string) []string {
	chr.rwMutex.RLock()
	defer chr.rwMutex.RUnlock()

	sectorID, _ := chr.GetLocation(key)
	prefList, _ := chr.GetNeighbors(sectorID)
	return prefList
}

// get the neighbors(nodeID + sectorID) of the sector in the ring, including the sector itself
func (chr *ConsistentHashRing) GetNeighbors(sectorID int) ([]string, []int) {
	target := chr.numReplicas + chr.numBackups
	if target > chr.numServers {
		target = chr.numServers
	}
	neighborNodes := make([]string, 0, target)
	neighborSectors := make([]int, 0, target)
	curNodeID := chr.sectorMap[sectorID]

	for len(neighborNodes) < target {
		repeatedNode := false
		for i := 0; i < len(neighborNodes); i++ {
			if neighborNodes[i] == curNodeID {
				repeatedNode = true
				break
			}
		}
		if !repeatedNode {
			neighborNodes = append(neighborNodes, curNodeID)
			neighborSectors = append(neighborSectors, sectorID)
		}
		// move to the next sector
		sectorID = (sectorID + 1) % chr.numSectors
		curNodeID = chr.sectorMap[sectorID]
	}
	return neighborNodes, neighborSectors
}

func (chr *ConsistentHashRing) GetCoordinator(key string) string {
	chr.rwMutex.RLock()
	defer chr.rwMutex.RUnlock()

	sectorID, _ := chr.GetLocation(key)

	return chr.sectorMap[sectorID]
}

// add a node to the consistent hash ring
func (chr *ConsistentHashRing) AddNode(newNodeID string) {
	chr.rwMutex.Lock()
	defer chr.rwMutex.Unlock()

	targetSectors := chr.numSectors / (chr.numServers + 1)

	// if new nodeID is already in the ring, return
	if _, ok := chr.nodes[newNodeID]; ok {
		fmt.Println("Node already exists in the ring")
		return
	}

	for curSectors := 0; curSectors < targetSectors; curSectors++ {
		richestNodeID := chr.GetRichestNode()
		sectorIndex := chr.TakeSectorFrom(richestNodeID)
		chr.nodes[newNodeID] = append(chr.nodes[newNodeID], sectorIndex)
		chr.sectorMap[sectorIndex] = newNodeID
	}
	chr.nodeIDs = append(chr.nodeIDs, newNodeID)
	chr.numServers++
}

func (chr *ConsistentHashRing) GetRichestNode() string {
	richestNodeID := ""
	maxSectors := 0
	// find the node with the most sectors
	for _, nodeID := range chr.nodeIDs {
		if richestNodeID == "" || len(chr.nodes[nodeID]) > maxSectors {
			richestNodeID = nodeID
			maxSectors = len(chr.nodes[nodeID])
		}
	}
	return richestNodeID
}

// take the first sector from a node
func (chr *ConsistentHashRing) TakeSectorFrom(nodeID string) int {
	// take the first sector from the node
	sectorIndex := chr.nodes[nodeID][0]
	chr.nodes[nodeID] = chr.nodes[nodeID][1:]
	return sectorIndex
}

func (chr *ConsistentHashRing) RemoveNode(nodeID string) {
	chr.rwMutex.Lock()
	defer chr.rwMutex.Unlock()

	redistributeSectors := chr.nodes[nodeID]
	delete(chr.nodes, nodeID)
	chr.nodeIDs = deleteFromSlice(chr.nodeIDs, nodeID)
	chr.numServers--

	// redistribute the sectors to the remaining nodes
	for i, sectorIndex := range redistributeSectors {
		curNodeID := chr.nodeIDs[i%chr.numServers]
		chr.nodes[curNodeID] = append(chr.nodes[curNodeID], sectorIndex)
		chr.sectorMap[sectorIndex] = curNodeID
	}
}

func deleteFromSlice(slice []string, id string) []string {
	for i, v := range slice {
		if v == id {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func (chr *ConsistentHashRing) NumReplicas() int {
	return chr.numReplicas
}


func (chr *ConsistentHashRing) GetNodeID(sectorID int) string {
	chr.rwMutex.RLock()
	defer chr.rwMutex.RUnlock()
	return chr.sectorMap[sectorID]
}

func (chr *ConsistentHashRing) GetSectors(nodeID string) []int {
	chr.rwMutex.RLock()
	defer chr.rwMutex.RUnlock()
	// copy the sectors, avoid returning the internal pointer
	sectors := make([]int, 0, len(chr.nodes[nodeID]))
	for _, sector := range chr.nodes[nodeID] {
		sectors = append(sectors, sector)
	}
	return sectors
}

// // get responsible sector for a key
// func (chr *ConsistentHashRing) GetResponsibleSector(key string, nodeID string) int {
// 	firstSector := chr.KeyToSector(key)
// 	neighborsNodes, neighborsSectors := chr.GetNeighbors(firstSector)
// 	for i := 0; i < len(neighborsSectors); i++ {
// 		if neighborsNodes[i] == nodeID {
// 			return neighborsSectors[i]
// 		}
// 	}
// 	return -1 // not found, should not happen
// }

func (chr *ConsistentHashRing) NumBackups() int {
	return chr.numBackups
}

func (chr *ConsistentHashRing) NumSectors() int {
	return chr.numSectors
}

func (chr *ConsistentHashRing) NumServers() int {
	return chr.numServers
}

func (chr *ConsistentHashRing) BucketsPerSector() int {
	return chr.bucketsPerSector
}