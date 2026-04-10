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

	hashFunc  func(string) uint32
	nodeIDs   []string
	nodes     map[string][]int // keep track of current nodes and their sectors
	sectorMap map[int]string   // sector to servers
}

func (chr *ConsistentHashRing) keyToSector(key string) int {
	hash := chr.hashFunc(key)
	// Map [0, 2^32-1] uniformly into [0, numSectors-1].
	return int((uint64(hash) * uint64(chr.numSectors)) >> 32)
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

	position := chr.keyToSector(key)

	prefList := make([]string, 0)
	curNodeID := chr.sectorMap[position]

	target := chr.numReplicas + chr.numBackups
	if target > chr.numServers {
		target = chr.numServers
	}
	for len(prefList) < target {
		repeatedNode := false
		for i := 0; i < len(prefList); i++ {
			if prefList[i] == curNodeID {
				repeatedNode = true
				break
			}
		}
		if !repeatedNode {
			prefList = append(prefList, curNodeID)
		}
		position = (position + 1) % chr.numSectors
		curNodeID = chr.sectorMap[position]
	}
	return prefList
}

func (chr *ConsistentHashRing) GetCoordinator(key string) string {
	chr.rwMutex.RLock()
	defer chr.rwMutex.RUnlock()

	position := chr.keyToSector(key)

	return chr.sectorMap[position]
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

func (chr *ConsistentHashRing) GetNumReplicas() int {
	chr.rwMutex.RLock()
	defer chr.rwMutex.RUnlock()
	return chr.numReplicas
}


func (chr *ConsistentHashRing) GetNodeID(sectorID int) string {
	chr.rwMutex.RLock()
	defer chr.rwMutex.RUnlock()
	return chr.sectorMap[sectorID]
}