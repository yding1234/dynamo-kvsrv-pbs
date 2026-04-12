package kvsrv

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"sort"
	"slices"
	"time"

	"6.5840/kvsrv1/rpc"
)

// writing to hash follows these two rules:
// 1. all the strings and types that aren't fixed-length are concatenated together beginning with its length
// 2. all integers are encoded in little-endian order

func writeUint64(h hash.Hash, x uint64) {
    var buf [8]byte // 8 bytes for uint64
    binary.LittleEndian.PutUint64(buf[:], x)
    h.Write(buf[:])
}

func writeString(h hash.Hash, s string) {
    writeUint64(h, uint64(len(s)))
    h.Write([]byte(s))
}

func writeBytes(h hash.Hash, b []byte) {
	writeUint64(h, uint64(len(b)))
	h.Write(b)
}

func writeObject(h hash.Hash, obj rpc.Object) {
	// write value, timestamp, and vclock
    writeString(h, obj.Value)
    writeUint64(h, obj.Context.Timestamp)

	vc := obj.Context.VC.ToVCEntries(nil) // sort by node name
	writeUint64(h, uint64(len(vc)))
    for _, entry := range vc {
        writeString(h, entry.Node)
        writeUint64(h, entry.Version)
    }
}


func writeKVPair(h hash.Hash,key string, objs []rpc.Object) {
    writeString(h, key)
	writeUint64(h, uint64(len(objs)))

	if !rpc.IsOrdered(objs, nil) { // TODO: change to use rpc.Object.IsOrdered
		objs = rpc.SortObjects(objs, nil)
	}
    for _, obj := range objs {
        writeObject(h, obj)
    }
}

func finalizeHash(h hash.Hash) [32]byte {
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}


type MerkleNode struct {
    Level    int
	Sectors  []int // sectors owned by this node
	Parent   *MerkleNode
    Left     *MerkleNode
    Right    *MerkleNode
	Hash     [32]byte
}

// func copySectors(sectors []int) []int {
// 	sectorsCopy := make([]int, len(sectors))
// 	copy(sectorsCopy, sectors)
// 	return sectorsCopy
// }

// TODO: fix after GetNodeDigest
func (kv *KVServer) MakeMerkleNode(level, sectors []int, left, right *MerkleNode) *MerkleNode {
	node := &MerkleNode{
        Level: level,
		Sectors: sectors,
		Parent: nil,
        Left: left,
        Right: right,
		Hash: make([]byte,0, 32),
    }
	node.Hash = node.GetNodeDigest(kv)
	if left != nil {
		left.Parent = node
	}
	if right != nil {
		right.Parent = node
	}
	return node
}

// TODO: get digest from the whole sector first, devided into smaller parts later
func (node *MerkleNode) GetNodeDigest(kv *KVServer) [32]byte {
	h := sha256.New()

	// writeUint64(h, uint64(level))
	// if it is a leaf node, write the kv pairs for all the sectors owned by this node
	if node.IsLeaf() {
		for _, sector := range node.Sectors {
			keys := kv.GetKeysFromSector(sector)
			if !slices.IsSorted(keys) {
				keysCopy := make([]string, len(keys))
				copy(keysCopy, keys)
				sort.Strings(keysCopy)
				keys = keysCopy
			}
			writeUint64(h, uint64(len(keys)))
			for _, key := range keys {
				writeKVPair(h, key, kv.GetSiblings(key))
			}
		}
		writeString(h, "empty")
		writeString(h, "empty")
		return finalizeHash(h)
	}

	if left == nil {
		writeString(h, "empty")
	} else {
		writeBytes(h, left.Hash[:])
	}
	if right == nil {
		writeString(h, "empty")
	} else {
		writeBytes(h, right.Hash[:])
	}

	return finalizeHash(h)
}

// TODO: fix this after divide the sectors into smaller parts
func (kv *KVServer) MakeMerkleLeaf(sector int) *MerkleNode {
	return kv.MakeMerkleNode(0, []int{sector}, nil, nil)
}

func (node *MerkleNode) IsLeaf() bool {
    return node.Left == nil && node.Right == nil
}

func (node *MerkleNode) IsRoot() bool {
    return node != nil && node.Parent == nil
}

func (node *MerkleNode) IsInternal() bool {
    return node.Left != nil && node.Right != nil
}

// TODO: fix this after divide the sectors into smaller parts
func (kv *KVServer) BuildMerkleTree(sector int) *MerkleNode {
	// take a copy of the kv data and sector keys
	kvData := kv.CopyKV()
	sectorKeys := kv.CopySectorKeys()

	sectors := []int{sector}

	// build the leaves of the merkle tree
	leaves := make([]*MerkleNode, 0, len(sectors))

	for _, sector := range sectors {
		keys := sectorKeys[sector]
		leaves = append(leaves, kv.MakeMerkleLeaf(sector))
	}

	// build the internal nodes and root of the merkle tree
	upperNodes := make([]*MerkleNode, 0, len(leaves)/2 + 1)
	level := 1
	for len(leaves) > 1 {
		i := 0
		for ; i + 1 < len(leaves); i += 2 {
			left := leaves[i]
			right := leaves[i+1]
			sectors := make([]int, 0, len(left.Sectors) + len(right.Sectors))
			sectors = append(sectors, left.Sectors...)
			sectors = append(sectors, right.Sectors...)
			upperNodes = append(upperNodes, kv.MakeMerkleNode(level, sectors, left, right))
		}
		if i + 1 == len(leaves) {
			upperNodes = append(upperNodes, kv.MakeMerkleNode(level, leaves[i].Sectors, leaves[i], nil))
		}
		leaves = upperNodes
		upperNodes = make([]*MerkleNode, 0, len(leaves)/2 + 1)
		level++
	}

	return leaves[0]
}

func (kv *KVServer) BuildAllMerkleTrees() {
	for sector := range kv.sectorKeys {
		kv.merkleRoots[sector] = kv.BuildMerkleTree(sector)
	}
}

func (kv *KVServer) StartRefreshMerkleTrees(interval time.Duration) {
    go func() {
		ticker := time.NewTicker(interval)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                kv.refreshMerkleTrees()
            case <-kv.stopCh:
                return
            }
        }
    }()
}

// TODO: Incremental update of the merkle tree instead of rebuilding the whole tree
func (kv *KVServer) refreshMerkleTrees() {
	// rebuild the merkle tree
	// TODO: jitter the rebuild time to avoid synchronization
	for sector := range kv.sectorKeys {
		newRoot := kv.BuildMerkleTree(sector)
		kv.merkleRoots[sector] = newRoot
	}
}

func (kv *KVServer) GetMerkleRoot(sector int) *MerkleNode {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.merkleRoots[sector]
}

// func (node *MerkleNode) GetNodeDigest() [32]byte {
// 	return node.Hash
// }

