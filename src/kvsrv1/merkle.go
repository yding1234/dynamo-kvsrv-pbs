package kvsrv

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
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

	// if !rpc.IsOrdered(objs, nil) {
	// 	objs = rpc.SortObjects(objs, nil)
	// }
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
	// TODO: currently only support one sector per node, but in the future we might need to support multiple sectors per node
	Sectors []int // sector IDs owned by this merkle node
	Parent   *MerkleNode
    Left     *MerkleNode
    Right    *MerkleNode
	Hash     [32]byte
}

func MakeMerkleNode(level int, sectorsCopy []int, left, right *MerkleNode, kvCopy map[string][]rpc.Object, sectorKeysCopy map[int][]string) *MerkleNode {
	node := &MerkleNode{
        Level: level,
		Sectors: sectorsCopy,
        Left: left,
        Right: right,
		Hash: getNodeDigest(level, sectorsCopy, left, right, kvCopy, sectorKeysCopy),
    }
	if left != nil {
		left.Parent = node
	}
	if right != nil {
		right.Parent = node
	}
	return node
}

func MakeMerkleLeaf(sector int, kvCopy map[string][]rpc.Object, keys []string) *MerkleNode {
	return MakeMerkleNode(0, []int{sector}, nil, nil, kvCopy, map[int][]string{sector: keys})
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

func getNodeDigest(level int, sectors []int, left, right *MerkleNode, kvCopy map[string][]rpc.Object, sectorKeysCopy map[int][]string) [32]byte {
	h := sha256.New()

	writeUint64(h, uint64(level))
	writeUint64(h, uint64(len(sectors)))

	// if it is a leaf node, write the kv pairs for all the sectors owned by this node
	if left == nil && right == nil {
		for _, sector := range sectors {
			keys := sectorKeysCopy[sector]
			writeUint64(h, uint64(len(keys)))
			for _, key := range keys {
				writeKVPair(h, key, kvCopy[key])
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

func MakeMerkleTree(kvCopy map[string][]rpc.Object, sectorKeysCopy map[int][]string) *MerkleNode {
	// build the leaves of the merkle tree
	leaves := make([]*MerkleNode, 0, len(sectorKeysCopy))

	for sector, keys := range sectorKeysCopy {
		leaves = append(leaves, MakeMerkleLeaf(sector, kvCopy, keys))
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
			upperNodes = append(upperNodes, MakeMerkleNode(level, sectors, left, right, kvCopy, sectorKeysCopy))
		}
		if i + 1 == len(leaves) {
			upperNodes = append(upperNodes, MakeMerkleNode(level, leaves[i].Sectors, leaves[i], nil, kvCopy, sectorKeysCopy))
		}
		leaves = upperNodes
		upperNodes = make([]*MerkleNode, 0, len(leaves)/2 + 1)
		level++
	}

	return leaves[0]
}

func (kv *KVServer) BuildMerkleTree() *MerkleNode {
	kvCopy, sectorKeysCopy := kv.CopyKVAndSectorKeys()
	return MakeMerkleTree(kvCopy, sectorKeysCopy)
}

func (kv *KVServer) StartRefreshMerkleTree(interval time.Duration) {
    go func() {
		ticker := time.NewTicker(interval)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                kv.refreshMerkleTree()
            case <-kv.stopCh:
                return
            }
        }
    }()
}

// TODO: Incremental update of the merkle tree instead of rebuilding the whole tree
func (kv *KVServer) refreshMerkleTree() {
	// rebuild the merkle tree
    newRoot := kv.BuildMerkleTree()
	kv.mu.Lock()
	kv.merkleRoot = newRoot
	kv.mu.Unlock()
}


