package kvsrv

import (
	"crypto/sha256"
	"encoding/hex"
	"6.5840/kvsrv1/rpc"
	"6.5840/vclock"
	"encoding/binary"
	"time"
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

func writeObject(h hash.Hash, obj rpc.Object) {
    writeString(h, obj.Value)
    writeUint64(h, obj.Timestamp)

	vc := obj.Context.VClock.ToVCEntries(nil) // sort by node name
	writeUint64(h, uint64(len(vc)))
    for node, version := range vc {
        writeString(h, node)
        writeUint64(h, version)
    }
}

// get the digest of the kv pair (key, objs)
func writeKVPair(h hash.Hash,key string, objs []rpc.Object) {
    writeString(h, key)
	writeUint64(h, uint64(len(objs)))

	if !rpc.IsOrdered(objs, nil) {
		objs = rpc.SortObjects(objs, nil)
	}
    for _, obj := range objs {
        writeObject(h, obj)
    }
    // return hex.EncodeToString(h.Sum(nil))
}


type MerkleNode struct {
    Level    int
	// TODO: currently only support one sector per node, but in the future we might need to support multiple sectors per node
	Sectors []int, // sector IDs owned by this merkle node
    Left     *MerkleNode
    Right    *MerkleNode
	Hash     [32]byte
}

func MakeMerkleNode(level int, sectors []int, left, right *MerkleNode, kv *KVServer) *MerkleNode {
    return &MerkleNode{
        Level: level,
		Sectors sectors, // TODO: check if copy() is needed
        Left: left,
        Right: right,
		Hash: kv.GetNodeDigest(level, sectors, left, right, nil),
    }
}

func MakeMerkleLeaf(sectors []int, kv *KVServer) *MerkleNode {
	return MakeMerkleNode(0, sectors, nil, nil, kv)
}

func (node *MerkleNode) IsLeaf() bool {
    return node.Left == nil && node.Right == nil
}

func (node *MerkleNode) IsRoot() bool {
    return node.Level == 0
}

func (node *MerkleNode) IsInternal() bool {
    return node.Left != nil && node.Right != nil
}


func (kv *KVServer) GetNodeDigest(level int, sectors []int, left, right *MerkleNode, hFunc hash.Hash) [32]byte {
	if hFunc == nil {
		h = sha256.New() // default hash function
	} else {
		h = hFunc() // use the provided hash function
	}

	writeUint64(h, uint64(level))
	writeUint64(h, uint64(len(sectors)))

	// if it is a leaf node, write the kv pairs for all the sectors owned by this node
	if left == nil && right == nil {
		for _, sector := range sectors {
			keys := kv.sectorsKeys[sector]
			writeUint64(h, uint64(len(keys)))
			for _, key := range keys {
				objs := kv.kv[key]
				writeKVPair(h, key, objs)
			}
		}
		writeString("empty") // left
		writeString("empty") // right
	} else { // if it is an internal node or root, write the digest of the left and right children
		if left == nil {
			writeString("empty")
		} else {
			h.Write(kv.GetNodeDigest(level + 1, left.Sectors, left.Left, left.Right, h))
		}
		if right == nil {
			writeString("empty")
		} else {
			h.Write(kv.GetNodeDigest(level + 1, right.Sectors, right.Left, right.Right, h))
		}
	}
	return h.Sum(nil) // return the hash of the node
}

func (kv *KVServer) MakeMerkleTree() *MerkleNode {
	// build the leaves of the merkle tree
	leaves := make([]*MerkleNode, 0, len(kv.sectorsKeys))

	for sector, keys := range kv.sectorsKeys {
		leaves = append(leaves, MakeMerkleLeaf(sector, keys, kv))
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
			upperNodes = append(upperNodes, MakeMerkleNode(level, sectors, left, right, kv))
		}
		if i + 1 == len(leaves) {
			upperNodes = append(upperNodes, MakeMerkleNode(level, leaves[i].Sectors, leaves[i], nil, kv))
		}
		leaves = upperNodes
		upperNodes = make([]*MerkleNode, 0, len(leaves)/2 + 1)
		level++
	}
	return leaves[0]
}


func (kv *KVServer) RebuildMerkleTree() {
	kv.stopCh <- struct{}{}
	<- kv.stopCh
	kv.MakeMerkleTree()
}


func (kv *KVServer) StartRefreshMerkleTree(interval time.Duration) {
    go func() {
		ticker := time.NewTicker(interval)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                kv.RebuildMerkleTree()
            case <-kv.stopCh:
                return
            }
        }
    }()
}

// TODO: Incremental update of the merkle tree instead of rebuilding the whole tree
func (kv *KVServer) refreshMerkleTree() {
	// copy the kv and sectors-keys map
    kv.mu.Lock()

    kvCopy := make(map[string][]rpc.Object, len(kv.kv))
    for k, objs := range kv.kv {
        kvCopy[k] = rpc.CopyObjects(objs)
    }

    sectorsCopy := make(map[int][]string, len(kv.sectorsKeys))
    for sectorID, keys := range kv.sectorsKeys {
        copied := make([]string, len(keys))
        copy(copied, keys)
        sectorsCopy[sectorID] = copied
    }

    kv.mu.Unlock()

	// rebuild the merkle tree
    newRoot := kv.MakeMerkleTree(kvCopy, sectorsCopy)
	kv.merkleRoot = newRoot
}
