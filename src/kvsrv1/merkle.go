package kvsrv

import (
	"crypto/sha256"
	"encoding/hex"
	"6.5840/kvsrv1/rpc"
	"6.5840/vclock"
	"encoding/binary"
)

// type  struct {
//     Value     string
//     Timestamp uint64
//     VCEntries []vclock.vcEntry // by lexicographical order
// }

// func MakeObjectDigest(obj rpc.Object) *objectDigest {
//     return &objectDigest{
//         Value: obj.Value,
//         Timestamp: obj.Context.Timestamp,
//         VCEntries: obj.Context.VClock.ToVCEntries(nil), // sort by node name
//     }
// }

// writing to hash follows these two rules:
// 1. all the strings and types that are fixed-length are concatenated together beginning with its length
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
	Sectors []int, // sector IDs owned by this merkle node
    Left     *MerkleNode
    Right    *MerkleNode
	Hash     [32]byte
}

func MakeMerkleNode(sectors []int, kv *KVServer) *MerkleNode {
    return &MerkleNode{
        Level: 0,
		Sectors sectors, // TODO: check if copy() is needed
        Left: nil,
        Right: nil,
		Hash: kv.GetNodeDigest(0, sectors, nil, nil, nil),
    }
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

