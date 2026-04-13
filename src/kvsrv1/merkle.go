package kvsrv

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
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

	Sector   int
	// only for leaf nodes, otherwise -1
	Bucket   int

	Parent   *MerkleNode
    Left     *MerkleNode
    Right    *MerkleNode
	Hash     [32]byte
}

// TODO: fix after GetNodeDigest
func (kv *KVServer) MakeMerkleNode(level, sector, bucket int, left, right *MerkleNode) *MerkleNode {
	node := &MerkleNode{
        Level: level,
		Sector: sector,
		Bucket: bucket,
		Parent: nil,
        Left: left,
        Right: right,
		Hash: [32]byte{},
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

func (kv *KVServer) MakeMerkleLeaf(sector, bucket int) *MerkleNode {
	return kv.MakeMerkleNode(0, sector, bucket, nil, nil)
}

func (kv *KVServer) MakeMerkleInternalNode(left, right *MerkleNode) *MerkleNode {
	return kv.MakeMerkleNode(left.Level + 1, left.Sector, -1, left, right)
}

// TODO: get digest from the whole sector first, devided into smaller parts later
func (node *MerkleNode) GetNodeDigest(kv *KVServer) [32]byte {
	h := sha256.New()

	// writeUint64(h, uint64(level))
	// if it is a leaf node, write the kv pairs for all the keys in the bucket
	if node.IsLeaf() {
		keys := kv.GetKeysFromBucket(node.Sector, node.Bucket)
		// sort the keys if not sorted
		if !slices.IsSorted(keys) {
			slices.Sort(keys)
		}
		writeUint64(h, uint64(len(keys)))
		for _, key := range keys {
			writeKVPair(h, key, kv.GetSiblings(key))
		}
		writeString(h, "empty")
		writeString(h, "empty")
		return finalizeHash(h)
	}

	if node.Left == nil {
		writeString(h, "empty")
	} else {
		writeBytes(h, node.Left.Hash[:])
	}
	if node.Right == nil {
		writeString(h, "empty")
	} else {
		writeBytes(h, node.Right.Hash[:])
	}

	return finalizeHash(h)
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

// Build one fixed leaf per bucket so every replica produces the same tree shape.
func (kv *KVServer) BuildMerkleTree(sector int) *MerkleNode {
	// build the leaves of the merkle tree
	leaves := make([]*MerkleNode, 0, kv.ring.BucketsPerSector())
	for bucket := 0; bucket < kv.ring.BucketsPerSector(); bucket++ {
		leaves = append(leaves, kv.MakeMerkleLeaf(sector, bucket))
	}

	// build the internal nodes and root of the merkle tree
	for len(leaves) > 1 {
		upperNodes := make([]*MerkleNode, 0, len(leaves)/2+1)
		i := 0
		for ; i + 1 < len(leaves); i += 2 {
			left := leaves[i]
			right := leaves[i+1]
			upperNodes = append(upperNodes, kv.MakeMerkleInternalNode(left, right))
		}
		if i + 1 == len(leaves) {
			upperNodes = append(upperNodes, kv.MakeMerkleInternalNode(leaves[i], nil))
		}
		leaves = upperNodes
	}

	return leaves[0]
}

func (kv *KVServer) BuildAllMerkleTrees() {
	for sector, _ := range kv.keysInBuckets {
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
	for sector := 0; sector < kv.ring.NumSectors(); sector++ {
		newRoot := kv.BuildMerkleTree(sector)
		kv.mu.Lock()
		kv.merkleRoots[sector] = newRoot
		kv.mu.Unlock()
	}
}

func (kv *KVServer) GetMerkleRoot(sector int) (*MerkleNode, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	root, ok := kv.merkleRoots[sector]
	return root, ok
}

func (root *MerkleNode) ToSummary() rpc.TreeSummary {
	hashes := make([][32]byte, 0)

	// access the nodes in BFS order
	queue := []*MerkleNode{root}
	visited := make(map[*MerkleNode]bool)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		hashes = append(hashes, node.Hash)
		visited[node] = true
		if node.IsInternal() && !visited[node.Left] {
			queue = append(queue, node.Left)
		}
		if node.IsInternal() && !visited[node.Right] {
			queue = append(queue, node.Right)
		}
	}
	
	return rpc.TreeSummary{
		Sector: root.Sector,
		Hashes: hashes,
	}
}

func IsEmptyHash(hash [32]byte) bool {
	h := sha256.New()
	writeUint64(h, 0)
	writeString(h, "empty")
	writeString(h, "empty")
	emptyHash := finalizeHash(h)
	return hash == emptyHash
}