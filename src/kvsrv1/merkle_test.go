package kvsrv

// go test ./kvsrv1 -run 'Test.*Merkle' -v

import (
	"reflect"
	"testing"

	"6.5840/kvsrv1/rpc"
	"6.5840/vclock"
)

func makeTestObject(value string, timestamp uint64, etag string, versions map[string]uint64) rpc.Object {
	vc := vclock.NewVClock()
	for node, version := range versions {
		vc[node] = version
	}
	return rpc.Object{
		Value: value,
		Context: rpc.Context{
			VC:        vc,
			Timestamp: timestamp,
			ETag:      etag,
		},
	}
}

func makeMerkleSnapshot() (map[string][]rpc.Object, map[int][]string) {
	return map[string][]rpc.Object{
			"alpha": {
				makeTestObject("A1", 10, "etag-a1", map[string]uint64{"s1": 1}),
			},
			"beta": {
				makeTestObject("B1", 20, "etag-b1", map[string]uint64{"s1": 1, "s2": 1}),
				makeTestObject("B2", 30, "etag-b2", map[string]uint64{"s3": 1}),
			},
			"gamma": {
				makeTestObject("G1", 40, "etag-g1", map[string]uint64{"s2": 2}),
			},
		}, map[int][]string{
			1: {"alpha", "beta"},
			2: {"gamma"},
		}
}

func TestCopyKVAndSectorKeysDeepCopy(t *testing.T) {
	server := &KVServer{
		kv: map[string][]rpc.Object{
			"alpha": {
				makeTestObject("A1", 10, "etag-a1", map[string]uint64{"s1": 1}),
			},
		},
		sectorKeys: map[int][]string{
			1: {"alpha"},
		},
	}

	kvCopy, sectorsCopy := server.CopyKVAndSectorKeys()

	server.kv["alpha"][0].Value = "changed"
	server.kv["alpha"][0].Context.ETag = "changed-etag"
	server.sectorKeys[1][0] = "changed-key"

	if got := kvCopy["alpha"][0].Value; got != "A1" {
		t.Fatalf("kv copy should not share object storage, got value %q", got)
	}
	if got := kvCopy["alpha"][0].Context.ETag; got != "etag-a1" {
		t.Fatalf("kv copy should preserve original context, got etag %q", got)
	}
	if got := sectorsCopy[1][0]; got != "alpha" {
		t.Fatalf("sector keys copy should not share slice storage, got key %q", got)
	}
}

func TestGetNodeDigestLeafChangesWhenDataChanges(t *testing.T) {
	kvCopy, sectorsCopy := makeMerkleSnapshot()

	digest1 := getNodeDigest(0, []int{1}, nil, nil, kvCopy, sectorsCopy)
	digest2 := getNodeDigest(0, []int{1}, nil, nil, kvCopy, sectorsCopy)
	if digest1 != digest2 {
		t.Fatal("same sorted snapshot should produce the same leaf digest")
	}

	changedKV, changedSectors := makeMerkleSnapshot()
	changedKV["beta"][0].Value = "B1-updated"
	changedKV["beta"][0].Context.ETag = "etag-b1-updated"

	digest3 := getNodeDigest(0, []int{1}, nil, nil, changedKV, changedSectors)
	if digest1 == digest3 {
		t.Fatal("leaf digest should change when sector contents change")
	}
}

func TestGetNodeDigestInternalDependsOnChildHashes(t *testing.T) {
	kvCopy, sectorsCopy := makeMerkleSnapshot()

	left := &MerkleNode{
		Level:   0,
		Sectors: []int{1},
		Hash:    getNodeDigest(0, []int{1}, nil, nil, kvCopy, sectorsCopy),
	}
	right := &MerkleNode{
		Level:   0,
		Sectors: []int{2},
		Hash:    getNodeDigest(0, []int{2}, nil, nil, kvCopy, sectorsCopy),
	}

	parent1 := getNodeDigest(1, []int{1, 2}, left, right, kvCopy, sectorsCopy)
	right.Hash[0] ^= 0xff
	parent2 := getNodeDigest(1, []int{1, 2}, left, right, kvCopy, sectorsCopy)

	if parent1 == parent2 {
		t.Fatal("internal node digest should change when a child hash changes")
	}
}

func TestMakeMerkleNodeCopiesSectorsAndSetsParents(t *testing.T) {
	kvCopy, sectorsCopy := makeMerkleSnapshot()
	left := &MerkleNode{Level: 0, Sectors: []int{1}}
	right := &MerkleNode{Level: 0, Sectors: []int{2}}
	sectors := []int{1, 2}

	node := MakeMerkleNode(1, sectors, left, right, kvCopy, sectorsCopy)
	sectors[0] = 99

	if !reflect.DeepEqual(node.Sectors, []int{1, 2}) {
		t.Fatalf("node should keep its own copy of sectors, got %v", node.Sectors)
	}
	if left.Parent != node {
		t.Fatal("left child parent pointer was not set")
	}
	if right.Parent != node {
		t.Fatal("right child parent pointer was not set")
	}
	if !node.IsInternal() {
		t.Fatal("node with two children should be internal")
	}
}

func TestMakeMerkleTreeSingleSectorReturnsLeafRoot(t *testing.T) {
	kvCopy := map[string][]rpc.Object{
		"alpha": {
			makeTestObject("A1", 10, "etag-a1", map[string]uint64{"s1": 1}),
		},
	}
	sectorsCopy := map[int][]string{
		7: {"alpha"},
	}

	root := MakeMerkleTree(kvCopy, sectorsCopy)
	wantHash := getNodeDigest(0, []int{7}, nil, nil, kvCopy, sectorsCopy)

	if root == nil {
		t.Fatal("expected non-nil root")
	}
	if !root.IsLeaf() {
		t.Fatal("single-sector tree root should be a leaf")
	}
	if !root.IsRoot() {
		t.Fatal("single-sector tree root should report IsRoot")
	}
	if !reflect.DeepEqual(root.Sectors, []int{7}) {
		t.Fatalf("unexpected root sectors: %v", root.Sectors)
	}
	if root.Hash != wantHash {
		t.Fatal("single-sector root hash should match leaf digest")
	}
}
