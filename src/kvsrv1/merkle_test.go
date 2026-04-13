package kvsrv

// go test ./kvsrv1 -run 'Test.*Merkle' -v

import (
	"testing"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
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

func makeMerkleTestServer() *KVServer {
	ring := chr.MakeConsistentHashRing(2, 4, 1, []string{"s1"})
	kv := MakeKVServer("s1", ring, 1, 1, map[string]*labrpc.ClientEnd{})
	kv.installObjects("alpha", []rpc.Object{
		makeTestObject("A1", 10, "etag-a1", map[string]uint64{"s1": 1}),
	})
	kv.installObjects("beta", []rpc.Object{
		makeTestObject("B1", 20, "etag-b1", map[string]uint64{"s1": 1, "s2": 1}),
		makeTestObject("B2", 30, "etag-b2", map[string]uint64{"s3": 1}),
	})
	kv.installObjects("gamma", []rpc.Object{
		makeTestObject("G1", 40, "etag-g1", map[string]uint64{"s2": 2}),
	})
	return kv
}

func TestCopyKVAndSectorKeysDeepCopy(t *testing.T) {
	server := makeMerkleTestServer()

	kvCopy := server.CopyKV()
	sectorsCopy := server.CopySectorKeys()
	sector, _ := server.ring.GetLocation("alpha")

	server.kv["alpha"][0].Value = "changed"
	server.kv["alpha"][0].Context.ETag = "changed-etag"
	sectorsCopyMutation := server.CopySectorKeys()
	sectorsCopyMutation[sector][0] = "changed-key"

	if got := kvCopy["alpha"][0].Value; got != "A1" {
		t.Fatalf("kv copy should not share object storage, got value %q", got)
	}
	if got := kvCopy["alpha"][0].Context.ETag; got != "etag-a1" {
		t.Fatalf("kv copy should preserve original context, got etag %q", got)
	}
	if len(sectorsCopy[sector]) == 0 {
		t.Fatal("expected copied sector keys to contain data")
	}
	foundAlpha := false
	for _, key := range sectorsCopy[sector] {
		if key == "alpha" {
			foundAlpha = true
			break
		}
	}
	if !foundAlpha {
		t.Fatalf("sector keys copy should preserve alpha in sector %d, got %v", sector, sectorsCopy[sector])
	}
	if len(sectorsCopyMutation[sector]) == 0 {
		t.Fatal("expected copied sector keys mutation slice to contain data")
	}
	if sectorsCopyMutation[sector][0] == sectorsCopy[sector][0] {
		t.Fatal("expected independent sector key copies")
	}
}

func TestGetNodeDigestLeafChangesWhenDataChanges(t *testing.T) {
	kv := makeMerkleTestServer()
	sector, bucket := kv.ring.GetLocation("alpha")
	leaf := kv.MakeMerkleLeaf(sector, bucket)
	digest1 := leaf.Hash

	kv.installObjects("alpha", []rpc.Object{
		makeTestObject("A1-updated", 11, "etag-a1-updated", map[string]uint64{"s1": 2}),
	})
	leaf2 := kv.MakeMerkleLeaf(sector, bucket)
	digest2 := leaf2.Hash

	if digest1 == digest2 {
		t.Fatal("leaf digest should change when bucket contents change")
	}
}

func TestGetNodeDigestInternalDependsOnChildHashes(t *testing.T) {
	kv := makeMerkleTestServer()
	left := kv.MakeMerkleLeaf(0, 0)
	right := kv.MakeMerkleLeaf(0, 1)
	parent1 := kv.MakeMerkleInternalNode(left, right).Hash

	right.Hash[0] ^= 0xff
	parent2 := kv.MakeMerkleNode(1, left.Sector, -1, left, right).Hash

	if parent1 == parent2 {
		t.Fatal("internal node digest should change when a child hash changes")
	}
}

func TestMakeMerkleNodeSetsParents(t *testing.T) {
	kv := makeMerkleTestServer()
	left := kv.MakeMerkleLeaf(0, 0)
	right := kv.MakeMerkleLeaf(0, 1)
	node := kv.MakeMerkleInternalNode(left, right)

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

func TestBuildMerkleTreeUsesAllBuckets(t *testing.T) {
	kv := makeMerkleTestServer()
	root := kv.BuildMerkleTree(0)
	summary := root.ToSummary()
	wantNodes := 2*kv.ring.BucketsPerSector() - 1

	if root == nil {
		t.Fatal("expected non-nil root")
	}
	if root.IsLeaf() {
		t.Fatal("multi-bucket tree root should not be a leaf")
	}
	if len(summary.Hashes) != wantNodes {
		t.Fatalf("unexpected number of nodes in summary: got %d want %d", len(summary.Hashes), wantNodes)
	}
}

func TestBuildMerkleTreeDeterministicAcrossKeyOrderWithinBucket(t *testing.T) {
	kv1 := makeMerkleTestServer()
	kv2 := makeMerkleTestServer()
	sector, bucket := kv1.ring.GetLocation("alpha")

	kv1.mu.Lock()
	kv1.keysInBuckets[sector][bucket] = []string{"alpha", "beta"}
	kv1.mu.Unlock()
	kv2.mu.Lock()
	kv2.keysInBuckets[sector][bucket] = []string{"beta", "alpha"}
	kv2.mu.Unlock()

	root1 := kv1.BuildMerkleTree(sector)
	root2 := kv2.BuildMerkleTree(sector)

	if root1.Hash != root2.Hash {
		t.Fatalf("expected deterministic root hash across key order: %x != %x", root1.Hash, root2.Hash)
	}
}

func TestBuildMerkleTreeDeterministicAcrossObjectOrderWithinKey(t *testing.T) {
	ring := chr.MakeConsistentHashRing(2, 4, 1, []string{"s1"})
	kv1 := MakeKVServer("s1", ring, 1, 1, map[string]*labrpc.ClientEnd{})
	kv2 := MakeKVServer("s1", ring, 1, 1, map[string]*labrpc.ClientEnd{})

	objects1 := []rpc.Object{
		makeTestObject("B1", 20, "etag-b1", map[string]uint64{"s1": 1, "s2": 1}),
		makeTestObject("B2", 30, "etag-b2", map[string]uint64{"s3": 1}),
	}
	objects2 := []rpc.Object{
		makeTestObject("B2", 30, "etag-b2", map[string]uint64{"s3": 1}),
		makeTestObject("B1", 20, "etag-b1", map[string]uint64{"s1": 1, "s2": 1}),
	}

	kv1.installObjects("beta", objects1)
	kv2.installObjects("beta", objects2)

	sector, _ := kv1.ring.GetLocation("beta")
	root1 := kv1.BuildMerkleTree(sector)
	root2 := kv2.BuildMerkleTree(sector)

	if root1.Hash != root2.Hash {
		t.Fatalf("expected deterministic root hash across object order: %x != %x", root1.Hash, root2.Hash)
	}
}
