package kvsrv

import (
	"testing"
	"time"

	"dynamo-kvsrv/kvsrv/chr"
	"dynamo-kvsrv/kvsrv/rpc"
	"dynamo-kvsrv/labrpc"
	"dynamo-kvsrv/tester"
)

func makeAntiEntropyUnitServer() *KVServer {
	ring := chr.MakeConsistentHashRing(1, 4, 1, []string{"s1"})
	return MakeKVServer("s1", ring, 1, 1, map[string]*labrpc.ClientEnd{})
}

func makeAntiEntropyCluster(t *testing.T) (*chr.ConsistentHashRing, map[string]*KVServer, func()) {
	t.Helper()

	nodeIDs := []string{"s1", "s2"}
	ring := chr.MakeConsistentHashRing(2, 8, len(nodeIDs), nodeIDs)
	net := labrpc.MakeNetwork()

	ends := make(map[string]map[string]*labrpc.ClientEnd, len(nodeIDs))
	for _, from := range nodeIDs {
		ends[from] = make(map[string]*labrpc.ClientEnd, len(nodeIDs))
		for _, to := range nodeIDs {
			endName := from + "->" + to
			end := net.MakeEnd(endName)
			net.Connect(endName, to)
			net.Enable(endName, true)
			ends[from][to] = end
		}
	}

	servers := make(map[string]*KVServer, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		servers[nodeID] = MakeKVServer(nodeID, ring, 1, 1, ends[nodeID])
	}
	for _, nodeID := range nodeIDs {
		rs := labrpc.MakeServer()
		rs.AddService(labrpc.MakeService(servers[nodeID]))
		net.AddServer(nodeID, rs)
	}

	cleanup := func() {
		for _, kv := range servers {
			close(kv.stopCh)
		}
		net.Cleanup()
	}
	return ring, servers, cleanup
}

func makeSmallAntiEntropyCluster(t *testing.T) (*chr.ConsistentHashRing, map[string]*KVServer, func()) {
	t.Helper()

	nodeIDs := []string{"s1", "s2"}
	ring := chr.MakeConsistentHashRing(2, 2, len(nodeIDs), nodeIDs)
	net := labrpc.MakeNetwork()

	ends := make(map[string]map[string]*labrpc.ClientEnd, len(nodeIDs))
	for _, from := range nodeIDs {
		ends[from] = make(map[string]*labrpc.ClientEnd, len(nodeIDs))
		for _, to := range nodeIDs {
			endName := from + "->" + to
			end := net.MakeEnd(endName)
			net.Connect(endName, to)
			net.Enable(endName, true)
			ends[from][to] = end
		}
	}

	servers := make(map[string]*KVServer, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		servers[nodeID] = MakeKVServer(nodeID, ring, 1, 1, ends[nodeID])
	}
	for _, nodeID := range nodeIDs {
		rs := labrpc.MakeServer()
		rs.AddService(labrpc.MakeService(servers[nodeID]))
		net.AddServer(nodeID, rs)
	}

	cleanup := func() {
		for _, kv := range servers {
			close(kv.stopCh)
		}
		net.Cleanup()
	}
	return ring, servers, cleanup
}

func replicaSectorForNode(ring *chr.ConsistentHashRing, sector int, nodeID string) int {
	_, neighborSectors := ring.GetNeighbors(sector)
	for _, neighborSector := range neighborSectors {
		if ring.GetNodeID(neighborSector) == nodeID {
			return neighborSector
		}
	}
	return -1
}

func TestFindDiffBucketsReturnsChangedBucket(t *testing.T) {
	kv1 := makeAntiEntropyUnitServer()
	kv2 := makeAntiEntropyUnitServer()

	const key = "anti-entropy-diff"
	sector, bucket := kv1.ring.GetLocation(key)

	kv1.installObjects(key, []rpc.Object{
		makeTestObject("old", 10, "etag-old", map[string]uint64{"s1": 1}),
	})
	kv2.installObjects(key, []rpc.Object{
		makeTestObject("new", 20, "etag-new", map[string]uint64{"s2": 1}),
	})

	root1, ok := kv1.GetMerkleRoot(sector)
	if !ok || root1 == nil {
		t.Fatal("expected first merkle root to exist")
	}
	root2, ok := kv2.GetMerkleRoot(sector)
	if !ok || root2 == nil {
		t.Fatal("expected second merkle root to exist")
	}

	diffBuckets := findDiffBuckets(root1.ToSummary(), root2.ToSummary())
	if len(diffBuckets) != 1 || diffBuckets[0] != bucket {
		t.Fatalf("unexpected diff buckets: got %v want [%d]", diffBuckets, bucket)
	}
}

func TestRepairGetDiffReturnsDiffKeyInfos(t *testing.T) {
	fresh := makeAntiEntropyUnitServer()
	stale := makeAntiEntropyUnitServer()

	const key = "anti-entropy-repair"
	sector, _ := fresh.ring.GetLocation(key)
	fresh.installObjects(key, []rpc.Object{
		makeTestObject("fresh", 20, "etag-fresh", map[string]uint64{"writer": 1}),
	})
	stale.installObjects(key, []rpc.Object{
		makeTestObject("stale", 10, "etag-stale", map[string]uint64{"writer": 0}),
	})

	root, ok := fresh.GetMerkleRoot(sector)
	if !ok || root == nil {
		t.Fatal("expected fresh merkle root to exist")
	}

	args := &rpc.RepairGetDiffArgs{Sector: sector, Summary: root.ToSummary()}
	reply := &rpc.RepairGetDiffReply{}
	stale.RepairGetDiff(args, reply)

	if reply.Err != rpc.OK {
		t.Fatalf("expected OK from RepairGetDiff, got %v", reply.Err)
	}
	if len(reply.DiffKeyInfos) != 1 {
		t.Fatalf("expected exactly one differing key, got %d", len(reply.DiffKeyInfos))
	}
	if reply.DiffKeyInfos[0].Key != key {
		t.Fatalf("expected differing key %q, got %q", key, reply.DiffKeyInfos[0].Key)
	}
	if !IsSameSiblings(reply.DiffKeyInfos[0].Objects, stale.GetSiblings(key)) {
		t.Fatalf("expected stale sibling set in diff reply, got %v", reply.DiffKeyInfos[0].Objects)
	}
}

func TestReconcileRepairsStaleReplica(t *testing.T) {
	ring, servers, cleanup := makeAntiEntropyCluster(t)
	defer cleanup()

	const key = "anti-entropy-reconcile"
	sector, _ := ring.GetLocation(key)
	freshNode := ring.GetNodeID(sector)
	staleNode := "s1"
	if staleNode == freshNode {
		staleNode = "s2"
	}

	currentObjects := []rpc.Object{
		makeTestObject("va", 20, "etag-a", map[string]uint64{"writer-a": 1}),
		makeTestObject("vb", 21, "etag-b", map[string]uint64{"writer-b": 1}),
	}

	servers[freshNode].installObjects(key, currentObjects)
	servers[staleNode].installObjects(key, []rpc.Object{currentObjects[0]})

	neighborSector := replicaSectorForNode(ring, sector, staleNode)
	if neighborSector < 0 {
		t.Fatalf("failed to find replica sector for node %q", staleNode)
	}

	servers[freshNode].Reconcile(sector, neighborSector)

	if got := servers[staleNode].GetSiblings(key); !IsSameSiblings(currentObjects, got) {
		t.Fatalf("expected reconcile to repair stale replica, got %v want %v", got, currentObjects)
	}
}

func TestStartAntiEntropyRepairsStaleReplica(t *testing.T) {
	ring, servers, cleanup := makeSmallAntiEntropyCluster(t)
	defer cleanup()

	const key = "anti-entropy-background"
	sector, _ := ring.GetLocation(key)
	freshNode := ring.GetNodeID(sector)
	staleNode := "s1"
	if staleNode == freshNode {
		staleNode = "s2"
	}

	currentObjects := []rpc.Object{
		makeTestObject("va", 20, "etag-a", map[string]uint64{"writer-a": 1}),
		makeTestObject("vb", 21, "etag-b", map[string]uint64{"writer-b": 1}),
	}

	servers[freshNode].installObjects(key, currentObjects)
	servers[staleNode].installObjects(key, []rpc.Object{currentObjects[0]})
	servers[freshNode].antiEntropyInterval = 10 * time.Millisecond

	before := servers[staleNode].GetSiblings(key)
	if IsSameSiblings(currentObjects, before) {
		t.Fatal("expected stale replica to start out outdated")
	}

	servers[freshNode].StartAntiEntropy()

	deadline := time.Now().Add(2 * time.Second) // 10 seconds
	for time.Now().Before(deadline) {
		if got := servers[staleNode].GetSiblings(key); IsSameSiblings(currentObjects, got) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	final := servers[staleNode].GetSiblings(key)
	t.Fatalf("expected background anti-entropy to repair stale replica, got %v want %v", final, currentObjects)
}

func TestStartKVServerStartsAntiEntropy(t *testing.T) {
	// Keep the sector space small so this test verifies automatic startup, not random-sector timing.
	oldNumServers, oldNumSectors, oldNumReplicas := numServers, numSectors, numReplicas
	oldInterval := defaultAntiEntropyInterval
	numServers, numSectors, numReplicas = 2, 2, 2
	defaultAntiEntropyInterval = 10 * time.Millisecond
	defer func() {
		numServers, numSectors, numReplicas = oldNumServers, oldNumSectors, oldNumReplicas
		defaultAntiEntropyInterval = oldInterval
	}()

	net := labrpc.MakeNetwork()
	defer net.Cleanup()
	
	nodeIDs := []string{
		tester.ServerName(tester.GRP0, 0),
		tester.ServerName(tester.GRP0, 1),
	}

	ends := make([][]*labrpc.ClientEnd, len(nodeIDs))
	for i, from := range nodeIDs {
		ends[i] = make([]*labrpc.ClientEnd, len(nodeIDs))
		for j, to := range nodeIDs {
			endName := from + "->" + to
			end := net.MakeEnd(endName)
			net.Connect(endName, to)
			net.Enable(endName, true)
			ends[i][j] = end
		}
	}

	servers := make(map[string]*KVServer, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		started := StartKVServer(nil, ends[i], tester.GRP0, i, nil)
		kv := started[0].(*KVServer)
		servers[nodeID] = kv
	}
	defer func() {
		for _, kv := range servers {
			close(kv.stopCh)
		}
	}()

	for _, nodeID := range nodeIDs {
		rs := labrpc.MakeServer()
		rs.AddService(labrpc.MakeService(servers[nodeID]))
		net.AddServer(nodeID, rs)
	}

	ring := servers[nodeIDs[0]].ring

	const key = "anti-entropy-startkvserver"
	sector, _ := ring.GetLocation(key)
	freshNode := ring.GetNodeID(sector)
	staleNode := nodeIDs[0]
	if staleNode == freshNode {
		staleNode = nodeIDs[1]
	}

	currentObjects := []rpc.Object{
		makeTestObject("va", 20, "etag-a", map[string]uint64{"writer-a": 1}),
		makeTestObject("vb", 21, "etag-b", map[string]uint64{"writer-b": 1}),
	}

	servers[freshNode].installObjects(key, currentObjects)
	servers[staleNode].installObjects(key, []rpc.Object{currentObjects[0]})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := servers[staleNode].GetSiblings(key); IsSameSiblings(currentObjects, got) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	final := servers[staleNode].GetSiblings(key)
	t.Fatalf("expected StartKVServer to start anti-entropy automatically, got %v want %v", final, currentObjects)
}

func TestStartAntiEntropyEventuallyRepairsStaleReplicaUnreliable(t *testing.T) {
	ring, servers, cleanup := makeSmallAntiEntropyCluster(t)
	defer cleanup()

	const key = "anti-entropy-unreliable"
	sector, _ := ring.GetLocation(key)
	freshNode := ring.GetNodeID(sector)
	staleNode := "s1"
	if staleNode == freshNode {
		staleNode = "s2"
	}

	currentObjects := []rpc.Object{
		makeTestObject("va", 20, "etag-a", map[string]uint64{"writer-a": 1}),
		makeTestObject("vb", 21, "etag-b", map[string]uint64{"writer-b": 1}),
	}

	servers[freshNode].installObjects(key, currentObjects)
	servers[staleNode].installObjects(key, []rpc.Object{currentObjects[0]})
	servers[freshNode].antiEntropyInterval = 10 * time.Millisecond
	for _, kv := range servers {
		for _, end := range kv.ends {
			end.SetCall(func(endname, svcMeth string, args []byte) ([]byte, bool) {
				if time.Now().UnixNano()%3 == 0 {
					return nil, false
				}
				return end.Forward(svcMeth, args)
			})
		}
	}

	before := servers[staleNode].GetSiblings(key)
	if IsSameSiblings(currentObjects, before) {
		t.Fatal("expected stale replica to start out outdated")
	}

	servers[freshNode].StartAntiEntropy()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := servers[staleNode].GetSiblings(key); IsSameSiblings(currentObjects, got) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	final := servers[staleNode].GetSiblings(key)
	t.Fatalf("expected background anti-entropy to repair stale replica under unreliable network, got %v want %v", final, currentObjects)
}
