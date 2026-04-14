package kvsrv

import (
	"testing"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
)

func makeHintedHandoffCluster(t *testing.T) (*chr.ConsistentHashRing, []string, map[string]*KVServer, func()) {
	t.Helper()

	nodeIDs := []string{"s1", "s2", "s3", "s4"}
	ring := chr.MakeConsistentHashRing(3, 8, len(nodeIDs), nodeIDs)
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
		servers[nodeID] = MakeKVServer(nodeID, ring, 2, 2, ends[nodeID])
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
	return ring, nodeIDs, servers, cleanup
}

func markMemberAlive(kv *KVServer, serverID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	member := kv.members[serverID]
	member.Status = rpc.Alive
	kv.members[serverID] = member
}

func markMemberDeadForHintTest(kv *KVServer, serverID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	member := kv.members[serverID]
	member.Status = rpc.Dead
	kv.members[serverID] = member
}

func findHandoffHolder(nodeIDs []string, prefList []string) string {
	inPref := make(map[string]bool, len(prefList))
	for _, serverID := range prefList {
		inPref[serverID] = true
	}
	for _, serverID := range nodeIDs {
		if !inPref[serverID] {
			return serverID
		}
	}
	return ""
}

func TestCoordPutStoresHintForDeadReplica(t *testing.T) {
	ring, nodeIDs, servers, cleanup := makeHintedHandoffCluster(t)
	defer cleanup()

	const key = "hint-store"
	coordinator := ring.GetCoordinator(key)
	prefList := ring.GetPreferenceList(key)
	deadReplica := prefList[0]
	if deadReplica == coordinator {
		deadReplica = prefList[1]
	}
	handoffHolder := findHandoffHolder(nodeIDs, prefList)
	if handoffHolder == "" {
		t.Fatal("expected a non-preference handoff holder")
	}

	markMemberDeadForHintTest(servers[coordinator], deadReplica)

	reply := rpc.PutReply{}
	servers[coordinator].CoordPut(&rpc.PutArgs{
		Key:         key,
		Object:      rpc.Object{Value: "value"},
		BaseContext: rpc.NewContext(),
	}, &reply)
	if reply.Err != rpc.OK {
		t.Fatalf("expected put to succeed with hinted handoff, got %v", reply.Err)
	}

	hints := servers[handoffHolder].GetHints(deadReplica)
	if len(hints) != 1 {
		t.Fatalf("expected 1 hinted write stored for target %s, got %d", deadReplica, len(hints))
	}
	if hints[0].Key != key || hints[0].Object.Value != "value" {
		t.Fatalf("unexpected hinted write: %+v", hints[0])
	}
	if got := servers[deadReplica].GetSiblings(key); len(got) != 0 {
		t.Fatalf("expected dead replica to miss direct write, got %v", got)
	}
}

func TestHintedHandoffReplaysWhenTargetRecovers(t *testing.T) {
	ring, nodeIDs, servers, cleanup := makeHintedHandoffCluster(t)
	defer cleanup()

	const key = "hint-replay"
	coordinator := ring.GetCoordinator(key)
	prefList := ring.GetPreferenceList(key)
	deadReplica := prefList[0]
	if deadReplica == coordinator {
		deadReplica = prefList[1]
	}
	handoffHolder := findHandoffHolder(nodeIDs, prefList)
	if handoffHolder == "" {
		t.Fatal("expected a non-preference handoff holder")
	}

	markMemberDeadForHintTest(servers[coordinator], deadReplica)

	reply := rpc.PutReply{}
	servers[coordinator].CoordPut(&rpc.PutArgs{
		Key:         key,
		Object:      rpc.Object{Value: "value"},
		BaseContext: rpc.NewContext(),
	}, &reply)
	if reply.Err != rpc.OK {
		t.Fatalf("expected put to succeed with hinted handoff, got %v", reply.Err)
	}

	markMemberAlive(servers[handoffHolder], deadReplica)
	servers[handoffHolder].replayHints(deadReplica)

	if hints := servers[handoffHolder].GetHints(deadReplica); len(hints) != 0 {
		t.Fatalf("expected hinted write to be removed after replay, got %d", len(hints))
	}
	if got := servers[deadReplica].GetSiblings(key); len(got) != 1 || got[0].Value != "value" {
		t.Fatalf("expected recovered target to receive replayed write, got %v", got)
	}
}
