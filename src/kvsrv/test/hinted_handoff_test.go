package kvsrv

import (
	"testing"
	"time"

	"dynamo-kvsrv/chr"
	"dynamo-kvsrv/kvsrv1/rpc"
	"dynamo-kvsrv/labrpc"
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

func findHintHolderForTarget(nodeIDs []string, servers map[string]*KVServer, target string) string {
	for _, nodeID := range nodeIDs {
		if len(servers[nodeID].GetHints(target)) > 0 {
			return nodeID
		}
	}
	return ""
}

func totalHintsForTarget(nodeIDs []string, servers map[string]*KVServer, target string) int {
	total := 0
	for _, nodeID := range nodeIDs {
		total += len(servers[nodeID].GetHints(target))
	}
	return total
}

func holdersForTarget(nodeIDs []string, servers map[string]*KVServer, target string) []string {
	holders := make([]string, 0)
	for _, nodeID := range nodeIDs {
		if len(servers[nodeID].GetHints(target)) > 0 {
			holders = append(holders, nodeID)
		}
	}
	return holders
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

	deadline := time.Now().Add(200 * time.Millisecond)
	handoffHolder := ""
	for time.Now().Before(deadline) {
		handoffHolder = findHintHolderForTarget(nodeIDs, servers, deadReplica)
		if handoffHolder != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if handoffHolder == "" {
		t.Fatalf("expected a hinted write holder for target %s", deadReplica)
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

	deadline := time.Now().Add(200 * time.Millisecond)
	handoffHolder := ""
	for time.Now().Before(deadline) {
		handoffHolder = findHintHolderForTarget(nodeIDs, servers, deadReplica)
		if handoffHolder != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if handoffHolder == "" {
		t.Fatalf("expected a hinted write holder for target %s", deadReplica)
	}
	markMemberAlive(servers[handoffHolder], deadReplica)
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		servers[handoffHolder].replayHints(deadReplica)
		if hints := servers[handoffHolder].GetHints(deadReplica); len(hints) == 0 {
			if got := servers[deadReplica].GetSiblings(key); len(got) == 1 && got[0].Value == "value" {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected recovered target to receive replayed write, got %v", servers[deadReplica].GetSiblings(key))
}

func TestHintedHandoffEventuallyReplaysUnderUnreliableNetwork(t *testing.T) {
	ring, nodeIDs, servers, cleanup := makeHintedHandoffCluster(t)
	defer cleanup()

	const key = "hint-replay-unreliable"
	coordinator := ring.GetCoordinator(key)
	prefList := ring.GetPreferenceList(key)
	deadReplica := prefList[0]
	if deadReplica == coordinator {
		deadReplica = prefList[1]
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

	deadline := time.Now().Add(200 * time.Millisecond)
	handoffHolder := ""
	for time.Now().Before(deadline) {
		handoffHolder = findHintHolderForTarget(nodeIDs, servers, deadReplica)
		if handoffHolder != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if handoffHolder == "" {
		t.Fatalf("expected a hinted write holder for target %s", deadReplica)
	}
	markMemberDeadForHintTest(servers[handoffHolder], deadReplica)
	end := servers[handoffHolder].ends[deadReplica]
	end.SetCall(func(endname, svcMeth string, args []byte) ([]byte, bool) {
		if time.Now().UnixNano()%3 != 0 {
			return nil, false
		}
		return end.Forward(svcMeth, args)
	})

	markMemberAlive(servers[handoffHolder], deadReplica)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		servers[handoffHolder].replayHints(deadReplica)
		if hints := servers[handoffHolder].GetHints(deadReplica); len(hints) == 0 {
			if got := servers[deadReplica].GetSiblings(key); len(got) == 1 && got[0].Value == "value" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	finalHints := servers[handoffHolder].GetHints(deadReplica)
	finalObjects := servers[deadReplica].GetSiblings(key)
	t.Fatalf("hinted handoff did not converge under unreliable network; hints=%d objects=%v", len(finalHints), finalObjects)
}

func TestHintedHandoffStressMultipleKeysUnreliable(t *testing.T) {
	ring, nodeIDs, servers, cleanup := makeHintedHandoffCluster(t)
	defer cleanup()

	const totalKeys = 12
	baseKey := "hint-stress-base"
	coordinator := ring.GetCoordinator(baseKey)
	prefList := ring.GetPreferenceList(baseKey)
	deadReplica := prefList[0]
	if deadReplica == coordinator {
		deadReplica = prefList[1]
	}

	for _, nodeID := range nodeIDs {
		markMemberDeadForHintTest(servers[nodeID], deadReplica)
	}

	writtenKeys := make([]string, 0, totalKeys)
	for i := 0; len(writtenKeys) < totalKeys && i < 2000; i++ {
		key := baseKey + "-" + string(rune('a'+(i%26))) + "-" + string(rune('A'+(i/26)))
		keyPrefList := ring.GetPreferenceList(key)
		containsDead := false
		for _, serverID := range keyPrefList {
			if serverID == deadReplica {
				containsDead = true
				break
			}
		}
		if !containsDead {
			continue
		}
		keyCoordinator := ring.GetCoordinator(key)
		reply := rpc.PutReply{}
		servers[keyCoordinator].CoordPut(&rpc.PutArgs{
			Key:         key,
			Object:      rpc.Object{Value: key},
			BaseContext: rpc.NewContext(),
		}, &reply)
		if reply.Err != rpc.OK && reply.Err != rpc.ErrWriteQuorumNotMet {
			t.Fatalf("put for key %q failed: %v", key, reply.Err)
		}
		writtenKeys = append(writtenKeys, key)
	}
	if len(writtenKeys) == 0 {
		t.Fatal("expected to generate at least one key mapped to the chosen dead replica")
	}

	if totalHintsForTarget(nodeIDs, servers, deadReplica) == 0 {
		t.Fatalf("expected hinted writes before replay for target %s", deadReplica)
	}
	for _, holder := range holdersForTarget(nodeIDs, servers, deadReplica) {
		end := servers[holder].ends[deadReplica]
		end.SetCall(func(endname, svcMeth string, args []byte) ([]byte, bool) {
			if time.Now().UnixNano()%4 != 0 {
				return nil, false
			}
			return end.Forward(svcMeth, args)
		})
		markMemberAlive(servers[holder], deadReplica)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, holder := range holdersForTarget(nodeIDs, servers, deadReplica) {
			servers[holder].replayHints(deadReplica)
		}
		remaining := totalHintsForTarget(nodeIDs, servers, deadReplica)
		if remaining == 0 {
			ok := true
			for _, key := range writtenKeys {
				if got := servers[deadReplica].GetSiblings(key); len(got) == 0 {
					ok = false
					break
				}
			}
			if ok {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("stress replay did not converge; remaining hints=%d", totalHintsForTarget(nodeIDs, servers, deadReplica))
}
