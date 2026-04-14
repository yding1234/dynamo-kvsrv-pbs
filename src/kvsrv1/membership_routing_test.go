package kvsrv

import (
	"testing"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
)

func makeMembershipRoutingCluster(t *testing.T) (*chr.ConsistentHashRing, map[string]*KVServer, func()) {
	t.Helper()

	nodeIDs := []string{"s1", "s2", "s3"}
	ring := chr.MakeConsistentHashRing(3, 6, len(nodeIDs), nodeIDs)
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
	return ring, servers, cleanup
}

func markMemberDead(kv *KVServer, serverID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	member := kv.members[serverID]
	member.Status = rpc.Dead
	kv.members[serverID] = member
}

func TestCoordGetSkipsDeadReplica(t *testing.T) {
	ring, servers, cleanup := makeMembershipRoutingCluster(t)
	defer cleanup()

	const key = "route-get-skip-dead"
	prefList := ring.GetPreferenceList(key)
	coordinator := ring.GetCoordinator(key)
	deadReplica := prefList[0]
	if deadReplica == coordinator {
		deadReplica = prefList[1]
	}

	objects := []rpc.Object{
		makeTestObject("value", 1, "etag", map[string]uint64{"writer": 1}),
	}
	for _, serverID := range prefList {
		if serverID == deadReplica {
			continue
		}
		servers[serverID].installObjects(key, objects)
	}

	markMemberDead(servers[coordinator], deadReplica)

	reply := rpc.GetReply{}
	servers[coordinator].CoordGet(&rpc.GetArgs{Key: key}, &reply)
	if reply.Err != rpc.OK {
		t.Fatalf("expected get to succeed with dead replica skipped, got %v", reply.Err)
	}
	if !IsSameSiblings(objects, reply.Objects) {
		t.Fatalf("expected get to return live replica objects, got %v want %v", reply.Objects, objects)
	}
}

func TestCoordGetReturnsQuorumNotMetWhenTooManyDead(t *testing.T) {
	ring, servers, cleanup := makeMembershipRoutingCluster(t)
	defer cleanup()

	const key = "route-get-quorum-dead"
	coordinator := ring.GetCoordinator(key)
	prefList := ring.GetPreferenceList(key)

	markMemberDead(servers[coordinator], prefList[1])
	markMemberDead(servers[coordinator], prefList[2])

	reply := rpc.GetReply{}
	servers[coordinator].CoordGet(&rpc.GetArgs{Key: key}, &reply)
	if reply.Err != rpc.ErrReadQuorumNotMet {
		t.Fatalf("expected ErrReadQuorumNotMet, got %v", reply.Err)
	}
}

func TestCoordPutSkipsDeadReplica(t *testing.T) {
	ring, servers, cleanup := makeMembershipRoutingCluster(t)
	defer cleanup()

	const key = "route-put-skip-dead"
	coordinator := ring.GetCoordinator(key)
	prefList := ring.GetPreferenceList(key)
	deadReplica := prefList[0]
	if deadReplica == coordinator {
		deadReplica = prefList[1]
	}

	markMemberDead(servers[coordinator], deadReplica)

	reply := rpc.PutReply{}
	servers[coordinator].CoordPut(&rpc.PutArgs{
		Key:         key,
		Object:      rpc.Object{Value: "value"},
		BaseContext: rpc.NewContext(),
	}, &reply)
	if reply.Err != rpc.OK {
		t.Fatalf("expected put to succeed with dead replica skipped, got %v", reply.Err)
	}

	if got := servers[deadReplica].GetSiblings(key); len(got) != 0 {
		t.Fatalf("expected dead replica to be skipped, but it stored %v", got)
	}

	liveWrites := 0
	for _, serverID := range prefList {
		if serverID == deadReplica {
			continue
		}
		if got := servers[serverID].GetSiblings(key); len(got) > 0 {
			liveWrites++
		}
	}
	if liveWrites < 2 {
		t.Fatalf("expected writes on live replicas to satisfy quorum, got %d", liveWrites)
	}
}

func TestCoordPutReturnsQuorumNotMetWhenTooManyDead(t *testing.T) {
	ring, servers, cleanup := makeMembershipRoutingCluster(t)
	defer cleanup()

	const key = "route-put-quorum-dead"
	coordinator := ring.GetCoordinator(key)
	prefList := ring.GetPreferenceList(key)

	markMemberDead(servers[coordinator], prefList[1])
	markMemberDead(servers[coordinator], prefList[2])

	reply := rpc.PutReply{}
	servers[coordinator].CoordPut(&rpc.PutArgs{
		Key:         key,
		Object:      rpc.Object{Value: "value"},
		BaseContext: rpc.NewContext(),
	}, &reply)
	if reply.Err != rpc.ErrWriteQuorumNotMet {
		t.Fatalf("expected ErrWriteQuorumNotMet, got %v", reply.Err)
	}
}
