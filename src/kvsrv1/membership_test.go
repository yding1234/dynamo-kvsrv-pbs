package kvsrv

import (
	"testing"
	"time"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
)

func makeMembershipUnitServer(id string, nodeIDs []string) *KVServer {
	ends := make(map[string]*labrpc.ClientEnd, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		ends[nodeID] = &labrpc.ClientEnd{}
	}
	ring := chr.MakeConsistentHashRing(1, 4, len(nodeIDs), nodeIDs)
	return MakeKVServer(id, ring, 1, 1, ends)
}

func TestGossipSyncMembersPropagatesHeartbeat(t *testing.T) {
	nodeIDs := []string{"s1", "s2"}
	s1 := makeMembershipUnitServer("s1", nodeIDs)
	s2 := makeMembershipUnitServer("s2", nodeIDs)

	s1.bumpLocalHeartbeat()
	args := rpc.SyncMembersArgs{MemberInfos: s1.GetAllMembers()}
	reply := rpc.SyncMembersReply{}

	s2.GossipSyncMembers(&args, &reply)

	s2.mu.Lock()
	got := s2.members["s1"]
	s2.mu.Unlock()
	if got.Heartbeat != 1 {
		t.Fatalf("expected propagated heartbeat 1, got %d", got.Heartbeat)
	}
	if got.Status != rpc.Alive {
		t.Fatalf("expected propagated status alive, got %v", got.Status)
	}
}

func TestMergeMembersSameHeartbeatPrefersWorseStatus(t *testing.T) {
	nodeIDs := []string{"s1", "s2"}
	kv := makeMembershipUnitServer("s1", nodeIDs)

	kv.mu.Lock()
	member := kv.members["s2"]
	member.Heartbeat = 7
	member.Status = rpc.Alive
	member.LastUpdated = time.Now()
	kv.members["s2"] = member
	kv.mu.Unlock()

	kv.mergeMembers([]rpc.MemberInfo{{
		ServerID:    "s2",
		Heartbeat:   7,
		Status:      rpc.Suspect,
		LastUpdated: time.Now(),
	}})

	kv.mu.Lock()
	got := kv.members["s2"]
	kv.mu.Unlock()
	if got.Status != rpc.Suspect {
		t.Fatalf("expected suspect to win for equal heartbeat, got %v", got.Status)
	}
}

func TestDetectMemberFailuresMarksSuspectAndDead(t *testing.T) {
	nodeIDs := []string{"s1", "s2"}
	kv := makeMembershipUnitServer("s1", nodeIDs)
	kv.failureTimeout = 50 * time.Millisecond
	kv.cleanupTimeout = 100 * time.Millisecond

	kv.mu.Lock()
	member := kv.members["s2"]
	member.LastUpdated = time.Now().Add(-75 * time.Millisecond)
	member.Status = rpc.Alive
	kv.members["s2"] = member
	kv.mu.Unlock()

	kv.detectMemberFailures()

	kv.mu.Lock()
	got := kv.members["s2"]
	kv.mu.Unlock()
	if got.Status != rpc.Suspect {
		t.Fatalf("expected suspect after failure timeout, got %v", got.Status)
	}

	kv.mu.Lock()
	member = kv.members["s2"]
	member.LastUpdated = time.Now().Add(-150 * time.Millisecond)
	member.Status = rpc.Suspect
	kv.members["s2"] = member
	kv.mu.Unlock()

	kv.detectMemberFailures()

	kv.mu.Lock()
	got = kv.members["s2"]
	kv.mu.Unlock()
	if got.Status != rpc.Dead {
		t.Fatalf("expected dead after cleanup timeout, got %v", got.Status)
	}
}
