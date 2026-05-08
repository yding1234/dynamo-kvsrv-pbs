package chr

// go test ./chr -v -race

import (
	"testing"
)

func TestMakeConsistentHashRing_sectorCoverage(t *testing.T) {
	const Q, S, N = 100, 5, 3
	chr := MakeConsistentHashRing(N, Q, S)

	if chr.numServers != S {
		t.Fatalf("numServers: got %d want %d", chr.numServers, S)
	}
	if len(chr.nodeIDs) != S {
		t.Fatalf("nodeIDs len: got %d want %d", len(chr.nodeIDs), S)
	}
	if len(chr.sectorMap) != Q {
		t.Fatalf("sectorMap len: got %d want %d", len(chr.sectorMap), Q)
	}

	seen := make(map[string]bool)
	for _, id := range chr.nodeIDs {
		seen[id] = true
	}

	for s := 0; s < Q; s++ {
		owner, ok := chr.sectorMap[s]
		if !ok {
			t.Fatalf("missing sector %d", s)
		}
		if !seen[owner] {
			t.Fatalf("sector %d owner %q not in nodeIDs", s, owner)
		}
	}

	total := 0
	for id, secs := range chr.nodes {
		if !seen[id] {
			t.Fatalf("unknown node id in nodes map: %q", id)
		}
		total += len(secs)
	}
	if total != Q {
		t.Fatalf("sum of sectors per node: got %d want %d", total, Q)
	}
}

func TestMakeConsistentHashRing_evenDistribution(t *testing.T) {
	const Q, S, N = 60, 4, 3
	chr := MakeConsistentHashRing(N, Q, S)

	want := Q / S
	for id, secs := range chr.nodes {
		if len(secs) != want {
			t.Fatalf("node %q has %d sectors, want %d", id, len(secs), want)
		}
	}
}

func TestFindCoordinator_returnsValidNode(t *testing.T) {
	chr := MakeConsistentHashRing(2, 32, 4)
	valid := make(map[string]struct{})
	for _, id := range chr.nodeIDs {
		valid[id] = struct{}{}
	}
	for _, key := range []string{"a", "b", "hello", "k", "z"} {
		c := chr.FindCoordinator(key)
		if _, ok := valid[c]; !ok {
			t.Fatalf("FindCoordinator(%q)=%q not in ring", key, c)
		}
	}
}

func TestGeneratePreferenceList_lengthAndNoDuplicates(t *testing.T) {
	const Q, S = 24, 3
	N := 3
	chr := MakeConsistentHashRing(N, Q, S)

	pl := chr.GeneratePreferenceList("mykey")
	if len(pl) != N {
		t.Fatalf("len pref list: got %d want %d", len(pl), N)
	}
	seen := make(map[string]bool)
	for _, id := range pl {
		if seen[id] {
			t.Fatalf("duplicate in preference list: %q", id)
		}
		seen[id] = true
	}
}

func TestGeneratePreferenceList_firstIsCoordinator(t *testing.T) {
	chr := MakeConsistentHashRing(3, 40, 5)
	key := "coord-check"
	coord := chr.FindCoordinator(key)
	pl := chr.GeneratePreferenceList(key)
	if len(pl) == 0 {
		t.Fatal("empty preference list")
	}
	if pl[0] != coord {
		t.Fatalf("pref[0]=%q coordinator=%q", pl[0], coord)
	}
}

func TestAddNode_preservesSectorCount(t *testing.T) {
	const Q, S0 = 100, 4
	chr := MakeConsistentHashRing(2, Q, S0)
	before := len(chr.sectorMap)

	chr.AddNode("new-node-alpha")

	if chr.numServers != S0+1 {
		t.Fatalf("numServers: got %d want %d", chr.numServers, S0+1)
	}
	if len(chr.sectorMap) != before {
		t.Fatalf("sectorMap size changed: got %d want %d", len(chr.sectorMap), before)
	}

	for s := 0; s < Q; s++ {
		if _, ok := chr.sectorMap[s]; !ok {
			t.Fatalf("missing sector %d after AddNode", s)
		}
	}
	total := 0
	for _, secs := range chr.nodes {
		total += len(secs)
	}
	if total != Q {
		t.Fatalf("total sectors after AddNode: got %d want %d", total, Q)
	}
}

func TestRemoveNode_preservesAllSectors(t *testing.T) {
	const Q, S = 36, 3
	chr := MakeConsistentHashRing(2, Q, S)
	victim := chr.nodeIDs[0]

	chr.RemoveNode(victim)

	if chr.numServers != S-1 {
		t.Fatalf("numServers: got %d want %d", chr.numServers, S-1)
	}
	for _, id := range chr.nodeIDs {
		if id == victim {
			t.Fatalf("victim still in nodeIDs")
		}
	}
	for s := 0; s < Q; s++ {
		owner, ok := chr.sectorMap[s]
		if !ok || owner == "" {
			t.Fatalf("sector %d bad owner after remove: %q ok=%v", s, owner, ok)
		}
		if owner == victim {
			t.Fatalf("sector %d still owned by removed node", s)
		}
	}
	total := 0
	for _, secs := range chr.nodes {
		total += len(secs)
	}
	if total != Q {
		t.Fatalf("total sectors after RemoveNode: got %d want %d", total, Q)
	}
}

func TestCustomHashFunc_deterministicCoordinator(t *testing.T) {
	chr := &ConsistentHashRing{
		numReplicas: 1,
		numSectors:  10,
		numServers:  2,
		hashFunc:    func(string) uint32 { return 0 },
		nodeIDs:     []string{"A", "B"},
		nodes: map[string][]int{
			"A": {0, 1, 2, 3, 4},
			"B": {5, 6, 7, 8, 9},
		},
		sectorMap: map[int]string{
			0: "A", 1: "A", 2: "A", 3: "A", 4: "A",
			5: "B", 6: "B", 7: "B", 8: "B", 9: "B",
		},
	}
	if c := chr.FindCoordinator("anything"); c != "A" {
		t.Fatalf("want coordinator A, got %q", c)
	}
}