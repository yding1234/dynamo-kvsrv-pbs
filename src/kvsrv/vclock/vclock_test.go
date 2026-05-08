package vclock

// go test ./vclock -v

import "testing"

func makeClock(vals map[string]uint64) VClock {
	v := NewVClock()
	for n, c := range vals {
		v[n] = c
	}
	return v
}

func TestNewVClockAndIncrement(t *testing.T) {
	v := NewVClock()
	if v == nil {
		t.Fatalf("NewVClock returned nil")
	}
	if len(v) != 0 {
		t.Fatalf("new clock should be empty, got %d entries", len(v))
	}

	v.Increment("A")
	v.Increment("A")
	v.Increment("B")

	if v["A"] != 2 || v["B"] != 1 {
		t.Fatalf("unexpected counters: A=%d B=%d", v["A"], v["B"])
	}
}

func TestGetAllNodesUnion(t *testing.T) {
	a := makeClock(map[string]uint64{"A": 1, "B": 2})
	b := makeClock(map[string]uint64{"B": 3, "C": 4})

	nodes := a.GetAllNodes(b)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes in union, got %d", len(nodes))
	}
	for _, n := range []string{"A", "B", "C"} {
		if _, ok := nodes[n]; !ok {
			t.Fatalf("missing node %q in union", n)
		}
	}
}

func TestCompareEqual(t *testing.T) {
	a := makeClock(map[string]uint64{"A": 1, "B": 2})
	b := makeClock(map[string]uint64{"A": 1, "B": 2})

	if got := a.Compare(b); got != Equal {
		t.Fatalf("expected Equal, got %d", got)
	}
}

func TestCompareBeforeAfter(t *testing.T) {
	a := makeClock(map[string]uint64{"A": 1, "B": 2})
	b := makeClock(map[string]uint64{"A": 2, "B": 3})

	if got := a.Compare(b); got != Before {
		t.Fatalf("expected Before, got %d", got)
	}
	if got := b.Compare(a); got != After {
		t.Fatalf("expected After, got %d", got)
	}
}

func TestCompareConcurrent(t *testing.T) {
	a := makeClock(map[string]uint64{"A": 3, "B": 1})
	b := makeClock(map[string]uint64{"A": 2, "B": 2})

	if got := a.Compare(b); got != Concurrent {
		t.Fatalf("expected Concurrent, got %d", got)
	}
	if got := b.Compare(a); got != Concurrent {
		t.Fatalf("expected Concurrent(reverse), got %d", got)
	}
}

func TestCompareMissingNodeAsZero(t *testing.T) {
	a := makeClock(map[string]uint64{"A": 1})
	b := makeClock(map[string]uint64{"A": 1, "B": 1})

	if got := a.Compare(b); got != Before {
		t.Fatalf("expected Before when missing node treated as 0, got %d", got)
	}
}

func TestMergeTakesMaxPerNode(t *testing.T) {
	a := makeClock(map[string]uint64{"A": 1, "B": 5})
	b := makeClock(map[string]uint64{"A": 3, "C": 2})

	a.Merge(b)

	want := map[string]uint64{"A": 3, "B": 5, "C": 2}
	if len(a) != len(want) {
		t.Fatalf("merge size mismatch: got %d want %d", len(a), len(want))
	}
	for n, w := range want {
		if got := a[n]; got != w {
			t.Fatalf("node %q merged counter mismatch: got %d want %d", n, got, w)
		}
	}
}
