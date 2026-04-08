package vclock

type VClock struct {
	Clocks map[string]uint64 // node name -> counter/version value
}

const (
	Equal      = 0
	Before     = -1
	After      = 1
	Concurrent = 2
)

func NewVClock() *VClock {
	return &VClock{Clocks: make(map[string]uint64)}
}

func (v *VClock) Increment(node string) {
	if v.Clocks == nil {
		v.Clocks = make(map[string]uint64)
	}
	v.Clocks[node] = v.Clocks[node] + 1
}

func (v *VClock) SetVersion(node string, version uint64) {
	if v.Clocks == nil {
		v.Clocks = make(map[string]uint64)
	}
	v.Clocks[node] = version
}

func (v *VClock) GetAllNodes(other *VClock) map[string]struct{} {
	// get all the nodes in the vclock
	nodes := make(map[string]struct{})
	for node := range v.Clocks {
		nodes[node] = struct{}{}
	}
	for node := range other.Clocks {
		nodes[node] = struct{}{}
	}
	return nodes
}

func (v *VClock) GetNodes() []string {
	nodes := make([]string, 0, len(v.Clocks))
	for node := range v.Clocks {
		nodes = append(nodes, node)
	}
	return nodes
}

func (v *VClock) GetVersion(node string) uint64 {
	if v.Clocks == nil {
		return 0
	}
	return v.Clocks[node]
}

func (v *VClock) Copy() *VClock {
	cp := NewVClock()
	for node, ver := range v.Clocks {
		cp.Clocks[node] = ver
	}
	return cp
}

func (v *VClock) Compare(other *VClock) int {
	hasBefore := false
	hasAfter := false

	nodes := v.GetAllNodes(other)

	for node := range nodes {
		if v.Clocks[node] < other.Clocks[node] {
			hasBefore = true
		} else if v.Clocks[node] > other.Clocks[node] {
			hasAfter = true
		}
		if hasBefore && hasAfter {
			return Concurrent
		}
	}

	if hasBefore && !hasAfter {
		return Before
	} else if !hasBefore && hasAfter {
		return After
	} else if !hasBefore && !hasAfter {
		return Equal
	}
	return Concurrent
}

func (v *VClock) Merge(other *VClock) {
	nodes := v.GetAllNodes(other)
	for node := range nodes {
		v.Clocks[node] = max(v.Clocks[node], other.Clocks[node])
	}
}
