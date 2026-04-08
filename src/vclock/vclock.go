package vclock

type VClock struct {
	clocks map[string]uint64 // node name -> counter value
}

const (
	Equal      = iota // 0
	Before            // 1
	After             // 2
	Concurrent        // 3
)

func NewVClock() *VClock {
	return &VClock{clocks: make(map[string]uint64)}
}

func (v *VClock) Increment(node string) {
	v.clocks[node] = v.clocks[node] + 1
}

func (v *VClock) GetAllNodes(other *VClock) map[string]struct{} {
	// get all the nodes in the vclock
	nodes := make(map[string]struct{})
	for node := range v.clocks {
		nodes[node] = struct{}{}
	}
	for node := range other.clocks {
		nodes[node] = struct{}{}
	}
	return nodes
}

func (v *VClock) Compare(other *VClock) int {
	hasBefore := false
	hasAfter := false

	nodes := v.GetAllNodes(other)

	for node := range nodes {
		if v.clocks[node] < other.clocks[node] {
			hasBefore = true
		} else if v.clocks[node] > other.clocks[node] {
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
		v.clocks[node] = max(v.clocks[node], other.clocks[node])
	}
}
