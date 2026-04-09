package vclock

type VClock map[string]uint64 // node name -> version value

const (
	Equal      = 0
	Before     = -1
	After      = 1
	Concurrent = 2
)

func NewVClock() VClock {
	return make(VClock)
}

func (v VClock) Increment(node string) {
	v[node]++
}

func (v VClock) SetVersion(node string, version uint64) {
	v[node] = version
}

func (v VClock) GetAllNodes(other VClock) map[string]struct{} {
	nodes := make(map[string]struct{})
	for node, _ := range v {
		nodes[node] = struct{}{}
	}
	for node, _ := range other {
		nodes[node] = struct{}{}
	}
	return nodes
}

func (v VClock) GetNodes() []string {
	nodes := make([]string, 0, len(v))
	for node := range v {
		nodes = append(nodes, node)
	}
	return nodes
}

func (v VClock) GetVersion(node string) uint64 {
	return v[node]
}

func (v VClock) Copy() VClock {
	cp := NewVClock()
	for node, ver := range v {
		cp[node] = ver
	}
	return cp
}

func (v VClock) IsInitial() bool {
	for _, ver := range v {
		if ver != 0 {
			return false
		}
	}
	return true
}

func (v VClock) Compare(other VClock) int {
	hasBefore := false
	hasAfter := false

	nodes := v.GetAllNodes(other)

	for node := range nodes {
		if v[node] < other[node] {
			hasBefore = true
		} else if v[node] > other[node] {
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

func (v VClock) Merge(other VClock) {
	nodes := v.GetAllNodes(other)
	for node := range nodes {
		v[node] = max(v[node], other[node])
	}
}
