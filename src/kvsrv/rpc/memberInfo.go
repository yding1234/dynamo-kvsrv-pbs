package rpc

type NodeStatus int

const (
    Alive NodeStatus = iota
    Suspect
    Dead
)

type MemberInfo struct {
	ServerID    string
	Heartbeat   uint64
	Status      NodeStatus
}

func (m *MemberInfo) Update(heartbeat uint64, status NodeStatus) {
	m.Heartbeat = heartbeat
	m.Status = status
}

func (m MemberInfo) IsIn(selectedMembers []MemberInfo) bool {
	for _, selectedMember := range selectedMembers {
		if m.ServerID == selectedMember.ServerID {
			return true
		}
	}
	return false
}

// true if m is worse than other, false otherwise
func (m MemberInfo) IsWorse(other MemberInfo) bool {
	return m.Status > other.Status
}

func (m MemberInfo) IsAlive() bool {
	return m.Status == Alive
}

func (m MemberInfo) IsSuspect() bool {
	return m.Status == Suspect
}

func (m MemberInfo) IsDead() bool {
	return m.Status == Dead
}