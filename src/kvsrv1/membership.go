package kvsrv

import (
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
)

func (kv *KVServer) StartSyncMembers() {
	go func() {
		ticker := time.NewTicker(kv.gossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				kv.bumpLocalHeartbeat()
				kv.SyncMembers()
			case <-kv.stopCh:
				return
			}
		}
	}()
}

func (kv *KVServer) StartMembershipFailureDetector() {
	go func() {
		ticker := time.NewTicker(kv.heartbeatTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				kv.detectMemberFailures()
			case <-kv.stopCh:
				return
			}
		}
	}()
}

// increment local heartbeat counter and update local member info
func (kv *KVServer) bumpLocalHeartbeat() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.heartbeatCounter++
	kv.UpdateMemberInfo(kv.id, kv.heartbeatCounter, rpc.Alive)
}

func (kv *KVServer) UpdateMemberInfo(serverID string, heartbeat uint64, status rpc.NodeStatus) {
	member := kv.members[serverID]
	member.Update(heartbeat, status)
	kv.members[serverID] = member
	kv.memberLastUpdated[serverID] = time.Now()
}

func (kv *KVServer) SyncMembers() {
	membersToSync := kv.GetRandomNeighbors()
	if len(membersToSync) == 0 {
		return
	}

	allMembers := kv.GetAllMembers()
	for _, member := range membersToSync {
		go func(member rpc.MemberInfo) {
			args := rpc.SyncMembersArgs{MemberInfos: allMembers}
			reply := rpc.SyncMembersReply{}
			ok := kv.ends[member.ServerID].Call("KVServer.GossipSyncMembers", &args, &reply)
			if ok {
				kv.mergeMembers(reply.MemberInfos)
			}
		}(member)
	}
}

func (kv *KVServer) GossipSyncMembers(args *rpc.SyncMembersArgs, reply *rpc.SyncMembersReply) {
	kv.mergeMembers(args.MemberInfos)
	reply.MemberInfos = kv.GetAllMembers()
}

func (kv *KVServer) mergeMembers(memberInfos []rpc.MemberInfo) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, member := range memberInfos {
		serverID := member.ServerID
		myMember, ok := kv.members[serverID]

		// if the member is not in my members, add it
		if !ok {
			kv.members[serverID] = member
			kv.memberLastUpdated[serverID] = time.Now()
			continue
		}

		// if the member is in my members, update it if it has a higher heartbeat or it has the same heartbeat but is worse
		heartbeat := member.Heartbeat
		myHeartbeat := myMember.Heartbeat
		if heartbeat > myHeartbeat ||
			(heartbeat == myHeartbeat && member.IsWorse(myMember)) {
			kv.UpdateMemberInfo(serverID, heartbeat, member.Status)
		}
	}
}

func (kv *KVServer) detectMemberFailures() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	now := time.Now()
	for serverID, member := range kv.members {
		if serverID == kv.id {
			continue
		}

		lastUpdated, ok := kv.memberLastUpdated[serverID]
		if !ok {
			lastUpdated = now
			kv.memberLastUpdated[serverID] = now
		}
		since := now.Sub(lastUpdated)
		switch {
			case since >= kv.cleanupTimeout:
				if member.Status != rpc.Dead {
					kv.UpdateMemberInfo(serverID, member.Heartbeat, rpc.Dead)
				}
			case since >= kv.failureTimeout:
				if member.Status == rpc.Alive {
					kv.UpdateMemberInfo(serverID, member.Heartbeat, rpc.Suspect)
				}
		}
	}
}

func (kv *KVServer) GetRandomNeighbors() []rpc.MemberInfo {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	candidates := make([]rpc.MemberInfo, 0, len(kv.members))
	for _, member := range kv.members {
		if member.ServerID == kv.id {
			continue
		}
		candidates = append(candidates, member)
	}

	if len(candidates) == 0 {
		return nil
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	n := kv.numNeighbors
	if n <= 0 || n > len(candidates) {
		n = len(candidates)
	}
	return append([]rpc.MemberInfo(nil), candidates[:n]...)
}

func (kv *KVServer) GetAllMembers() []rpc.MemberInfo {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	memberInfos := make([]rpc.MemberInfo, 0, len(kv.members))
	for _, memberInfo := range kv.members {
		memberInfos = append(memberInfos, memberInfo)
	}
	return memberInfos
}



