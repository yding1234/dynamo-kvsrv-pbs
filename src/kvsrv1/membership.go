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

func (kv *KVServer) bumpLocalHeartbeat() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.heartbeatCounter++
	self := kv.members[kv.id]
	self.Update(kv.heartbeatCounter, rpc.Alive)
	kv.members[kv.id] = self
}

func (kv *KVServer) SyncMembers() {
	membersToSync := kv.GetRandomNeighbors()
	if len(membersToSync) == 0 {
		return
	}

	snapshot := kv.GetAllMembers()
	for _, member := range membersToSync {
		go func(member rpc.MemberInfo) {
			args := rpc.SyncMembersArgs{MemberInfos: snapshot}
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

func (kv *KVServer) mergeMembers(remote []rpc.MemberInfo) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, member := range remote {
		current, ok := kv.members[member.ServerID]
		if !ok {
			member.LastUpdated = time.Now()
			kv.members[member.ServerID] = member
			continue
		}

		if member.ServerID == kv.id {
			if member.Heartbeat > current.Heartbeat {
				current.Update(member.Heartbeat, rpc.Alive)
				kv.heartbeatCounter = member.Heartbeat
				kv.members[member.ServerID] = current
			}
			continue
		}

		if member.Heartbeat > current.Heartbeat {
			current.Update(member.Heartbeat, member.Status)
			kv.members[member.ServerID] = current
			continue
		}
		if member.Heartbeat < current.Heartbeat {
			continue
		}

		if member.IsWorse(current) {
			current.Update(member.Heartbeat, member.Status)
			kv.members[member.ServerID] = current
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

		since := now.Sub(member.LastUpdated)
		switch {
		case since >= kv.cleanupTimeout:
			if member.Status != rpc.Dead {
				member.Update(member.Heartbeat, rpc.Dead)
				kv.members[serverID] = member
			}
		case since >= kv.failureTimeout:
			if member.Status == rpc.Alive {
				member.Update(member.Heartbeat, rpc.Suspect)
				kv.members[serverID] = member
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



