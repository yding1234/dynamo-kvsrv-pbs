package kvsrv

import (
	"math/rand"
	"time"
)

func (kv *KVServer) StartSyncMembers() {
	go func() {
		ticker := time.NewTicker(kv.gossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				kv.SyncMembers()
			case <-kv.stopCh:
				return
			}
		}
	}()
}

func (kv *KVServer) SyncMembers() {
	members := kv.members
	// select numNeighbors random members to sync with
	rand.Seed(time.Now().UnixNano())
	membersToSync := kv.GetRandomNeighbors()

	// send sync members request to random members
	ch := make(chan SyncMembersResult, kv.numNeighbors)

	type SyncMembersResult struct {
		MemberInfos []rpc.MemberInfo
		OK bool
	}
	for _, member := range membersToSync {
		go func(member rpc.MemberInfo) {
			ok := kv.ends[member.ServerID].Call("KVServer.GossipSyncMembers", &rpc.SyncMembersArgs{MemberInfos: members}, &rpc.SyncMembersReply{})
			if ok {
				ch <- SyncMembersResult{MemberInfos: reply.MemberInfos, OK: ok}
			}
			else {
				ch <- SyncMembersResult{MemberInfos: nil, OK: false}
			}
		}(member)
	}

	// collect results
	for i := 0; i < kv.numNeighbors; i++ {
		result := <-ch
		if result.OK {
			// check returned member infos, and update my members
			for _, member := range result.MemberInfos {
				if !member.IsIn(kv.members) ||
					member.Heartbeat > kv.members[member.ServerID].Heartbeat ||
					(member.Heartbeat == kv.members[member.ServerID].Heartbeat && member.IsWorse(kv.members[member.ServerID])) {
					kv.members[member.ServerID] = member
				}
			}
		}
	}
}

func (kv *KVServer) GossipSyncMembers(args *rpc.SyncMembersArgs, reply *rpc.SyncMembersReply) {
	myMembers := kv.members

	// member infos to reply, including missing and stale member infos
	replyMemberInfos := make([]rpc.MemberInfo, 0)

	missingMembers := kv.GetAllMembers()
	
	for _, member := range args.MemberInfos {
		myMemberInfo := kv.members[member.ServerID]
		// member is in my members
		if member.IsIn(myMembers) {
			missingMembers = deleteMember(missingMembers, member.ServerID)
			if member.Heartbeat > myMemberInfo.Heartbeat {
				myMemberInfo.Update(member.Heartbeat, member.Status)
				if member.IsDead() {
					kv.ring.RemoveNode(member.ServerID)
				}
			} else if member.Heartbeat < myMemberInfo.Heartbeat {
				replyMemberInfos = append(replyMemberInfos, member)
			} else { // heartbeat is the same
				if member.IsWorse(myMemberInfo) {
					myMemberInfo.Update(member.Heartbeat, member.Status)
				} else if myMemberInfo.IsWorse(member) {
					replyMemberInfos = append(replyMemberInfos, myMemberInfo)
				}
			}
		} else { // member is not in my members
			if member.IsAlive() {
				kv.members[member.ServerID] = member
				kv.ring.AddNode(member.ServerID)
			}
		}
	}

	// add missing members to replyMemberInfos
	replyMemberInfos = append(replyMemberInfos, missingMembers...)

	// reply
	reply.MemberInfos = replyMemberInfos
}

func (kv *KVServer) GetRandomNeighbors() []rpc.MemberInfo {
	members := kv.members
	rand.Seed(time.Now().UnixNano())
	randomMembers := make([]rpc.MemberInfo, kv.numNeighbors)
	randIndex := 0
	for i := 0; i < kv.numNeighbors; i++ {
		randIndex = rand.Intn(len(members))
		// skip myself and already selected members
		for members[randIndex].ServerID == kv.id || members[randIndex].IsIn(RandomMembers) {
			randIndex = rand.Intn(len(members))
		}
		randomMembers[i] = members[randIndex]
	}
	return randomMembers
}

func (kv *KVServer) GetAllMembers() []rpc.MemberInfo {
	memberInfos := make([]rpc.MemberInfo, 0)
	for _, memberInfo := range kv.members {
		memberInfos = append(memberInfos, memberInfo)
	}
	return memberInfos
}

func deleteMember(members []rpc.MemberInfo, serverID string) []rpc.MemberInfo {
	for i, member := range members {
		if member.ServerID == serverID {
			return append(members[:i], members[i+1:]...)
		}
	}
	return members
}



