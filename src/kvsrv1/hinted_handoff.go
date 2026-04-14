package kvsrv

import (
	"sort"
	"time"

	"6.5840/kvsrv1/rpc"
)

func (kv *KVServer) HintedPut(args *rpc.HintedPutArgs, reply *rpc.HintedPutReply) {
	// store the put request in the hints map
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.hints[args.TargetServer] = append(kv.hints[args.TargetServer], args.PutArgs.Copy())
	reply.Err = rpc.OK
}

func (kv *KVServer) StartHintedHandoff() {
	go func() {
		ticker := time.NewTicker(kv.hintedHandoffInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				kv.replayAllHints()
			case <-kv.stopCh:
				return
			}
		}
	}()
}

func (kv *KVServer) replayAllHints() {
	targets := kv.GetAllTargets()

	for _, target := range targets {
		kv.replayHints(target)
	}
}

func (kv *KVServer) replayHints(target string) {
	// if the target server isn't alive, do nothing
	if !kv.isAlive(target) {
		return
	}

	hints := kv.GetHints(target)

	// resend pending put requests to the target server
	for _, hint := range hints {
		args := hint
		reply := rpc.PutReply{}
		ok := kv.ends[target].Call("KVServer.ReplicaPut", &args, &reply)
		if ok && reply.Err == rpc.OK {
			kv.removeHint(target, hint)
		}
	}
}

// choose a handoff node from the candidates
func (kv *KVServer) chooseHandoffNode(exclude map[string]bool) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	candidates := make([]string, 0, len(kv.members))
	for serverID, member := range kv.members {
		if exclude[serverID] || member.Status == rpc.Dead {
			continue
		}
		candidates = append(candidates, serverID)
	}
	if len(candidates) == 0 {
		return "", false
	}
	sort.Strings(candidates)
	return candidates[0], true
}

func (kv *KVServer) isAlive(serverID string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	member, ok := kv.members[serverID]
	return ok && member.Status == rpc.Alive
}

func (kv *KVServer) isDead(serverID string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	member, ok := kv.members[serverID]
	return ok && member.Status == rpc.Dead
}


// remove one matching hinted put for the given target server
func (kv *KVServer) removeHint(target string, matched rpc.PutArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	hints := kv.hints[target]
	for i, hint := range hints {
		if hint.Key == matched.Key &&
			hint.Object.Value == matched.Object.Value &&
			hint.Object.Context.IsEqual(matched.Object.Context) &&
			hint.BaseContext.IsEqual(matched.BaseContext) {
			kv.hints[target] = append(hints[:i], hints[i+1:]...)
			if len(kv.hints[target]) == 0 {
				delete(kv.hints, target)
			}
			return
		}
	}
}

func (kv *KVServer) GetHints(target string) []rpc.PutArgs {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	hints := make([]rpc.PutArgs, len(kv.hints[target]))
	for i, hint := range kv.hints[target] {
		hints[i] = hint.Copy()
	}
	return hints
}

func (kv *KVServer) GetAllTargets() []string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	targets := make([]string, 0, len(kv.hints))
	for target, _ := range kv.hints {
		targets = append(targets, target)
	}
	return targets
}