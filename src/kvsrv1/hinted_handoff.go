package kvsrv

import (
	"sort"
	"time"

	"6.5840/kvsrv1/rpc"
)

type HintedWrite struct {
	TargetServer string
	Key          string
	Object       rpc.Object
	BaseContext  rpc.Context
}

func (kv *KVServer) HintedPut(args *rpc.HintedPutArgs, reply *rpc.HintedPutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	hint := HintedWrite{
		TargetServer: args.TargetServer,
		Key:          args.Key,
		Object:       rpc.NewObject(args.Object.Value, args.Object.Context.Copy()),
		BaseContext:  args.BaseContext.Copy(),
	}
	kv.hints[args.TargetServer] = append(kv.hints[args.TargetServer], hint)
	if member, ok := kv.members[args.TargetServer]; ok {
		member.Status = rpc.Dead
		kv.members[args.TargetServer] = member
	}
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

func (kv *KVServer) isDead(serverID string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	member, ok := kv.members[serverID]
	return ok && member.Status == rpc.Dead
}

func (kv *KVServer) replayAllHints() {
	kv.mu.Lock()
	targets := make([]string, 0, len(kv.hints))
	for target := range kv.hints {
		targets = append(targets, target)
	}
	kv.mu.Unlock()

	for _, target := range targets {
		kv.replayHintsForTarget(target)
	}
}

func (kv *KVServer) replayHintsForTarget(target string) {
	if kv.isDead(target) {
		return
	}

	kv.mu.Lock()
	hints := append([]HintedWrite(nil), kv.hints[target]...)
	kv.mu.Unlock()

	for _, hint := range hints {
		args := rpc.PutArgs{
			Key:         hint.Key,
			Object:      hint.Object,
			BaseContext: hint.BaseContext.Copy(),
		}
		reply := rpc.PutReply{}
		ok := kv.ends[target].Call("KVServer.ReplicaPut", &args, &reply)
		if ok && reply.Err == rpc.OK {
			kv.removeHint(target, hint)
		}
	}
}

func (kv *KVServer) removeHint(target string, matched HintedWrite) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	hints := kv.hints[target]
	for i, hint := range hints {
		if hint.Key == matched.Key &&
			hint.TargetServer == matched.TargetServer &&
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

func (kv *KVServer) CopyHints(target string) []HintedWrite {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	hints := kv.hints[target]
	copied := make([]HintedWrite, len(hints))
	for i, hint := range hints {
		copied[i] = HintedWrite{
			TargetServer: hint.TargetServer,
			Key:          hint.Key,
			Object:       rpc.NewObject(hint.Object.Value, hint.Object.Context.Copy()),
			BaseContext:  hint.BaseContext.Copy(),
		}
	}
	return copied
}