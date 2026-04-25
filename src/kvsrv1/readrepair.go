package kvsrv

import "6.5840/kvsrv1/rpc"

func (kv *KVServer) RepairPut(args *rpc.RepairArgs, reply *rpc.RepairReply) {
	// TODO: we also need to consider any write roll back if delete is true
	if args.Delete {
		kv.installObjects(args.Key, nil)
		reply.Err = rpc.OK
		return
	}

	kv.mergeObjects(args.Key, args.Objects)
	reply.Err = rpc.OK
}

func findStaleReplicas(canonicalSiblings []rpc.Object, results []rpc.ForwardGetResult) []string {
	staleReplicas := make([]string, 0)

	for _, res := range results {
		if !res.OK {
			continue
		}
		if res.Reply.Err != rpc.OK {
			staleReplicas = append(staleReplicas, res.ServerID)
			continue
		}
		if !IsSameSiblings(canonicalSiblings, res.Reply.Objects) {
			staleReplicas = append(staleReplicas, res.ServerID)
		}
	}
	return staleReplicas
}

func IsSameSiblings(siblings []rpc.Object, other []rpc.Object) bool {
	if len(siblings) != len(other) {
		return false
	}
	for _, sibling := range siblings {
		found := false
		for _, other := range other {
			if sibling.IsEqual(other) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (kv *KVServer) repairReplicas(key string, canonicalSiblings []rpc.Object, staleReplicas []string) {
	for _, staleReplica := range staleReplicas {
		go kv.ends[staleReplica].Call("KVServer.RepairPut", 
			&rpc.RepairArgs{Key: key, Objects: canonicalSiblings}, &rpc.RepairReply{})
	}
}
