package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"6.5840/chr"
)


type Clerk struct {
	clnt   *tester.Clnt
	ring *chr.ConsistentHashRing
	readQuorum int
	writeQuorum int
}

func MakeClerk(clnt *tester.Clnt, ring *chr.ConsistentHashRing, readQuorum int, writeQuorum int) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, 
		ring: ring, 
		readQuorum: readQuorum, 
		writeQuorum: writeQuorum}
	return ck
}

// Read operation
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	ch := make(chan rpc.GetReply, len(preferredList))
	preferredList := ck.ring.GeneratePreferenceList(key)
	
	// send get requests to all servers in the preferred list
	for _, serverID := range preferredList {
		go func(serverID string) {
			args := rpc.GetArgs{Key: key}
			reply := rpc.GetReply{}

			ok := ck.clnt.Call(serverID, "KVServer.Get", &args, &reply) // first try

			for !ok || (ok && reply.Err != rpc.OK) {
				if ok && reply.Err == rpc.ErrNoKey {
					ch <- reply
				}
				ok = ck.clnt.Call(serverID, "KVServer.Get", &args, &reply) // retry
			}

			ch <- reply // success, return OK
		}(serverID)
	}
	
	// wait for all responses
	finalReply := make([]rpc.GetReply, 0)
	for i := 0; i < len(preferredList); i++ {
		reply := <-ch
		if reply.Err == rpc.OK {
			// if the version isn't in the final reply, add it
			existSameVersion := false
			for _, r := range finalReply {
				if r.Version == reply.Version {
					existSameVersion = true
					break
				}
			}
			if !existSameVersion {
				finalReply = append(finalReply, reply)
			}
		}
	}
	
	// if the final reply is empty, return ErrNoKey
	if len(finalReply) < ck.readQuorum {
		return "", 0, rpc.ErrNoKey
	}
	
	// return the value and version of the final reply
	// temporary solution, should be sorted by vector clock
	return finalReply[ck.readQuorum - 1].Value, finalReply[ck.readQuorum - 1].Version, rpc.OK
}


// Write operation
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	ch := make(chan rpc.Err, len(preferredList))
	preferredList := ck.ring.GeneratePreferenceList(key)

	// send put requests to all servers in the preferred list
	for _, serverID := range preferredList {
		go func(serverID string) {
			args := rpc.PutArgs{Key: key, Value: value, Version: version}
			reply := rpc.PutReply{}
			resent := false

			ok := ck.clnt.Call(serverID, "KVServer.Put", &args, &reply) // first try

			for !ok || (ok && reply.Err == rpc.ErrVersion){
				if ok && !resent {ch <- reply.Err}
				if ok && resent {ch <- rpc.ErrMaybe}
				resent = true
				ok = ck.clnt.Call(serverID, "KVServer.Put", &args, &reply) // retry
			}
			ch <- reply.Err
		}(serverID)
	}

	// wait for all responses
	successCount := 0
	exitsVersionErr := false
	for i := 0; i < len(preferredList); i++ {
		err := <-ch
		if err == rpc.OK {
			successCount++
		}
	}
	
	// check all responses, return the appropriate error
	if successCount >= ck.writeQuorum {
		return rpc.OK
	} 
	if exitsVersionErr {
		return rpc.ErrVersion
	} 
	return rpc.ErrMaybe
}
