package kvsrv

import (
	"dynamo-kvsrv/kvsrv1/rpc"
	
	"time"
)
const (
	readRepairWorkerCount    = 5 
	readRepairQueueCapacity  = 128 
	readRepairCoalesceWindow = 20 * time.Millisecond 
)

type readRepairJob struct {
	key               string
	canonicalSiblings []rpc.Object
	results           []rpc.ForwardGetResult
	ch                <-chan rpc.ForwardGetResult
	remaining         int
}

// used to coalesce read repair jobs by key
type readRepairCoalesceEntry struct {
	job   *readRepairJob
	timer *time.Timer
}

func (kv *KVServer) StartReadRepairWorkers() {
	for i := 0; i < readRepairWorkerCount; i++ {
		go kv.readRepairWorkerLoop()
	}
}

func (kv *KVServer) readRepairWorkerLoop() {
	for {
		select {
		case job := <-kv.readRepairJobCh:
			if job == nil {
				continue
			}
			kv.finishCoordGetReadRepair(job.key, job.canonicalSiblings, job.results, job.ch, job.remaining)
		case <-kv.stopCh:
			kv.drainReadRepairPipeline()
			return
		}
	}
}


func (kv *KVServer) scheduleReadRepair(job *readRepairJob) {
	kv.readRepairCoalesceMu.Lock()
	defer kv.readRepairCoalesceMu.Unlock()

	// if the job is already in the coalesce map, 
	// delete the existing job and schedule the new job
	if e, ok := kv.readRepairCoalesce[job.key]; ok {
		e.timer.Stop()
		go drainForwardGetResults(e.job.ch, e.job.remaining)
		delete(kv.readRepairCoalesce, job.key)
	}
	
	t := time.AfterFunc(readRepairCoalesceWindow, func() {
		kv.tryFlushReadRepairCoalesceKey(job.key, job)
	})

	kv.readRepairCoalesce[job.key] = &readRepairCoalesceEntry{job: job, timer: t}
}

func (kv *KVServer) tryFlushReadRepairCoalesceKey(key string, expectJob *readRepairJob) bool{
	kv.readRepairCoalesceMu.Lock()
	defer kv.readRepairCoalesceMu.Unlock()

	e, ok := kv.readRepairCoalesce[key]

	if !ok || e.job != expectJob {
		return false
	}

	select {
	case kv.readRepairJobCh <- expectJob:
		e.timer.Stop()
		delete(kv.readRepairCoalesce, key)
		return true
	default:
		e.timer.Stop()
		e.timer.Reset(2*readRepairCoalesceWindow) // reset the timer to 2x the coalesce window
		return false
	}
}


func (kv *KVServer) drainReadRepairPipeline() {
	kv.readRepairPipelineOnce.Do(func() {
		kv.readRepairCoalesceMu.Lock()
		for _, e := range kv.readRepairCoalesce {
			e.timer.Stop()
			go drainForwardGetResults(e.job.ch, e.job.remaining)
		}
		kv.readRepairCoalesce = make(map[string]*readRepairCoalesceEntry)
		kv.readRepairCoalesceMu.Unlock()

		for {
			select {
			case job := <-kv.readRepairJobCh:
				if job != nil {
					go drainForwardGetResults(job.ch, job.remaining)
				}
			default:
				return
			}
		}
	})
}


func (kv *KVServer) finishCoordGetReadRepair(key string, canonicalSiblings []rpc.Object,
	results []rpc.ForwardGetResult, ch <-chan rpc.ForwardGetResult, remaining int) {
	// collect remaining results
	for i := 0; i < remaining; i++ {
		res := <-ch
		results = append(results, res)
		if !res.OK {
			continue
		}
		if res.Reply.Err == rpc.OK {
			for _, obj := range res.Reply.Objects {
				if obj.CanBeAddedTo(canonicalSiblings) {
					canonicalSiblings = rpc.AddObject(canonicalSiblings, obj, nil)
				}
			}
		}
	}

	// check if the canonical siblings are available
	hasCanonical := len(canonicalSiblings) > 0
	if !hasCanonical {
		return
	}

	staleReplicas := findStaleReplicas(canonicalSiblings, results)
	if len(staleReplicas) == 0 {
		return
	}
	kv.repairReplicas(key, canonicalSiblings, staleReplicas)
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

func (kv *KVServer) RepairPut(args *rpc.RepairArgs, reply *rpc.RepairReply) {
	// TODO: we also need to consider any write roll back if delete is true
	if args.Delete {
		// delete the key from the current sector
		kv.installObjects(args.Key, nil)
		reply.Err = rpc.OK
		return
	}

	kv.mergeObjectsAndDiscard(args.Key, args.Objects)
	reply.Err = rpc.OK
}