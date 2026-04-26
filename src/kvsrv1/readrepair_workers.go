package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
)

const (
	readRepairWorkerCount    = 8
	readRepairQueueCapacity  = 128
	readRepairCoalesceWindow = 5 * time.Millisecond
)

// readRepairJob captures everything finishCoordGetReadRepair needs after the
// coordinator has already returned to the client.
type readRepairJob struct {
	key               string
	canonicalSiblings []rpc.Object
	results           []rpc.ForwardGetResult
	ch                <-chan rpc.ForwardGetResult
	remaining         int
}

type readRepairCoalesceEntry struct {
	job   *readRepairJob
	timer *time.Timer
}

func (kv *KVServer) startReadRepairWorkers() {
	for i := 0; i < readRepairWorkerCount; i++ {
		go kv.readRepairWorkerLoop()
	}
}

// scheduleReadRepair debounces by key: bursts of reads on the same key within
// readRepairCoalesceWindow collapse to a single job (the latest). Superseded
// jobs only drain their ReplicaGet channel — no RepairPut — so forwarders
// never block.
func (kv *KVServer) scheduleReadRepair(job *readRepairJob) {
	kv.readRepairCoalesceMu.Lock()
	if e, ok := kv.readRepairCoalesce[job.key]; ok {
		e.timer.Stop()
		go drainForwardGetResults(e.job.ch, e.job.remaining)
		delete(kv.readRepairCoalesce, job.key)
	}
	expect := job
	t := time.AfterFunc(readRepairCoalesceWindow, func() {
		kv.flushReadRepairCoalesceKey(job.key, expect)
	})
	kv.readRepairCoalesce[job.key] = &readRepairCoalesceEntry{job: expect, timer: t}
	kv.readRepairCoalesceMu.Unlock()
}

func (kv *KVServer) flushReadRepairCoalesceKey(key string, expect *readRepairJob) {
	kv.readRepairCoalesceMu.Lock()
	e, ok := kv.readRepairCoalesce[key]
	if !ok || e.job != expect {
		kv.readRepairCoalesceMu.Unlock()
		return
	}
	delete(kv.readRepairCoalesce, key)
	kv.readRepairCoalesceMu.Unlock()

	select {
	case kv.readRepairJobCh <- expect:
	default:
		go drainForwardGetResults(expect.ch, expect.remaining)
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

// drainReadRepairPipeline runs once per KVServer when stopCh closes: cancel
// debounce timers, drain pending coalesced jobs without repair, then drain the
// worker queue the same way.
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
