package kvsrv_eval

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/vclock"
)

func ObserveDeltaP(collector *PBSCollector, delta time.Duration) float64 {
	writes := collector.Writes()
	reads := collector.Reads()

	if len(reads) == 0 {
		return 0
	}

	consistentCount := 0
	for _, read := range reads {
		if isDeltaRegular(read, writes, delta) {
			consistentCount++
		}
	}
	return float64(consistentCount) / float64(len(reads))
}

func ObserveKP(collector *PBSCollector, k int) float64 {
	writes := collector.Writes()
	reads := collector.Reads()
	if len(reads) == 0 {
		return 0
	}

	consistentCount := 0
	for _, read := range reads {
		if isKRegular(read, writes, k) {
			consistentCount++
		}
	}
	return float64(consistentCount) / float64(len(reads))
}

func ObserveDeltaPSweep(collector *PBSCollector, deltas []time.Duration) []float64 {
	results := make([]float64, len(deltas))
	for i, delta := range deltas {
		results[i] = ObserveDeltaP(collector, delta)
	}
	return results
}

func ObserveKPSweep(collector *PBSCollector, ks []int) []float64 {
	results := make([]float64, len(ks))
	for i, k := range ks {
		results[i] = ObserveKP(collector, k)
	}
	return results
}

func isDeltaRegular(read CompletedRead, writes []CompletedWrite, delta time.Duration) bool {
	latestWriteBeforeDelta :=  NewCompletedWrite(read.Key, time.Time{}, time.Time{}, rpc.Object{})

	for _, write := range writes {
		if write.Key != read.Key {
			continue
		}

		if overlaps(read, write) {
			if IsConsistent(write.Object, read.ReturnedObjects) {
				return true
			}
			continue
		}

		if write.CommittedAt.After(read.StartedAt) && !write.StartedAt.Before(read.ReturnedAt) {
			continue
		}

		if write.CommittedAt.Before(read.StartedAt.Sub(delta)) {
			if write.CommittedAt.After(latestWriteBeforeDelta.CommittedAt) {
				latestWriteBeforeDelta = write
			}
			continue
		}

		if IsConsistent(write.Object, read.ReturnedObjects) {
			return true
		}
	}

	// If the read did not return a completed write from the delta window, it may
	// still legally return the latest write that was visible exactly delta ago.
	if IsConsistent(latestWriteBeforeDelta.Object, read.ReturnedObjects) {
		return true
	}

	return false
}

func isKRegular(read CompletedRead, writes []CompletedWrite, k int) bool {
	kCandidate := make([]CompletedWrite, 0, k) // the k latest writes before read start
	overlapped := make([]CompletedWrite, 0, 0) // the overlapped writes between read start and read completion

	for _, write := range writes {
		if write.Key != read.Key {
			continue
		}

		if overlaps(read, write) {
			overlapped = append(overlapped, write)
			continue
		}

		if !write.CommittedAt.After(read.StartedAt) {
			if len(kCandidate) < k {
				kCandidate = append(kCandidate, write)
			} else {
				// find the stalest write in kCandidate and replace it with the current write
				stalest := 0
				for i := 1; i < k; i++ {
					if kCandidate[i].CommittedAt.Before(kCandidate[stalest].CommittedAt) {
						stalest = i
					}
				}
				kCandidate[stalest] = write
			}
		}
	}

	for _, write := range overlapped {
		if IsConsistent(write.Object, read.ReturnedObjects) {
			return true
		}
	}
	for _, write := range kCandidate {
		if IsConsistent(write.Object, read.ReturnedObjects) {
			return true
		}
	}
	return false
}

// IsConsistent returns true if any returned object is causally equal to or
// newer than the target write object.
func IsConsistent(writeObj rpc.Object, readObjs []rpc.Object) bool {
	for _, readObj := range readObjs {
		cmp := writeObj.Context.Compare(readObj.Context)
		if cmp == vclock.Equal || cmp == vclock.Before {
			return true
		}
		if cmp == vclock.After {
			return false
		}
	}
	return false
}

func overlaps(read CompletedRead, write CompletedWrite) bool {
	return write.StartedAt.Before(read.ReturnedAt) && write.CommittedAt.After(read.StartedAt)
}