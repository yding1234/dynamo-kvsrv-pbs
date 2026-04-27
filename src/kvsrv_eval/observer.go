package kvsrv_eval

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/vclock"
)

func ObserveDeltaP(reads []CompletedRead, writes []CompletedWrite, delta time.Duration) float64 {

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

func ObserveKP(reads []CompletedRead, writes []CompletedWrite, k int) float64 {
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
	writes := collector.Writes()
	reads := collector.Reads()

	for i, delta := range deltas {
		results[i] = ObserveDeltaP(reads, writes, delta)
	}
	return results
}

// ObserveDeltaPSweepE2E returns end-to-end non-stale probability:
//
//	(# reads in the collector that are delta-regular) / all_read_attempts
//
// The collector records successful CoordGet completions; all_read_attempts must
// count every read RPC (e.g. reader gets + writer refresh gets), not just reader outcome sums.
// When allReadAttempts <= 0, it falls back to the successful-read denominator.
func ObserveDeltaPSweepE2E(collector *PBSCollector, deltas []time.Duration, allReadAttempts int64) []float64 {
	if allReadAttempts <= 0 {
		return ObserveDeltaPSweep(collector, deltas)
	}
	results := make([]float64, len(deltas))
	writes := collector.Writes()
	reads := collector.Reads()
	denom := float64(allReadAttempts)

	for i, delta := range deltas {
		nonStale := 0
		for _, read := range reads {
			if isDeltaRegular(read, writes, delta) {
				nonStale++
			}
		}
		results[i] = float64(nonStale) / denom
	}
	return results
}

func ObserveKPSweep(collector *PBSCollector, ks []int) []float64 {
	results := make([]float64, len(ks))
	writes := collector.Writes()
	reads := collector.Reads()

	for i, k := range ks {
		results[i] = ObserveKP(reads, writes, k)
	}
	return results
}

// ObserveKPSweepE2E returns end-to-end non-stale probability:
// non_stale_reads / all_read_attempts.
// When allReadAttempts <= 0, it falls back to the successful-read denominator.
func ObserveKPSweepE2E(collector *PBSCollector, ks []int, allReadAttempts int64) []float64 {
	if allReadAttempts <= 0 {
		return ObserveKPSweep(collector, ks)
	}
	results := make([]float64, len(ks))
	writes := collector.Writes()
	reads := collector.Reads()
	denom := float64(allReadAttempts)

	for i, k := range ks {
		nonStale := 0
		for _, read := range reads {
			if isKRegular(read, writes, k) {
				nonStale++
			}
		}
		results[i] = float64(nonStale) / denom
	}
	return results
}

func isDeltaRegular(read CompletedRead, writes []CompletedWrite, delta time.Duration) bool {
	cutoff := read.StartedAt.Add(-delta)
	latestWriteBeforeDelta := NewCompletedWrite(read.Key, time.Time{}, time.Time{}, rpc.Object{})

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

		if write.CommittedAt.Before(cutoff) {
			if write.CommittedAt.After(latestWriteBeforeDelta.CommittedAt) {
				latestWriteBeforeDelta = write
			}
			continue
		}

		if IsConsistent(write.Object, read.ReturnedObjects) {
			return true
		}
	}

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
		if cmp == vclock.After {
			return false
		}
		if cmp == vclock.Equal || cmp == vclock.Before {
			return true
		}
	}
	return false
}

func overlaps(read CompletedRead, write CompletedWrite) bool {
	return write.StartedAt.Before(read.ReturnedAt) && write.CommittedAt.After(read.StartedAt)
}
