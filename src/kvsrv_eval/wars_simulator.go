package kvsrv_eval

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

// SimulationConfig configures the paper's Monte Carlo evaluation of
// (delta, p)-regular semantics under the WARS model.
type SimulationConfig struct {
	NumReplicas int
	ReadQuorum  int
	WriteQuorum int
	Delta       time.Duration
	Iterations  int
	RNG         *rand.Rand
}

func (c SimulationConfig) Validate() error {
	switch {
	case c.NumReplicas <= 0:
		return fmt.Errorf("NumReplicas must be > 0")
	case c.ReadQuorum <= 0 || c.ReadQuorum > c.NumReplicas:
		return fmt.Errorf("ReadQuorum must be in [1, NumReplicas]")
	case c.WriteQuorum <= 0 || c.WriteQuorum > c.NumReplicas:
		return fmt.Errorf("WriteQuorum must be in [1, NumReplicas]")
	case c.Delta < 0:
		return fmt.Errorf("Delta must be >= 0")
	case c.Iterations <= 0:
		return fmt.Errorf("Iterations must be > 0")
	}
	return nil
}

type SimulationResult struct {
	ConsistentTrials int
	Iterations       int
	Probability      float64
}

// SimulateDeltaP runs the WARS Monte Carlo simulation described in the PBS
// paper and returns the estimated probability of a read being consistent
// delta time after a write commits.
func SimulateDeltaP(config SimulationConfig, samplers WARSSamplers) (SimulationResult, error) {
	if err := config.Validate(); err != nil {
		return SimulationResult{}, err
	}
	if err := validateSamplers(samplers); err != nil {
		return SimulationResult{}, err
	}

	consistentTrials := 0
	for iter := 0; iter < config.Iterations; iter++ {
		if simulateTrial(config, samplers) {
			consistentTrials++
		}
	}

	return SimulationResult{
		ConsistentTrials: consistentTrials,
		Iterations:       config.Iterations,
		Probability:      float64(consistentTrials) / float64(config.Iterations),
	}, nil
}

func validateSamplers(samplers WARSSamplers) error {
	switch {
	case samplers.WriteRequest.Len() == 0:
		return fmt.Errorf("WriteRequest sampler is empty")
	case samplers.WriteAck.Len() == 0:
		return fmt.Errorf("WriteAck sampler is empty")
	case samplers.ReadRequest.Len() == 0:
		return fmt.Errorf("ReadRequest sampler is empty")
	case samplers.ReadResponse.Len() == 0:
		return fmt.Errorf("ReadResponse sampler is empty")
	}
	return nil
}

func simulateTrial(config SimulationConfig, samplers WARSSamplers) bool {
	writeRequests := make([]time.Duration, config.NumReplicas)
	writeLatencies := make([]time.Duration, config.NumReplicas)
	readRequests := make([]time.Duration, config.NumReplicas)
	readLatencies := make([]time.Duration, config.NumReplicas)

	for replica := 0; replica < config.NumReplicas; replica++ {
		writeRequests[replica] = samplers.WriteRequest.GetSample(config.RNG)
		writeAck := samplers.WriteAck.GetSample(config.RNG)
		writeLatencies[replica] = writeRequests[replica] + writeAck

		readRequests[replica] = samplers.ReadRequest.GetSample(config.RNG)
		readResponse := samplers.ReadResponse.GetSample(config.RNG)
		readLatencies[replica] = readRequests[replica] + readResponse
	}

	writeFinish := nthSmallest(writeLatencies, config.WriteQuorum)
	firstReplies := firstKIndices(readLatencies, config.ReadQuorum)
	for _, replica := range firstReplies {
		if writeFinish+readRequests[replica]+config.Delta >= writeRequests[replica] {
			return true
		}
	}

	return false
}

func nthSmallest(values []time.Duration, n int) time.Duration {
	copied := make([]time.Duration, len(values))
	copy(copied, values)
	sort.Slice(copied, func(i, j int) bool {
		return copied[i] < copied[j]
	})
	return copied[n-1]
}

func firstKIndices(values []time.Duration, k int) []int {
	indices := make([]int, len(values))
	for i := range values {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return values[indices[i]] < values[indices[j]]
	})

	selected := make([]int, k)
	copy(selected, indices[:k])
	return selected
}
