package kvsrv_eval

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// MessageTrace records one coordinator<->replica exchange.
type MessageTrace struct {
	SentAt      time.Time
	ArrivedAt   time.Time
	RespondedAt time.Time
	ReceivedAt  time.Time
}

func NewMessageTrace(sentAt time.Time, arrivedAt time.Time, respondedAt time.Time, receivedAt time.Time) MessageTrace {
	return MessageTrace{
		SentAt:      sentAt,
		ArrivedAt:   arrivedAt,
		RespondedAt: respondedAt,
		ReceivedAt:  receivedAt,
	}
}

func (m MessageTrace) Validate() error {
	switch {
	case m.SentAt.IsZero():
		return fmt.Errorf("message trace missing SentAt")
	case m.ArrivedAt.IsZero():
		return fmt.Errorf("message trace missing ArrivedAt")
	case m.RespondedAt.IsZero():
		return fmt.Errorf("message trace missing RespondedAt")
	case m.ReceivedAt.IsZero():
		return fmt.Errorf("message trace missing ReceivedAt")
	case m.ArrivedAt.Before(m.SentAt):
		return fmt.Errorf("message trace arrived before it was sent")
	case m.RespondedAt.Before(m.ArrivedAt):
		return fmt.Errorf("message trace responded before it arrived")
	case m.ReceivedAt.Before(m.RespondedAt):
		return fmt.Errorf("message trace received before response was sent")
	}
	return nil
}

func (m MessageTrace) RequestLatency() time.Duration {
	return m.ArrivedAt.Sub(m.SentAt)
}

func (m MessageTrace) ResponseLatency() time.Duration {
	return m.ReceivedAt.Sub(m.RespondedAt)
}

func (m MessageTrace) ServiceLatency() time.Duration {
	return m.RespondedAt.Sub(m.ArrivedAt)
}

// WARSTrace stores the raw observed samples for the four WARS latency classes.
type WARSTrace struct {
	WriteRequests []time.Duration // W
	WriteAcks     []time.Duration // A
	ReadRequests  []time.Duration // R
	ReadResponses []time.Duration // S
}

func (t WARSTrace) Validate() error {
	switch {
	case len(t.WriteRequests) == 0:
		return fmt.Errorf("write request trace is empty")
	case len(t.WriteAcks) == 0:
		return fmt.Errorf("write ack trace is empty")
	case len(t.ReadRequests) == 0:
		return fmt.Errorf("read request trace is empty")
	case len(t.ReadResponses) == 0:
		return fmt.Errorf("read response trace is empty")
	}

	if err := validateSamples("write request trace", t.WriteRequests); err != nil {
		return err
	}
	if err := validateSamples("write ack trace", t.WriteAcks); err != nil {
		return err
	}
	if err := validateSamples("read request trace", t.ReadRequests); err != nil {
		return err
	}
	if err := validateSamples("read response trace", t.ReadResponses); err != nil {
		return err
	}
	return nil
}

// EmpiricalSampler resamples directly from observed latency samples.
type EmpiricalSampler struct {
	samples []time.Duration
}

func NewEmpiricalSampler(samples []time.Duration) (EmpiricalSampler, error) {
	if len(samples) == 0 {
		return EmpiricalSampler{}, fmt.Errorf("sampler requires at least one sample")
	}
	if err := validateSamples("sampler", samples); err != nil {
		return EmpiricalSampler{}, err
	}

	return EmpiricalSampler{samples: append([]time.Duration(nil), samples...)}, nil
}

func (s EmpiricalSampler) Len() int {
	return len(s.samples)
}

func (s EmpiricalSampler) Samples() []time.Duration {
	return append([]time.Duration(nil), s.samples...)
}

func (s EmpiricalSampler) GetSample(rng *rand.Rand) time.Duration {
	if len(s.samples) == 0 {
		panic("sampling from empty empirical sampler")
	}
	return s.samples[rng.Intn(len(s.samples))]
}

// WARSSamplers packages one sampler per WARS latency class.
type WARSSamplers struct {
	WriteRequest EmpiricalSampler
	WriteAck     EmpiricalSampler
	ReadRequest  EmpiricalSampler
	ReadResponse EmpiricalSampler
}

func NewWARSSamplers(trace WARSTrace) (WARSSamplers, error) {
	if err := trace.Validate(); err != nil {
		return WARSSamplers{}, err
	}

	writeRequest, err := NewEmpiricalSampler(trace.WriteRequests)
	if err != nil {
		return WARSSamplers{}, err
	}
	writeAck, err := NewEmpiricalSampler(trace.WriteAcks)
	if err != nil {
		return WARSSamplers{}, err
	}
	readRequest, err := NewEmpiricalSampler(trace.ReadRequests)
	if err != nil {
		return WARSSamplers{}, err
	}
	readResponse, err := NewEmpiricalSampler(trace.ReadResponses)
	if err != nil {
		return WARSSamplers{}, err
	}

	return WARSSamplers{
		WriteRequest: writeRequest,
		WriteAck:     writeAck,
		ReadRequest:  readRequest,
		ReadResponse: readResponse,
	}, nil
}

// Tracer collects raw WARS samples and can export them either as raw traces or
// as empirical samplers for the Monte Carlo simulator.
type Tracer struct {
	mu    sync.Mutex
	trace WARSTrace
}

func NewTracer() *Tracer {
	return &Tracer{trace: WARSTrace{}}
}

func (t *Tracer) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.trace = WARSTrace{}
}

func (t *Tracer) Trace() WARSTrace {
	t.mu.Lock()
	defer t.mu.Unlock()

	return WARSTrace{
		WriteRequests: append([]time.Duration(nil), t.trace.WriteRequests...),
		WriteAcks:     append([]time.Duration(nil), t.trace.WriteAcks...),
		ReadRequests:  append([]time.Duration(nil), t.trace.ReadRequests...),
		ReadResponses: append([]time.Duration(nil), t.trace.ReadResponses...),
	}
}

func (t *Tracer) GetSamplers() (WARSSamplers, error) {
	return NewWARSSamplers(t.Trace())
}

func (t *Tracer) AddWriteRequestLatency(latency time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.trace.WriteRequests = append(t.trace.WriteRequests, latency)
	return nil
}

func (t *Tracer) AddWriteAckLatency(latency time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.trace.WriteAcks = append(t.trace.WriteAcks, latency)
	return nil
}

func (t *Tracer) AddReadRequestLatency(latency time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.trace.ReadRequests = append(t.trace.ReadRequests, latency)
	return nil
}

func (t *Tracer) AddReadResponseLatency(latency time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.trace.ReadResponses = append(t.trace.ReadResponses, latency)
	return nil
}

func (t *Tracer) AddWriteSample(requestLatency, ackLatency time.Duration) error {
	err := t.AddWriteRequestLatency(requestLatency)
	if err != nil {
		return err
	}
	err = t.AddWriteAckLatency(ackLatency)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tracer) AddReadSample(requestLatency, responseLatency time.Duration) error {
	err := t.AddReadRequestLatency(requestLatency)
	if err != nil {
		return err
	}
	err = t.AddReadResponseLatency(responseLatency)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tracer) ObserveWrite(trace MessageTrace) error {
	if err := trace.Validate(); err != nil {
		return err
	}
	return t.AddWriteSample(trace.RequestLatency(), trace.ResponseLatency())
}

func (t *Tracer) ObserveRead(trace MessageTrace) error {
	if err := trace.Validate(); err != nil {
		return err
	}
	return t.AddReadSample(trace.RequestLatency(), trace.ResponseLatency())
}

// check if the samples are non-negative
func validateSamples(label string, samples []time.Duration) error {
	for _, sample := range samples {
		if sample < 0 {
			return fmt.Errorf("%s contains negative latency: %v", label, sample)
		}
	}
	return nil
}
