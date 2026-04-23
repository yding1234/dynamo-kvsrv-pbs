package kvsrv_eval

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
)

// MessageTrace records one coordinator<->replica exchange
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

// WARSTrace stores the raw observed samples for the four WARS latency classes.
type WARSTrace struct {
	WriteRequests []time.Duration // W
	WriteAcks     []time.Duration // A
	ReadRequests  []time.Duration // R
	ReadResponses []time.Duration // S
}

// EmpiricalSampler resamples directly from observed latency samples.
type EmpiricalSampler struct {
	samples []time.Duration
}

func NewEmpiricalSampler(samples []time.Duration) (EmpiricalSampler, error) {
	if len(samples) == 0 {
		return EmpiricalSampler{}, fmt.Errorf("sampler requires at least one sample")
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

type CompletedWrite struct {
	Key         string
	StartedAt   time.Time
	CommittedAt time.Time
	Object      rpc.Object
}

type CompletedRead struct {
	Key             string
	StartedAt       time.Time
	ReturnedAt      time.Time
	ReturnedObjects []rpc.Object
}

func NewCompletedWrite(key string, startedAt time.Time, committedAt time.Time, object rpc.Object) CompletedWrite {
	return CompletedWrite{
		Key:         key,
		StartedAt:   startedAt,
		CommittedAt: committedAt,
		Object: rpc.Object{
			Value:   object.Value,
			Context: object.Context.Copy(),
		},
	}
}

func NewCompletedRead(key string, startedAt time.Time, returnedAt time.Time, returnedObjects []rpc.Object) CompletedRead {
	return CompletedRead{
		Key:             key,
		StartedAt:       startedAt,
		ReturnedAt:      returnedAt,
		ReturnedObjects: rpc.CopyObjects(returnedObjects),
	}
}

type PBSCollector struct {
	mu sync.Mutex

	// raw samples, used for WARS simulation
	trace WARSTrace

	// completed writes and reads, used for deltaP and KP evaluation
	writes []CompletedWrite
	reads  []CompletedRead
}

func NewPBSCollector() *PBSCollector {
	return &PBSCollector{trace: WARSTrace{},
		writes: make([]CompletedWrite, 0),
		reads:  make([]CompletedRead, 0),
	}
}

func (c *PBSCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace = WARSTrace{}
	c.writes = make([]CompletedWrite, 0)
	c.reads = make([]CompletedRead, 0)
}

func (c *PBSCollector) Trace() WARSTrace {
	c.mu.Lock()
	defer c.mu.Unlock()

	return WARSTrace{
		WriteRequests: append([]time.Duration(nil), c.trace.WriteRequests...),
		WriteAcks:     append([]time.Duration(nil), c.trace.WriteAcks...),
		ReadRequests:  append([]time.Duration(nil), c.trace.ReadRequests...),
		ReadResponses: append([]time.Duration(nil), c.trace.ReadResponses...),
	}
}

func (c *PBSCollector) GetSamplers() (WARSSamplers, error) {
	return NewWARSSamplers(c.Trace())
}

func (c *PBSCollector) ObserveWriteLatency(m MessageTrace) error {
	if err := m.Validate(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.WriteRequests = append(c.trace.WriteRequests, m.RequestLatency())
	c.trace.WriteAcks = append(c.trace.WriteAcks, m.ResponseLatency())
	return nil
}

func (c *PBSCollector) ObserveReadLatency(m MessageTrace) error {
	if err := m.Validate(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.ReadRequests = append(c.trace.ReadRequests, m.RequestLatency())
	c.trace.ReadResponses = append(c.trace.ReadResponses, m.ResponseLatency())
	return nil
}

func (c *PBSCollector) ObserveCompletedWrite(w CompletedWrite) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// insert the write into the writes list by committed time
	insertIndex := len(c.writes)
	for i := range c.writes {
		if c.writes[i].CommittedAt.After(w.CommittedAt) {
			insertIndex = i
			break
		}
	}
	c.writes = append(c.writes, CompletedWrite{})
	copy(c.writes[insertIndex+1:], c.writes[insertIndex:])
	c.writes[insertIndex] = w
}

func (c *PBSCollector) ObserveCompletedRead(r CompletedRead) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// insert the read into the reads list by returned time
	insertIndex := len(c.reads)
	for i := range c.reads {
		if c.reads[i].ReturnedAt.After(r.ReturnedAt) {
			insertIndex = i
			break
		}
	}
	c.reads = append(c.reads, CompletedRead{})
	copy(c.reads[insertIndex+1:], c.reads[insertIndex:])
	c.reads[insertIndex] = r
}

func (c *PBSCollector) Writes() []CompletedWrite {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]CompletedWrite(nil), c.writes...)
}

func (c *PBSCollector) Reads() []CompletedRead {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]CompletedRead(nil), c.reads...)
}

func (c *PBSCollector) AddWriteSample(requestLatency, ackLatency time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.WriteRequests = append(c.trace.WriteRequests, requestLatency)
	c.trace.WriteAcks = append(c.trace.WriteAcks, ackLatency)
	return nil
}

func (c *PBSCollector) AddReadSample(requestLatency, responseLatency time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.ReadRequests = append(c.trace.ReadRequests, requestLatency)
	c.trace.ReadResponses = append(c.trace.ReadResponses, responseLatency)
	return nil
}