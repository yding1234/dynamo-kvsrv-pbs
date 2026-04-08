package meta

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"6.5840/vclock"
)

const scalarNode = "__scalar__" // backward compatibility during migration

type Context struct {
	VC        *vclock.VClock
	Timestamp uint64
	ETag      string // hash of the value
}

func hash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func NewContext() Context {
	return Context{
		VC:        vclock.NewVClock(),
		Timestamp: uint64(time.Now().Unix()),
		ETag:      "",
	}
}

func ZeroContext() Context {
	return ContextFromCounter(0)
}

// Create a new context from a counter
func ContextFromCounter(counter uint64) Context {
	ctx := NewContext()
	ctx.VC.SetVersion(scalarNode, counter)
	return ctx
}

func (ctx Context) Counter() uint64 {
	if ctx.VC == nil {
		return 0
	}
	return ctx.VC.GetVersion(scalarNode)
}

//
func (ctx Context) Next() Context {
	next := ContextFromCounter(ctx.Counter() + 1)
	next.Timestamp = uint64(time.Now().Unix())
	next.ETag = ctx.ETag
	return next
}

func (ctx Context) Compare(other Context) int {
	if ctx.VC == nil || other.VC == nil {
		switch {
		case ctx.Counter() < other.Counter():
			return vclock.Before
		case ctx.Counter() > other.Counter():
			return vclock.After
		default:
			return vclock.Equal
		}
	}
	return ctx.VC.Compare(other.VC)
}

func (ctx Context) Merge(other Context) Context {
	merged := ContextFromCounter(ctx.Counter())
	if ctx.VC != nil {
		merged.VC = ctx.VC.Copy()
	}
	if merged.VC == nil {
		merged.VC = vclock.NewVClock()
	}
	if other.VC != nil {
		merged.VC.Merge(other.VC)
	}
	if other.Timestamp > ctx.Timestamp {
		merged.Timestamp = other.Timestamp
	} else {
		merged.Timestamp = ctx.Timestamp
	}
	if merged.ETag == "" {
		merged.ETag = other.ETag
	}
	return merged
}
