package rpc

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"6.5840/vclock"
)

// go test ./kvsrv1 -run TestConflictSiblingsAndConverge -v

type Context struct {
	VC        vclock.VClock
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

func NewContextFromVClock(vc vclock.VClock) Context {
	return Context{
		VC: vc.Copy(),
		Timestamp: uint64(time.Now().Unix()),
		ETag: "",
	}
}

func (ctx Context) Copy() Context {
	return Context{
		VC: ctx.VC.Copy(),
		Timestamp: ctx.Timestamp,
		ETag: ctx.ETag,
	}
}

func (ctx Context) IsInitial() bool {
	return ctx.VC.IsInitial() && ctx.ETag == ""
}

func (ctx Context) IsEqual(other Context) bool {
	return ctx.Compare(other) == vclock.Equal && ctx.Timestamp == other.Timestamp && ctx.ETag == other.ETag
}

// update the context with the new node and value
func (ctx *Context) Update(node string, value string) {
	ctx.VC = ctx.VC.Copy()
	ctx.VC.Increment(node)
	ctx.Timestamp = uint64(time.Now().Unix())
	ctx.ETag = hash(value)
}

func (ctx Context) Compare(other Context) int {
	return ctx.VC.Compare(other.VC)
}

func (ctx Context) Merge(other Context, newValue string) Context {
	merged := NewContextFromVClock(ctx.VC.Copy())
	merged.VC.Merge(other.VC)

	merged.Timestamp = uint64(time.Now().Unix())
	merged.ETag = hash(newValue)
	return merged
}
