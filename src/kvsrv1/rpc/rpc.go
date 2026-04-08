package rpc

import "6.5840/kvsrv1/meta"

type Err string

const (
	// RPC error
	ErrRPCFailure = "ErrRPCFailure"

	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For consistent hashing
	ErrNotCoordinator = "ErrNotCoordinator"
	ErrReadQuorumNotMet  = "ErrReadQuorumNotMet"
    ErrWriteQuorumNotMet = "ErrWriteQuorumNotMet"
)

type Context = meta.Context
type Tversion = Context // backward compatibility during migration

func ZeroContext() Context { return meta.ZeroContext() }
func ContextFromCounter(counter uint64) Context { return meta.ContextFromCounter(counter) }

type PutArgs struct {
	Key     string
	Value   string
	Context Context
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type Object struct {
	Value   string
	Context Context
}

type GetReply struct {
	Objects []Object
	Err     Err
}
