package rpc

import "time"

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

	// For anti-entropy
	ErrNoHashValue = "ErrNoHashValue"

	// For membership
)

// for get/put requests between the coordinator and the client
type PutArgs struct {
	Key     string
	Object  Object
	BaseContext Context // the context of the original request
}

func (args PutArgs) Copy() PutArgs {
	return PutArgs{
		Key:     args.Key,
		Object:  args.Object,
		BaseContext: args.BaseContext.Copy(),
	}
}

type PutReply struct {
	Err Err
	// For tracing
	ArrivedAt time.Time
	RespondedAt time.Time
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Objects []Object
	Err     Err
	// For tracing
	ArrivedAt time.Time
	RespondedAt time.Time
}

// for forwarding requests to the replicas
type ForwardGetResult struct {
	ServerID string
	OK    bool
	Reply GetReply
}

type ForwardPutResult struct {
	OK  bool
	Err Err
}

// for read repair
type RepairArgs struct {
    Key     string
    Objects []Object // the canonical siblings
    Delete  bool // if true, delete the key from the current sector
}

type RepairReply struct {
    Err Err
}

// for anti-entropy
type TreeSummary struct {
	Sector int // the sector ID of the tree
    Hashes    [][32]byte // the hashes of the nodes in the tree
}

type RepairGetDiffArgs struct {
	Sector int // the sector ID to be reconciled with
	Summary TreeSummary
}


type KeyInfo struct {
    Key    string
    Objects []Object
}

type RepairGetDiffReply struct {
	DiffKeyInfos []KeyInfo
	Err Err
}

// for membership gossip
type SyncMembersArgs struct {
	MemberInfos []MemberInfo
}

type SyncMembersReply struct {
	MemberInfos []MemberInfo
}

// for hinted handoff
type HintedPutArgs struct {
	TargetServer string
	PutArgs PutArgs
}

type HintedPutReply struct {
	Err Err
	// For tracing
	ArrivedAt time.Time
	RespondedAt time.Time
}