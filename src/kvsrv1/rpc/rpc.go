package rpc

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

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Objects []Object
	Err     Err
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

type HintedPutArgs struct {
	TargetServer string
	Key          string
	Object       Object
	BaseContext  Context
}

type HintedPutReply struct {
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