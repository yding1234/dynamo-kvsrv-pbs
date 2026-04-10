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

// for read repair
type RepairArgs struct {
    Key     string
    Objects []Object // the canonical siblings
    Delete  bool
}

type RepairReply struct {
    Err Err
}

// for anti-entropy
type AntiEntropyHashArgs struct {
    PeerID string
    Start  int
    End    int
    Level  int
}

