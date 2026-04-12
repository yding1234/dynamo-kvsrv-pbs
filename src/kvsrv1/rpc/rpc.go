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

type TreeSummary struct {
    SectorID int // the sector ID of the tree
    // Node index 0 is the root, 1-2 is the second layer, 3-6 is the third layer...
	// TODO: use the BFS order to store the nodes
    Nodes    [][32]byte
}

// for anti-entropy
type RepairGetDiffArgs struct {
	SectorID int // the sector ID to be reconciled with
	TreeSummary TreeSummary
}


// TODO: TO BE CHECKED
type RepairGetDiffReply struct {
	Diff map[int][]TreeSummary // the difference between the two merkle trees
	Err Err
}

type AntiEntropyHashArgs struct {
    SectorID int
    Level    int
}

type AntiEntropyHashReply struct {
    Err  Err
    Hash [32]byte
}

// 如果某个 range hash 不同，就请求它的左右子区间 hash。
type AntiEntropyChildrenReply struct {
    Err       Err
    LeftHash  [32]byte
    RightHash [32]byte
    MidKey string // the key of the middle child
}

// 到叶子后，直接拿这个 leaf 里的 key 和对象集。
type AntiEntropyLeafReply struct {
    Err   Err
    Items map[string][]Object
}