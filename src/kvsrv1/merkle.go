package kvsrv

import (
	"crypto/sha256"
	"encoding/hex"
	"6.5840/kvsrv1/rpc"
)

type objectDigest struct {
    Value     string
    Timestamp uint64
    VCEntries []vcEntry // by lexicographical order
}

func NewObjectDigest(obj rpc.Object) *objectDigest {
    return &objectDigest{
        Value: obj.Value,
        Timestamp: obj.Context.Timestamp,
        VCEntries: obj.Context.VClock.ToVCEntries(nil), // sort by node name
    }
}

// hash follows these two rules:
// 1. all the literals (strings and integers) are concatenated together beginning with its length
// 2. all integers are encoded in little-endian order

func writeUint64(h hash.Hash, x uint64) {
    var buf [8]byte
    binary.LittleEndian.PutUint64(buf[:], x)
    h.Write(buf[:])
}

func writeString(h hash.Hash, s string) {
    writeUint64(h, uint64(len(s)))
    h.Write([]byte(s))
}

func (obj *objectDigest) WriteObjectDigest(h hash.Hash) {
    writeString(h, obj.Value)
    writeUint64(h, obj.Timestamp)
    for _, entry := range obj.VCEntries {
        writeString(h, entry.Node)
        writeUint64(h, entry.Version)
    }
}

func GetDigest(key string, objs []rpc.Object, h hash.Hash) string {
	if h == nil {
		h = sha256.New()
	}

    writeString(h, key)
	writeUint64(h, uint64(len(objs)))

	if !rpc.IsOrdered(objs, nil) {
		objs = rpc.SortObjects(objs, nil)
	}
    for _, obj := range objs {
        NewObjectDigest(obj).WriteObjectDigest(h)
    }
    return hex.EncodeToString(h.Sum(nil))
}