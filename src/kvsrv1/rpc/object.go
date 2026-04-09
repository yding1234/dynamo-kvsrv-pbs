package rpc

import (
	"6.5840/vclock"
)

type Object struct {
	Value string
	Context Context
}

func NewObject(value string, context Context) Object {
	return Object{
		Value: value,
		Context: context,
	}
}

// add the candidate object into the siblings list.
func AddObject(siblings []Object, candidate Object) ([]Object, bool) {
    for _, sibling := range siblings {
        cmp := candidate.Context.Compare(sibling.Context)
        if cmp == vclock.Before || cmp == vclock.Equal {
            return siblings, false
        }
    }

    kept := make([]Object, 0, len(siblings)+1)
    for _, sibling := range siblings {
        cmp := candidate.Context.Compare(sibling.Context)
        if cmp != vclock.After {
            kept = append(kept, sibling)
        }
    }
    kept = append(kept, candidate)
    return kept, true
}