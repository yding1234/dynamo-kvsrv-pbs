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

func (obj Object) CanBeAddedTo(siblings []Object) bool {
    for _, sibling := range siblings {
        cmp := obj.Context.Compare(sibling.Context)
        if cmp == vclock.Before || cmp == vclock.Equal {
            return false
        }
    }
    return true
}

// add the candidate object into the siblings list.
// precondition: candidate.CanBeAddedTo(siblings) = true, and siblings is sorted by sort
func AddObject(siblings []Object, candidate Object, sort func(i, j Object) bool) Object {
    if sort == nil {
        sort = SortByTimestamp
    }
    kept := make([]Object, len(siblings)+1)

    added := false
    for _, sibling := range siblings {
        if candidate.Context.Compare(sibling.Context) == vclock.After { continue }
        if !added && sort(candidate, sibling) {
            kept = append(kept, candidate)
            added = true
            continue
        }
        kept = append(kept, sibling)
    }
    // if the candidate is not added, add it to the end
    if !added {
        kept = append(kept, candidate)
    }
    return kept
}

// precondition: i.Context.Compare(j.Context) = vclock.Concurrent or vclock.After
func SortByTimestamp(i, j Object) bool {
    return i.Context.Timestamp < j.Context.Timestamp
}

func CopyObjects(siblings []Object) []Object {
    copied := make([]Object, len(siblings))
    for i, sibling := range siblings {
        copied[i] = Object{
            Value: sibling.Value,
            Context: sibling.Context.Copy(),
        }
    }
    return copied
}

func (obj Object) IsEqual(other Object) bool {
	return obj.Context.IsEqual(other.Context)
}

// check if the siblings are sorted by causal order, sort is for breaking ties
func IsOrdered(siblings []Object, sort func(i, j Object) bool) bool {
    if sort == nil {
        sort = SortByTimestamp
    }

    for i := 1; i < len(siblings); i++ {
        if siblings[i-1].Context.Compare(siblings[i].Context) == vclock.After {
            return false
        }
        if !sort(siblings[i-1], siblings[i]) {
            return false
        }
    }
    return true
}

func sortObjects(siblings []Object, sort func(i, j Object) bool) []Object {
    if sort == nil {
        sort = SortByTimestamp
    }
    copied := CopyObjects(siblings)
    for i := 0; i < len(siblings); i++ {
        for j := i + 1; j < len(siblings); j++ {
            if copied[j].Context.Compare(copied[i].Context) == vclock.Before ||
            (copied[j].Context.Compare(copied[i].Context) == vclock.Equal && sort(copied[j], copied[i])) || 
            (copied[j].Context.Compare(copied[i].Context) == vclock.Concurrent && sort(copied[j], copied[i])) {
                copied[i], copied[j] = copied[j], copied[i]
            }
        }
    }
    return copied
}