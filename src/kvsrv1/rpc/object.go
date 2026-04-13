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

// check if the object can be added to the others by causal order
func (obj Object) CanBeAddedTo(others []Object) bool {
    for _, other := range others {
        cmp := obj.Context.Compare(other.Context)
        if cmp == vclock.Before || cmp == vclock.Equal {
            return false
        }
    }
    return true
}

// add the candidate object into the siblings list.
// precondition: candidate.CanBeAddedTo(siblings) = true, and siblings is sorted by sort
func AddObject(siblings []Object, candidate Object, sort func(i, j Object) bool) []Object {
    if sort == nil {
        sort = SortByTimestamp
    }
    kept := make([]Object, 0, len(siblings)+1)

    added := false
    for _, sibling := range siblings {
        cmp := candidate.Context.Compare(sibling.Context)
        if cmp == vclock.After {
            continue
        }
        if cmp == vclock.Equal {
            kept = append(kept, sibling)
            added = true
            continue
        }
        if !added && (cmp == vclock.Before || (cmp == vclock.Concurrent && sort(candidate, sibling))) {
            kept = append(kept, candidate)
            added = true
        }
        kept = append(kept, sibling)
    }
    // if the candidate is not added, add it to the end
    if !added {
        kept = append(kept, candidate)
    }
    return kept
}

// merge the siblings and otherSiblings into a new sorted list
// precondition: objects and otherObjects are already sorted by causal order
func MergeObjects(objects []Object, otherObjects []Object) []Object {
    merged := make([]Object, 0, len(objects)+len(otherObjects))
    for _, obj := range objects {
        merged = AddObject(merged, obj, nil) // nil means no specify sort function
    }
    for _, obj := range otherObjects {
        merged = AddObject(merged, obj, nil)
    }
    return merged
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

func SortObjects(siblings []Object, sort func(i, j Object) bool) []Object {
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