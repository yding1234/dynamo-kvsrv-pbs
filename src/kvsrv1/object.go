package kvsrv

import (
	"6.5840/kvsrv1/meta"
)

type Object struct {
	Value string
	Context meta.Context
}

func NewObject(value string, context meta.Context) *Object {
	return &Object{
		Value: value,
		Context: context,
	}
}
