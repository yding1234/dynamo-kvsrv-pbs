package tester

import (
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"
)

type KvInput struct {
	Op      uint8 // 0 => get, 1 => put
	Key     string
	Value   string
	Version uint64
}

type KvOutput struct {
	Value   string
	Version uint64
	Err     string
}

type KvState struct {
	Value   string
	Version uint64
}

// KvModel is used only for annotation/visualization placeholders with Porcupine.
var KvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := make(map[string][]porcupine.Operation)
		for _, v := range history {
			key := v.Input.(KvInput).Key
			m[key] = append(m[key], v)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ret := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			ret = append(ret, m[k])
		}
		return ret
	},
	Init: func() interface{} {
		return KvState{"", 0}
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		inp := input.(KvInput)
		out := output.(KvOutput)
		st := state.(KvState)
		switch inp.Op {
		case 0:
			return out.Value == st.Value, state
		case 1:
			if st.Version == inp.Version {
				return out.Err == "OK" || out.Err == "ErrMaybe", KvState{inp.Value, st.Version + 1}
			}
			return out.Err == "ErrVersion" || out.Err == "ErrMaybe", st
		default:
			return false, "<invalid>"
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(KvInput)
		out := output.(KvOutput)
		switch inp.Op {
		case 0:
			return fmt.Sprintf("get('%s') -> ('%s', '%d', '%s')", inp.Key, out.Value, out.Version, out.Err)
		case 1:
			return fmt.Sprintf("put('%s', '%s', '%d') -> ('%s')", inp.Key, inp.Value, inp.Version, out.Err)
		default:
			return "<invalid>"
		}
	},
}
