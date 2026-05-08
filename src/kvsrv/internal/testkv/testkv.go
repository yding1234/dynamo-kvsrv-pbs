package testkv

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"dynamo-kvsrv/kvsrv/rpc"
	"dynamo-kvsrv/tester"
	"dynamo-kvsrv/kvsrv/vclock"
)

const ElectionTimeout = 1 * time.Second

func RandValue(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

const testContextNode = "__test__"

func Ctx(counter uint64) rpc.Context {
	vc := vclock.NewVClock()
	if counter > 0 {
		vc.SetVersion(testContextNode, counter)
	}
	return rpc.NewContextFromVClock(vc)
}

func ZeroContext() rpc.Context {
	return rpc.NewContext()
}

func Counter(c rpc.Context) uint64 {
	var total uint64
	for _, ver := range c.VC {
		total += ver
	}
	return total
}

func nextContext(c rpc.Context, value string) rpc.Context {
	next := c
	next.VC = c.VC.Copy()
	next.Update(testContextNode, value)
	return next
}

type IKVClerk interface {
	Get(string) (string, rpc.Context, rpc.Err)
	Put(string, string, rpc.Context) rpc.Err
}

type TestClerk struct {
	IKVClerk
	Clnt *tester.Clnt
	Cfg  *tester.Config
}

func (tck *TestClerk) Put(key string, value string, context rpc.Context) rpc.Err {
	tck.Cfg.OpInc()
	return tck.IKVClerk.Put(key, value, context)
}

func (tck *TestClerk) Get(key string) (string, rpc.Context, rpc.Err) {
	tck.Cfg.OpInc()
	return tck.IKVClerk.Get(key)
}

type IClerkMaker interface {
	MakeClerk() IKVClerk
	DeleteClerk(IKVClerk)
}

type Test struct {
	*tester.Config
	t          *testing.T
	mck        IClerkMaker
	randomkeys bool
}

func MakeTest(t *testing.T, cfg *tester.Config, randomkeys bool, mck IClerkMaker) *Test {
	return &Test{
		Config:     cfg,
		t:          t,
		mck:        mck,
		randomkeys: randomkeys,
	}
}

func (ts *Test) Cleanup() {
	ts.Config.End()
	ts.Config.Cleanup()
}

func (ts *Test) ConnectClnts(clnts []*tester.Clnt) {
	for _, c := range clnts {
		c.ConnectAll()
	}
}

func (ts *Test) MakeClerk() IKVClerk {
	return ts.mck.MakeClerk()
}

func (ts *Test) PutAtLeastOnce(ck IKVClerk, key, value string, ver rpc.Context, me int) rpc.Context {
	verPrev := ver
	for {
		err := ts.Put(ck, key, value, ver, me)
		if err == rpc.OK {
			ver = nextContext(ver, value)
			break
		}
		if err == rpc.ErrMaybe || err == rpc.ErrVersion {
			ver = nextContext(ver, value)
		} else {
			if Counter(ver) != 0 {
				ts.Fatalf("Put %v ver %d err %v", key, Counter(ver), err)
			}
		}
	}
	desp := fmt.Sprintf("Put(%v, %v) completes", key, value)
	details := fmt.Sprintf("version: %v -> %v", Counter(verPrev), Counter(ver))
	tester.AnnotateInfo(desp, details)
	return ver
}

func (ts *Test) CheckGet(ck IKVClerk, key, value string, version rpc.Context) {
	tester.AnnotateCheckerBegin(fmt.Sprintf("checking Get(%v) = (%v, %v)", key, value, Counter(version)))
	val, ver, err := ts.Get(ck, key, 0)
	if err != rpc.OK {
		text := fmt.Sprintf("Get(%v) returns error = %v", key, err)
		tester.AnnotateCheckerFailure(text, text)
		ts.Fatalf(text)
	}
	if val != value || Counter(ver) != Counter(version) {
		text := fmt.Sprintf("Get(%v) returns (%v, %v) != (%v, %v)", key, val, Counter(ver), value, Counter(version))
		tester.AnnotateCheckerFailure(text, text)
		ts.Fatalf(text)
	}
	text := fmt.Sprintf("Get(%v) returns (%v, %v) as expected", key, val, Counter(ver))
	tester.AnnotateCheckerSuccess(text, "OK")
}

type ClntRes struct {
	Nok    int
	Nmaybe int
}

func (ts *Test) CheckPutConcurrent(ck IKVClerk, key string, rs []ClntRes, res *ClntRes, reliable bool) {
	e := EntryV{}
	ver0 := ts.GetJson(ck, key, -1, &e)
	for _, r := range rs {
		res.Nok += r.Nok
		res.Nmaybe += r.Nmaybe
	}
	if reliable {
		if Counter(ver0) != uint64(res.Nok) {
			ts.Fatalf("Reliable: Wrong number of puts: server %d clnts %v", Counter(ver0), res)
		}
	} else if Counter(ver0) > uint64(res.Nok+res.Nmaybe) {
		ts.Fatalf("Unreliable: Wrong number of puts: server %d clnts %v", Counter(ver0), res)
	}
}

func (ts *Test) runClient(me int, ca chan ClntRes, done chan struct{}, mkc IClerkMaker, fn Fclnt) {
	ck := mkc.MakeClerk()
	v := fn(me, ck, done)
	ca <- v
	mkc.DeleteClerk(ck)
}

type Fclnt func(int, IKVClerk, chan struct{}) ClntRes

func (ts *Test) SpawnClientsAndWait(nclnt int, t time.Duration, fn Fclnt) []ClntRes {
	ca := make([]chan ClntRes, nclnt)
	done := make(chan struct{})
	for cli := 0; cli < nclnt; cli++ {
		ca[cli] = make(chan ClntRes)
		go ts.runClient(cli, ca[cli], done, ts.mck, fn)
	}
	time.Sleep(t)
	for i := 0; i < nclnt; i++ {
		done <- struct{}{}
	}
	rs := make([]ClntRes, nclnt)
	for cli := 0; cli < nclnt; cli++ {
		rs[cli] = <-ca[cli]
	}
	return rs
}

func (ts *Test) GetJson(ck IKVClerk, key string, me int, v any) rpc.Context {
	val, ver, err := Get(ts.Config, ck, key)
	if err == rpc.OK {
		if err := json.Unmarshal([]byte(val), v); err != nil {
			ts.Fatalf("Unmarshal err %v", Counter(ver))
		}
		return ver
	}
	ts.Fatalf("%d: Get %q err %v", me, key, err)
	return ZeroContext()
}

func (ts *Test) PutJson(ck IKVClerk, key string, v any, ver rpc.Context, me int) rpc.Err {
	b, err := json.Marshal(v)
	if err != nil {
		ts.Fatalf("%d: marshal %v", me, err)
	}
	return Put(ts.Config, ck, key, string(b), ver)
}

func (ts *Test) PutAtLeastOnceJson(ck IKVClerk, key string, value any, ver rpc.Context, me int) rpc.Context {
	b, err := json.Marshal(value)
	if err != nil {
		ts.Fatalf("%d: marshal %v", me, err)
	}
	jsonValue := string(b)

	for {
		if err := ts.Put(ck, key, jsonValue, ZeroContext(), me); err != rpc.ErrMaybe {
			break
		}
		ver = nextContext(ver, jsonValue)
	}
	return ver
}

type EntryV struct {
	Id int
	V  uint64
}

func (ts *Test) OnePut(me int, ck IKVClerk, key string, ver rpc.Context) (rpc.Context, bool) {
	for {
		err := ts.PutJson(ck, key, EntryV{me, Counter(ver)}, ver, me)
		if !(err == rpc.OK || err == rpc.ErrVersion || err == rpc.ErrMaybe) {
			ts.Fatalf("Wrong error %v", err)
		}
		e := EntryV{}
		ver0 := ts.GetJson(ck, key, me, &e)
		if err == rpc.OK && Counter(ver0) == Counter(ver)+1 {
			if e.Id != me && e.V != Counter(ver) {
				ts.Fatalf("Wrong value %v", e)
			}
		}
		ver = ver0
		if err == rpc.OK || err == rpc.ErrMaybe {
			return ver, err == rpc.OK
		}
	}
}

func (ts *Test) Partitioner(gid tester.Tgid, ch chan bool) {
	defer func() { ch <- true }()
	for {
		select {
		case <-ch:
			return
		default:
			a := make([]int, ts.Group(gid).N())
			for i := 0; i < ts.Group(gid).N(); i++ {
				a[i] = (rand.Int() % 2)
			}
			pa := make([][]int, 2)
			for i := 0; i < 2; i++ {
				pa[i] = make([]int, 0)
				for j := 0; j < ts.Group(gid).N(); j++ {
					if a[j] == i {
						pa[i] = append(pa[i], j)
					}
				}
			}
			ts.Group(gid).Partition(pa[0], pa[1])
			tester.AnnotateTwoPartitions(pa[0], pa[1])
			time.Sleep(ElectionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
		}
	}
}

func (ts *Test) OneClientPut(cli int, ck IKVClerk, ka []string, done chan struct{}) ClntRes {
	res := ClntRes{}
	verm := make(map[string]rpc.Context)
	for _, k := range ka {
		verm[k] = ZeroContext()
	}
	ok := false
	for {
		select {
		case <-done:
			return res
		default:
			k := ka[0]
			if ts.randomkeys {
				k = ka[rand.Int()%len(ka)]
			}
			verm[k], ok = ts.OnePut(cli, ck, k, verm[k])
			if ok {
				res.Nok += 1
			} else {
				res.Nmaybe += 1
			}
		}
	}
}

func MakeKeys(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "k" + strconv.Itoa(i)
	}
	return keys
}

func (ts *Test) SpreadPutsSize(ck IKVClerk, n, valsz int) ([]string, []string) {
	ka := MakeKeys(n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		va[i] = tester.Randstring(valsz)
		ck.Put(ka[i], va[i], ZeroContext())
	}
	for i := 0; i < n; i++ {
		ts.CheckGet(ck, ka[i], va[i], Ctx(1))
	}
	return ka, va
}

func (ts *Test) SpreadPuts(ck IKVClerk, n int) ([]string, []string) {
	return ts.SpreadPutsSize(ck, n, 20)
}

type entry struct {
	Id int
	N  int
}

func (ts *Test) OneClientAppend(me int, ck IKVClerk, done chan struct{}) ClntRes {
	nmay := 0
	nok := 0
	for i := 0; ; i++ {
		select {
		case <-done:
			return ClntRes{nok, nmay}
		default:
			for {
				es := []entry{}
				ver := ts.GetJson(ck, "k", me, &es)
				es = append(es, entry{me, i})
				if err := ts.PutJson(ck, "k", es, ver, me); err == rpc.OK {
					nok += 1
					break
				} else if err == rpc.ErrMaybe {
					nmay += 1
					break
				}
			}
		}
	}
}

type EntryN struct {
	Id int
	N  int
}

func (ts *Test) CheckAppends(es []EntryN, nclnt int, rs []ClntRes, ver rpc.Context) {
	expect := make(map[int]int)
	skipped := make(map[int]int)
	for i := 0; i < nclnt; i++ {
		expect[i] = 0
		skipped[i] = 0
	}
	for _, e := range es {
		if expect[e.Id] > e.N {
			ts.Fatalf("%d: wrong expecting %v but got %v", e.Id, expect[e.Id], e.N)
		} else if expect[e.Id] == e.N {
			expect[e.Id] += 1
		} else {
			s := (e.N - expect[e.Id])
			expect[e.Id] = e.N + 1
			skipped[e.Id] += s
		}
	}
	if len(es)+1 != int(Counter(ver)) {
		ts.Fatalf("%d appends in val != puts on server %d", len(es), Counter(ver))
	}
	for c, n := range expect {
		if skipped[c] > rs[c].Nmaybe {
			ts.Fatalf("%d: skipped puts %d on server > %d maybe", c, skipped[c], rs[c].Nmaybe)
		}
		if n > rs[c].Nok+rs[c].Nmaybe {
			ts.Fatalf("%d: %d puts on server > ok+maybe %d", c, n, rs[c].Nok+rs[c].Nmaybe)
		}
	}
}

func Get(cfg *tester.Config, ck IKVClerk, key string) (string, rpc.Context, rpc.Err) {
	cfg.OpInc()
	return ck.Get(key)
}

func Put(cfg *tester.Config, ck IKVClerk, key string, value string, context rpc.Context) rpc.Err {
	cfg.OpInc()
	return ck.Put(key, value, context)
}

func (ts *Test) Get(ck IKVClerk, key string, _ int) (string, rpc.Context, rpc.Err) {
	ts.OpInc()
	return ck.Get(key)
}

func (ts *Test) Put(ck IKVClerk, key string, value string, context rpc.Context, _ int) rpc.Err {
	ts.OpInc()
	return ck.Put(key, value, context)
}
