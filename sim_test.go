package sim

import (
	"context"
	"fmt"
	"testing"
)

const (
	testServer   NodeID = "server"
	testClient   NodeID = "client"
	testOps             = 20
	testInterval        = 100
)

func testConfig(seed uint64) Config {
	return Config{
		Seed:       seed,
		MinLatency: 1,
		MaxLatency: 50,
		MaxEvents:  100000,
	}
}

// buildKV 构造 client+server 拓扑并注册标准不变式。
func buildKV(cfg Config) *Engine {
	e := NewEngine(cfg)
	e.AddNode(testServer, &KVServer{})
	e.AddNode(testClient, &KVClient{Server: testServer, Total: testOps, Interval: testInterval})
	addKVInvariants(e)
	return e
}

func serverOf(e *Engine) *KVServer { return e.Node(testServer).(*KVServer) }
func clientOf(e *Engine) *KVClient { return e.Node(testClient).(*KVClient) }

func addKVInvariants(e *Engine) {
	// 已确认的结果不能丢失：客户端每个 ack 都对应服务器已应用的 OpID。
	e.AddInvariant("acked-not-lost", func(e *Engine) error {
		s := serverOf(e)
		for id := range clientOf(e).Acked {
			if !s.Applied[id] {
				return fmt.Errorf("acked op %d missing at server", id)
			}
		}
		return nil
	})
	// 同一操作不能重复生效。
	e.AddInvariant("no-duplicate-apply", func(e *Engine) error {
		s := serverOf(e)
		if s.AppliedCount != len(s.Applied) {
			return fmt.Errorf("applied count %d != unique ops %d", s.AppliedCount, len(s.Applied))
		}
		return nil
	})
	// 可见状态自洽：每个已应用的写都体现在 Store 中。
	e.AddInvariant("store-consistent", func(e *Engine) error {
		s := serverOf(e)
		for id := range s.Applied {
			k := fmt.Sprintf("key-%d", id)
			if s.Store[k] != fmt.Sprintf("value-%d", id) {
				return fmt.Errorf("store[%s] inconsistent for op %d", k, id)
			}
		}
		return nil
	})
}

func TestDeterminism(t *testing.T) {
	run := func() Result {
		e := buildKV(testConfig(1234))
		res, err := e.Run(context.Background())
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return res
	}
	a, b := run(), run()
	if a.StateHash != b.StateHash {
		t.Fatalf("same seed diverged: %x != %x", a.StateHash, b.StateHash)
	}
	if a.Events != b.Events || a.Now != b.Now {
		t.Fatalf("event/time mismatch: %+v vs %+v", a, b)
	}
	if len(a.Faults) != len(b.Faults) {
		t.Fatalf("fault records diverged")
	}
	c := buildKV(testConfig(999))
	resC, err := c.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	_ = resC // 不同种子允许不同结果，只要求能跑完
}

func TestCompletesAllOps(t *testing.T) {
	e := buildKV(testConfig(7))
	res, err := e.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Reason != StopExhausted {
		t.Fatalf("expected exhausted, got %s", res.Reason)
	}
	if got := len(clientOf(e).Acked); got != testOps {
		t.Fatalf("acked %d/%d ops", got, testOps)
	}
	if got := serverOf(e).AppliedCount; got != testOps {
		t.Fatalf("applied %d/%d ops", got, testOps)
	}
}
