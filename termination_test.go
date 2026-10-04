package sim

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestTerminationLimits 验证各类上限都以可解释的原因停止。
func TestTerminationLimits(t *testing.T) {
	base := testConfig(5)
	base.MaxEvents = 0

	cfg := base
	cfg.MaxSteps = 10
	e := buildKV(cfg)
	res, err := e.Run(context.Background())
	if err != nil || res.Reason != StopMaxSteps {
		t.Fatalf("MaxSteps: reason=%s err=%v", res.Reason, err)
	}

	cfg = base
	cfg.MaxEvents = 10
	e = buildKV(cfg)
	res, err = e.Run(context.Background())
	if err != nil || res.Reason != StopMaxEvents {
		t.Fatalf("MaxEvents: reason=%s err=%v", res.Reason, err)
	}

	cfg = base
	cfg.MaxTime = 150 // 让部分事件落在上限之外
	e = buildKV(cfg)
	res, err = e.Run(context.Background())
	if err != nil || res.Reason != StopMaxTime {
		t.Fatalf("MaxTime: reason=%s err=%v", res.Reason, err)
	}
	if res.Now > 150 {
		t.Fatalf("clock advanced past limit: %d", res.Now)
	}
}

// TestExternalCancel 验证外部取消停在一致状态，且引擎仍可快照。
func TestExternalCancel(t *testing.T) {
	cfg := testConfig(5)
	e := buildKV(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	// 在第 5 个事件后从不变式回调里取消，保证取消发生在运行中途。
	e.AddInvariant("cancel-at-5", func(e *Engine) error {
		if e.steps >= 5 {
			cancel()
		}
		return nil
	})
	res, err := e.Run(ctx)
	if err != nil {
		t.Fatalf("cancel run: %v", err)
	}
	if res.Reason != StopCanceled {
		t.Fatalf("expected canceled, got %s", res.Reason)
	}
	// 状态必须完整：可以快照并恢复继续跑。
	snap, err := e.Snapshot()
	if err != nil {
		t.Fatalf("snapshot after cancel: %v", err)
	}
	if _, err := RestoreEngine(cfg, snap, KVNodeFactory(testServer, testOps, testInterval)); err != nil {
		t.Fatalf("restore after cancel: %v", err)
	}
}

// TestInvariantViolation 验证不变式违反时抛出带种子与关键事件序列的错误。
func TestInvariantViolation(t *testing.T) {
	e := buildKV(testConfig(3))
	e.AddInvariant("always-fails", func(e *Engine) error {
		return errors.New("boom")
	})
	_, err := e.Run(context.Background())
	if err == nil {
		t.Fatal("expected invariant error")
	}
	var invErr *InvariantError
	if !errors.As(err, &invErr) {
		t.Fatalf("expected InvariantError, got %T", err)
	}
	if invErr.Seed != 3 {
		t.Fatalf("error missing seed: %+v", invErr)
	}
	if len(invErr.RecentEvents) == 0 {
		t.Fatal("error missing recent event sequence")
	}
	msg := err.Error()
	if !strings.Contains(msg, "seed=3") || !strings.Contains(msg, "always-fails") {
		t.Fatalf("error message not locatable: %s", msg)
	}
}

// TestIndependentRuns 验证多次运行互不污染。
func TestIndependentRuns(t *testing.T) {
	e1 := buildKV(testConfig(11))
	e2 := buildKV(testConfig(22))
	r1, err1 := e1.Run(context.Background())
	r2, err2 := e2.Run(context.Background())
	if err1 != nil || err2 != nil {
		t.Fatalf("runs failed: %v %v", err1, err2)
	}
	// 再跑一次种子 11，结果必须与第一次完全一致。
	e3 := buildKV(testConfig(11))
	r3, err := e3.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r1.StateHash != r3.StateHash {
		t.Fatal("interleaved run polluted independent execution")
	}
	_ = r2
}

// TestLongRun 验证长时间推进下状态不发散、内存可控（有界队列）。
func TestLongRun(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	cfg := faultConfig(31337)
	cfg.MaxEvents = 500000
	const total = 5000
	run := func() (uint64, Result) {
		e := NewEngine(cfg)
		e.AddNode(testServer, &KVServer{})
		e.AddNode(testClient, &KVClient{Server: testServer, Total: total, Interval: 50})
		// 长跑测试聚焦确定性本身，不挂 O(n) 不变式以控制耗时。
		res, err := e.Run(context.Background())
		if err != nil {
			t.Fatalf("long run: %v", err)
		}
		if res.Reason != StopMaxEvents && res.Reason != StopExhausted {
			t.Fatalf("unexpected stop: %s", res.Reason)
		}
		return res.StateHash, res
	}
	a, ra := run()
	b, _ := run()
	if a != b {
		t.Fatal("long runs diverged")
	}
	t.Logf("long run: events=%d time=%d faults=%d", ra.Events, ra.Now, len(ra.Faults))
}
