package sim

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// --- 测试辅助 ---

const kvKind = "kv"

func kvFactories() map[string]func() Node {
	return map[string]func() Node{
		kvKind: func() Node { return &KVNode{} },
	}
}

// setupCluster 注册 n 个 KV 节点并注入 numOps 个客户端写操作。
// 注入时刻与目标节点完全由配置决定，不依赖任何真实源。
func setupCluster(n, numOps int) func(*Simulator) {
	return func(s *Simulator) {
		ids := make([]NodeID, n)
		for i := 0; i < n; i++ {
			ids[i] = NodeID(fmt.Sprintf("n%d", i))
		}
		quorum := n/2 + 1
		for i, id := range ids {
			peers := make([]NodeID, 0, n-1)
			for j, p := range ids {
				if j != i {
					peers = append(peers, p)
				}
			}
			s.RegisterNode(kvKind, NewKVNode(id, peers, quorum))
		}
		for i := 0; i < numOps; i++ {
			op := KVOp{OpID: fmt.Sprintf("op-%d", i), Key: fmt.Sprintf("key-%d", i), Value: fmt.Sprintf("v-%d", i)}
			s.InjectClient(Time(1+i*3), ids[i%n], op)
		}
	}
}

func addKVInvariants(s *Simulator, n int) {
	ids := make([]NodeID, n)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("n%d", i))
	}
	quorum := n/2 + 1
	s.AddInvariant("confirmed-durable", ConfirmedDurable(quorum, ids...))
	s.AddInvariant("no-duplicate-apply", NoDuplicateApply(ids...))
	s.AddInvariant("values-consistent", ValuesConsistent(ids...))
}

func baseConfig(seed uint64) Config {
	return Config{
		Seed:           seed,
		MaxSteps:       2000,
		LogCap:         -1,
		InvariantEvery: 10,
	}
}

func faultConfig() FaultConfig {
	return FaultConfig{
		DropRate:        0.1,
		DupRate:         0.1,
		ReorderRate:     0.2,
		MaxReorderDelay: 5,
		CrashRate:       0.01,
		RestartDelay:    20,
		PartitionRate:   0.01,
		PartitionLen:    30,
		ClockJumpRate:   0.01,
		MaxClockJump:    100,
	}
}

// --- 1. 确定性 ---

func TestDeterministic(t *testing.T) {
	cfg := baseConfig(7)
	cfg.Faults = faultConfig()
	run := func() Result {
		s := New(cfg)
		setupCluster(5, 40)(s)
		addKVInvariants(s, 5)
		return s.Run(context.Background())
	}
	r1, r2 := run(), run()
	if r1.FinalHash != r2.FinalHash {
		t.Fatalf("same seed diverged: %016x vs %016x", r1.FinalHash, r2.FinalHash)
	}
	if r1.Steps != r2.Steps || r1.Events != r2.Events {
		t.Fatalf("step/event counts diverged: %+v vs %+v", r1, r2)
	}
	// 不同种子应（几乎必然）产生不同演化。
	cfg2 := cfg
	cfg2.Seed = 8
	s3 := New(cfg2)
	setupCluster(5, 40)(s3)
	addKVInvariants(s3, 5)
	r3 := s3.Run(context.Background())
	if r3.FinalHash == r1.FinalHash {
		t.Fatalf("different seeds produced identical final hash")
	}
}

// --- 2. 故障注入可定位 ---

func TestFaultInjection(t *testing.T) {
	cfg := baseConfig(42)
	cfg.Faults = faultConfig()
	s := New(cfg)
	setupCluster(5, 60)(s)
	addKVInvariants(s, 5)
	res := s.Run(context.Background())
	if res.Reason == ReasonInvariant {
		t.Fatalf("invariant violated: %v\nrecent: %v", res.Violation, res.Violation.Recent)
	}
	notes := map[string]int{}
	for _, e := range s.Log().Entries() {
		for _, tag := range []string{"fault:drop", "dup", "fault:partition", "fault:crash", "fault:restart", "fault:heal", "fault:clock-jump"} {
			if strings.Contains(e.Note, tag) || strings.Contains(e.Event.Fault, tag) {
				notes[tag]++
			}
		}
	}
	for _, tag := range []string{"fault:drop", "dup", "fault:partition", "fault:crash", "fault:clock-jump"} {
		if notes[tag] == 0 {
			t.Fatalf("expected fault %q to be injected at least once; notes=%v", tag, notes)
		}
	}
	// 每个故障事件都必须能定位：事件 ID + 注入点标注。
	for _, e := range s.Log().Entries() {
		if e.Event.Fault != "" && e.Note == "" {
			t.Fatalf("fault event #%d (%s) lacks locating note", e.Event.ID, e.Event.Fault)
		}
	}
}

// --- 3. 快照与恢复 ---

func TestSnapshotResumeEquivalence(t *testing.T) {
	cfg := baseConfig(99)
	cfg.Faults = faultConfig()
	cfg.MaxSteps = 500

	// 不中断的完整运行。
	full := New(cfg)
	setupCluster(4, 50)(full)
	addKVInvariants(full, 4)
	rFull := full.Run(context.Background())

	// 跑到一半快照，从快照继续。
	half := New(cfg)
	halfCfg := cfg
	halfCfg.MaxSteps = 250
	half = New(halfCfg)
	setupCluster(4, 50)(half)
	addKVInvariants(half, 4)
	rHalf := half.Run(context.Background())
	if rHalf.Reason != ReasonMaxSteps {
		t.Fatalf("expected max-steps stop, got %v", rHalf.Reason)
	}
	snap, err := half.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	resumed, err := Restore(snap, kvFactories())
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	resumed.Configure(func(c *Config) { c.MaxSteps = 500 })
	addKVInvariants(resumed, 4)
	rResumed := resumed.Run(context.Background())

	if rResumed.FinalHash != rFull.FinalHash {
		t.Fatalf("resumed run diverged: %016x != %016x", rResumed.FinalHash, rFull.FinalHash)
	}
	if rResumed.Steps != rFull.Steps {
		t.Fatalf("resumed steps %d != full steps %d", rResumed.Steps, rFull.Steps)
	}
}

func TestCheckpointResume(t *testing.T) {
	cfg := baseConfig(123)
	cfg.Faults = faultConfig()
	cfg.MaxSteps = 300
	cfg.CheckpointEvery = 50
	cfg.MaxCheckpoints = 3

	full := New(cfg)
	setupCluster(4, 40)(full)
	addKVInvariants(full, 4)
	rFull := full.Run(context.Background())
	if len(rFull.Checkpoints) == 0 {
		t.Fatalf("no checkpoints recorded")
	}
	if len(rFull.Checkpoints) > 3 {
		t.Fatalf("checkpoint cap not enforced: %v", rFull.Checkpoints)
	}
	// 从最早保留的检查点恢复，继续推进，终态必须一致。
	data, ok := full.CheckpointData(rFull.Checkpoints[0])
	if !ok {
		t.Fatalf("checkpoint %d missing", rFull.Checkpoints[0])
	}
	resumed, err := ResumeFrom(data, kvFactories())
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	addKVInvariants(resumed, 4)
	rResumed := resumed.Run(context.Background())
	if rResumed.FinalHash != rFull.FinalHash {
		t.Fatalf("checkpoint resume diverged: %016x != %016x", rResumed.FinalHash, rFull.FinalHash)
	}
}

func TestSnapshotVersionMismatch(t *testing.T) {
	cfg := baseConfig(1)
	s := New(cfg)
	setupCluster(2, 5)(s)
	s.Run(context.Background())
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// 未篡改的快照必须能恢复。
	if _, err := Restore(snap, kvFactories()); err != nil {
		t.Fatalf("untampered snapshot should restore: %v", err)
	}
	// 版本号不兼容的快照必须被明确拒绝。
	snap2, err := encodeSnapshotVersion(snapshotVersion + 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Restore(snap2, kvFactories())
	if !errors.Is(err, ErrSnapshotVersion) {
		t.Fatalf("expected ErrSnapshotVersion, got %v", err)
	}
}

// --- 4. 重放 ---

func TestReplay(t *testing.T) {
	cfg := baseConfig(2024)
	cfg.Faults = faultConfig()
	s := New(cfg)
	setup := setupCluster(5, 50)
	setup(s)
	addKVInvariants(s, 5)
	res := s.Run(context.Background())
	if res.Reason == ReasonInvariant {
		t.Fatalf("invariant violated: %v", res.Violation)
	}
	if len(s.Log().Effects()) == 0 {
		t.Fatalf("expected some confirmed effects")
	}
	// 全程重放。
	_, rres, err := Replay(cfg, setup, s.Log(), 0)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if rres.FinalHash != res.FinalHash {
		t.Fatalf("replay final hash %016x != original %016x", rres.FinalHash, res.FinalHash)
	}
	// 部分重放到一半步数。
	half := res.Steps / 2
	_, hres, err := Replay(cfg, setup, s.Log(), half)
	if err != nil {
		t.Fatalf("partial replay: %v", err)
	}
	if hres.Steps != half {
		t.Fatalf("partial replay stopped at %d, want %d", hres.Steps, half)
	}
}

// --- 5. 终止语义 ---

func TestTerminationLimits(t *testing.T) {
	mk := func(mutate func(*Config)) (*Simulator, Result) {
		cfg := baseConfig(5)
		cfg.Faults = faultConfig()
		mutate(&cfg)
		s := New(cfg)
		setupCluster(3, 1000)(s)
		return s, s.Run(context.Background())
	}
	_, r := mk(func(c *Config) { c.MaxSteps = 100 })
	if r.Reason != ReasonMaxSteps || r.Steps != 100 {
		t.Fatalf("MaxSteps: got %v steps=%d", r.Reason, r.Steps)
	}
	_, r = mk(func(c *Config) { c.MaxSteps = 0; c.MaxEvents = 200 })
	if r.Reason != ReasonMaxEvents {
		t.Fatalf("MaxEvents: got %v", r.Reason)
	}
	_, r = mk(func(c *Config) { c.MaxSteps = 0; c.MaxTime = 50 })
	if r.Reason != ReasonMaxTime || r.Now > 50 {
		t.Fatalf("MaxTime: got %v now=%d", r.Reason, r.Now)
	}
	// 外部取消。
	cfg := baseConfig(5)
	cfg.MaxSteps = 0
	s := New(cfg)
	setupCluster(3, 100000)(s)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { cancel() }()
	r = s.Run(ctx)
	if r.Reason != ReasonCanceled {
		t.Fatalf("expected canceled, got %v", r.Reason)
	}
	// 停止后状态必须完整可快照，且不污染后续独立运行。
	if _, err := s.Snapshot(); err != nil {
		t.Fatalf("snapshot after cancel: %v", err)
	}
	s2 := New(cfg)
	setupCluster(3, 10)(s2)
	r2 := s2.Run(context.Background())
	s3 := New(cfg)
	setupCluster(3, 10)(s3)
	r3 := s3.Run(context.Background())
	if r2.FinalHash != r3.FinalHash {
		t.Fatalf("independent runs after cancel diverged")
	}
}

// --- 6. 不变式违反可定位 ---

func TestInvariantViolationReport(t *testing.T) {
	cfg := baseConfig(77)
	s := New(cfg)
	setupCluster(3, 10)(s)
	s.AddInvariant("always-fails-at-10", func(s *Simulator) error {
		if s.Now() >= 10 {
			return fmt.Errorf("synthetic failure for test")
		}
		return nil
	})
	res := s.Run(context.Background())
	if res.Reason != ReasonInvariant {
		t.Fatalf("expected invariant stop, got %v", res.Reason)
	}
	v := res.Violation
	if v == nil || v.Seed != 77 || v.Step == 0 {
		t.Fatalf("violation missing reproduction info: %+v", v)
	}
	if len(v.Recent) == 0 {
		t.Fatalf("violation missing recent event sequence")
	}
	if !strings.Contains(v.Error(), "seed=77") {
		t.Fatalf("violation message lacks seed: %v", v)
	}
}

// --- 7. 长时间 / 多实体运行 ---

func TestLongRunDeterministic(t *testing.T) {
	cfg := baseConfig(31337)
	cfg.Faults = faultConfig()
	cfg.MaxSteps = 8000
	cfg.LogCap = 128 // 限制内存：只保留最近 128 条
	cfg.InvariantEvery = 50
	run := func() Result {
		s := New(cfg)
		setupCluster(10, 800)(s)
		addKVInvariants(s, 10)
		return s.Run(context.Background())
	}
	r1 := run()
	r2 := run()
	if r1.FinalHash != r2.FinalHash {
		t.Fatalf("long run diverged: %016x vs %016x", r1.FinalHash, r2.FinalHash)
	}
	if r1.Reason == ReasonInvariant {
		t.Fatalf("long run violated invariant: %v", r1.Violation)
	}
}

// --- 8. 在途事件上限（并行度/背压） ---

func TestMaxInFlightBackpressure(t *testing.T) {
	cfg := baseConfig(9)
	cfg.MaxInFlight = 16
	run := func() Result {
		s := New(cfg)
		setupCluster(3, 200)(s)
		return s.Run(context.Background())
	}
	r1, r2 := run(), run()
	if r1.DroppedBackpressure == 0 {
		t.Fatalf("expected backpressure drops with MaxInFlight=16")
	}
	if r1.FinalHash != r2.FinalHash || r1.DroppedBackpressure != r2.DroppedBackpressure {
		t.Fatalf("backpressure run not deterministic")
	}
}
