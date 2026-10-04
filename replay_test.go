package sim

import (
	"context"
	"testing"
)

func recordRun(t *testing.T, seed uint64) (*EventLog, Result) {
	t.Helper()
	cfg := faultConfig(seed)
	cfg.RecordLog = true
	cfg.CheckpointEvery = 5
	e := buildKV(cfg)
	res, err := e.Run(context.Background())
	if err != nil {
		t.Fatalf("seed %d record run: %v", seed, err)
	}
	return e.Log(), res
}

// TestReplayFromSeed 验证：仅凭种子与事件日志即可重放到相同状态。
func TestReplayFromSeed(t *testing.T) {
	for _, seed := range []uint64{1, 42, 2024} {
		log, _ := recordRun(t, seed)
		if len(log.Entries) == 0 {
			t.Fatalf("seed %d: empty log", seed)
		}
		cfg := faultConfig(seed)
		if err := Replay(cfg, log, KVNodeFactory(testServer, testOps, testInterval)); err != nil {
			t.Fatalf("seed %d replay: %v", seed, err)
		}
	}
}

// TestReplayFromCheckpoint 验证：从任意已记录检查点重放，结果与完整运行一致。
func TestReplayFromCheckpoint(t *testing.T) {
	seed := uint64(777)
	log, res := recordRun(t, seed)
	if len(log.Checkpoints) == 0 {
		t.Fatal("no checkpoints recorded")
	}
	cfg := faultConfig(seed)
	factory := KVNodeFactory(testServer, testOps, testInterval)
	for _, cp := range log.Checkpoints {
		if err := ReplayFrom(cfg, log, cp.Index, factory); err != nil {
			t.Fatalf("replay from checkpoint %d: %v", cp.Index, err)
		}
	}
	// 从最后一个检查点重放后的最终哈希应与完整运行一致：
	// 上面的 ReplayFrom 已逐事件校验到日志末尾，末尾哈希即 res.StateHash。
	last := log.Entries[len(log.Entries)-1]
	if last.StateHash != res.StateHash {
		t.Fatalf("log tail hash %x != run result %x", last.StateHash, res.StateHash)
	}
}

// TestReplayDetectsTampering 验证：篡改日志会被重放检出。
func TestReplayDetectsTampering(t *testing.T) {
	log, _ := recordRun(t, 99)
	if len(log.Entries) < 2 {
		t.Fatal("log too short")
	}
	log.Entries[len(log.Entries)/2].StateHash ^= 0xdeadbeef
	err := Replay(faultConfig(99), log, KVNodeFactory(testServer, testOps, testInterval))
	if err == nil {
		t.Fatal("expected replay to detect tampered hash")
	}
}
