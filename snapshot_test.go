package sim

import (
	"context"
	"errors"
	"testing"
)

// TestSnapshotRestoreIdentical 验证：中途保存快照再恢复继续推进，
// 最终状态与不中断运行逐位一致（含故障注入场景）。
func TestSnapshotRestoreIdentical(t *testing.T) {
	for _, seed := range []uint64{1, 42, 2024} {
		cfg := faultConfig(seed)

		// 不中断的参照运行。
		ref := buildKV(cfg)
		refRes, err := ref.Run(context.Background())
		if err != nil {
			t.Fatalf("seed %d ref: %v", seed, err)
		}

		// 先跑一半，快照，再从快照继续。
		first := buildKV(cfg)
		// 手动推进一半事件。
		target := refRes.Events / 2
		for first.events < target {
			if err := first.step(); err != nil {
				t.Fatalf("seed %d step: %v", seed, err)
			}
		}
		snap, err := first.Snapshot()
		if err != nil {
			t.Fatalf("seed %d snapshot: %v", seed, err)
		}
		restored, err := RestoreEngine(cfg, snap, KVNodeFactory(testServer, testOps, testInterval))
		if err != nil {
			t.Fatalf("seed %d restore: %v", seed, err)
		}
		addKVInvariants(restored)
		contRes, err := restored.Run(context.Background())
		if err != nil {
			t.Fatalf("seed %d continue: %v", seed, err)
		}
		if contRes.StateHash != refRes.StateHash {
			t.Fatalf("seed %d: restored run diverged: %x != %x",
				seed, contRes.StateHash, refRes.StateHash)
		}
		if contRes.Events != refRes.Events {
			t.Fatalf("seed %d: event count %d != %d", seed, contRes.Events, refRes.Events)
		}
	}
}

// TestSnapshotVersionMismatch 验证旧版本快照被明确拒绝。
func TestSnapshotVersionMismatch(t *testing.T) {
	e := buildKV(testConfig(1))
	snap, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// 篡改版本号。
	var tampered []byte
	tampered = append(tampered, snap...)
	old := []byte(`"Version":1`)
	idx := indexOf(tampered, old)
	if idx < 0 {
		t.Fatal("version field not found in snapshot")
	}
	tampered[idx+len(`"Version":`)] = '0'
	_, err = RestoreEngine(testConfig(1), tampered, KVNodeFactory(testServer, testOps, testInterval))
	if !errors.Is(err, ErrSnapshotVersion) {
		t.Fatalf("expected ErrSnapshotVersion, got %v", err)
	}
}

// TestSnapshotCorrupt 验证损坏数据不会悄悄读出不完整状态。
func TestSnapshotCorrupt(t *testing.T) {
	e := buildKV(testConfig(1))
	snap, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	_, err = RestoreEngine(testConfig(1), snap[:len(snap)/2], KVNodeFactory(testServer, testOps, testInterval))
	if err == nil {
		t.Fatal("expected error for truncated snapshot")
	}
}

func indexOf(b, sub []byte) int {
	for i := 0; i+len(sub) <= len(b); i++ {
		if string(b[i:i+len(sub)]) == string(sub) {
			return i
		}
	}
	return -1
}
