package sim

import (
	"context"
	"testing"
)

func faultConfig(seed uint64) Config {
	cfg := testConfig(seed)
	cfg.Faults = FaultConfig{
		DropProb:        0.2,
		DuplicateProb:   0.2,
		ReorderProb:     0.3,
		ReorderMaxDelay: 200,
		CrashProb:       0.01,
		CrashMinDown:    50,
		CrashMaxDown:    500,
		PartitionProb:   0.01,
		PartMinHold:     50,
		PartMaxHold:     500,
		ClockJumpProb:   0.01,
		ClockJumpMax:    1000,
	}
	return cfg
}

func TestFaultInjectionDeterministic(t *testing.T) {
	run := func() Result {
		e := buildKV(faultConfig(2024))
		res, err := e.Run(context.Background())
		if err != nil {
			t.Fatalf("run with faults: %v", err)
		}
		return res
	}
	a, b := run(), run()
	if a.StateHash != b.StateHash {
		t.Fatalf("fault run diverged: %x != %x", a.StateHash, b.StateHash)
	}
	if len(a.Faults) == 0 {
		t.Fatal("expected faults to be injected")
	}
	// 故障记录逐条一致，且每条都能定位到事件。
	for i := range a.Faults {
		if a.Faults[i] != b.Faults[i] {
			t.Fatalf("fault record %d diverged: %v vs %v", i, a.Faults[i], b.Faults[i])
		}
	}
}

func TestFaultKindsAllInjected(t *testing.T) {
	seen := make(map[FaultKind]bool)
	for seed := uint64(1); seed <= 20; seed++ {
		e := buildKV(faultConfig(seed))
		res, err := e.Run(context.Background())
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		for _, f := range res.Faults {
			seen[f.Kind] = true
		}
	}
	for _, k := range []FaultKind{
		FaultDrop, FaultDuplicate, FaultReorder,
		FaultPartition, FaultHeal, FaultCrash, FaultRestart, FaultClockJump,
	} {
		if !seen[k] {
			t.Errorf("fault kind %s never injected across 20 seeds", k)
		}
	}
}

func TestFaultsDoNotBreakInvariants(t *testing.T) {
	// 高故障率下跑多个种子，不变式（已在引擎中注册）必须始终成立，
	// 且最终所有操作都能完成。
	for seed := uint64(100); seed < 110; seed++ {
		e := buildKV(faultConfig(seed))
		_, err := e.Run(context.Background())
		if err != nil {
			t.Fatalf("seed %d: invariant violated: %v", seed, err)
		}
		if got := len(clientOf(e).Acked); got != testOps {
			t.Fatalf("seed %d: acked %d/%d under faults", seed, got, testOps)
		}
		if got := serverOf(e).AppliedCount; got != testOps {
			t.Fatalf("seed %d: applied %d/%d under faults", seed, got, testOps)
		}
	}
}
