package sim

import "testing"

func TestRNGDeterministic(t *testing.T) {
	a, b := newRNG(42), newRNG(42)
	for i := 0; i < 1000; i++ {
		if a.Uint64() != b.Uint64() {
			t.Fatalf("rng diverged at %d", i)
		}
	}
}

func TestRNGLongRunNoDrift(t *testing.T) {
	// 长时间推进后，相同种子的随机流仍逐位一致（无状态漂移）。
	a := newRNG(7)
	for i := 0; i < 1_000_000; i++ {
		a.Uint64()
	}
	b := newRNG(7)
	for i := 0; i < 1_000_000; i++ {
		b.Uint64()
	}
	if a.state != b.state {
		t.Fatal("rng state diverged after long run")
	}
}
