package sim

// rng 是确定性伪随机数源（splitmix64），状态可序列化，
// 给定种子后输出序列在任何机器、任何进程中都完全一致。
// 不依赖 math/rand，避免 Go 版本间算法差异。
type rng struct {
	state uint64
}

func newRNG(seed uint64) *rng { return &rng{state: seed} }

// Uint64 返回下一个伪随机数（splitmix64 递推）。
func (r *rng) Uint64() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// Intn 返回 [0, n) 内的均匀随机整数；n <= 0 时返回 0。
func (r *rng) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.Uint64() % uint64(n))
}

// Float64 返回 [0, 1) 内的均匀随机浮点数。
func (r *rng) Float64() float64 {
	return float64(r.Uint64()>>11) / (1 << 53)
}

// Bool 以概率 p 返回 true。
func (r *rng) Bool(p float64) bool {
	if p <= 0 {
		return false
	}
	if p >= 1 {
		return true
	}
	return r.Float64() < p
}
