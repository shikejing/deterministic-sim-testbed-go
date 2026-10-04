package sim

import "math/bits"

// RNG 是确定性的伪随机源（splitmix64），
// 状态仅为一个 uint64，保证可快照、可复现、不随运行时间发散。
// splitmix64 具有完整的 2^64 周期，长期推进下统计性质稳定。
type RNG struct {
	state uint64
}

func NewRNG(seed uint64) *RNG { return &RNG{state: seed} }

// Uint64 返回下一个伪随机数。
func (r *RNG) Uint64() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// Intn 返回 [0,n) 内的均匀随机整数，n<=0 时返回 0。
// 使用乘高位法避免取模偏差。
func (r *RNG) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	hi, _ := bits.Mul64(r.Uint64(), uint64(n))
	return int(hi)
}

// Float64 返回 [0,1) 内的均匀随机浮点。
func (r *RNG) Float64() float64 {
	return float64(r.Uint64()>>11) * (1.0 / (1 << 53))
}

// Bool 以概率 p 返回 true。
func (r *RNG) Bool(p float64) bool {
	if p <= 0 {
		return false
	}
	if p >= 1 {
		return true
	}
	return r.Float64() < p
}

// State / SetState 用于快照与恢复。
func (r *RNG) State() uint64     { return r.state }
func (r *RNG) SetState(s uint64) { r.state = s }
