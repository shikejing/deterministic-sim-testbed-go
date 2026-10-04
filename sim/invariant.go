package sim

import "fmt"

// Invariant 是对任意时刻可见状态的自洽性校验。
// 返回 nil 表示通过；返回错误即视为违反。
type Invariant func(s *Simulator) error

// Violation 是不变式违反时的最小复现信息。
type Violation struct {
	Name   string   // 不变式名称
	Seed   uint64   // 复现所需种子
	Step   uint64   // 违反发生时的步数
	Detail string   // 具体描述
	Recent []string // 最近关键事件序列（人类可读）
}

func (v *Violation) Error() string {
	return fmt.Sprintf("invariant %q violated at step %d (seed=%d): %s",
		v.Name, v.Step, v.Seed, v.Detail)
}
