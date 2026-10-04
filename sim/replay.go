package sim

import (
	"context"
	"fmt"
)

// Replay 用相同配置与种子从头重放前 upto 步（upto=0 表示放到自然结束），
// 并逐条校验：凡原始日志中记录的 (Step, Hash)，重放后同一步的哈希必须一致；
// 原始运行产生的对外效应也必须逐条复现（仅校验，不再次触发真实副作用）。
// setup 负责注册节点与客户端注入，必须与原始运行使用同一函数。
//
// 重放不接触真实时钟与真实网络：所有推进仍由虚拟事件队列驱动。
func Replay(cfg Config, setup func(*Simulator), log *EventLog, upto uint64) (*Simulator, Result, error) {
	if upto > 0 {
		cfg.MaxSteps = upto
	}
	cfg.LogCap = -1 // 重放需要完整日志用于比对
	s := New(cfg)
	s.replayMode = true
	setup(s)
	res := s.Run(context.Background())

	// 校验逐步哈希。
	orig := make(map[uint64]uint64, len(log.Entries()))
	for _, e := range log.Entries() {
		orig[e.Step] = e.Hash
	}
	for _, e := range s.log.Entries() {
		if want, ok := orig[e.Step]; ok && want != e.Hash {
			return s, res, fmt.Errorf("sim: replay diverged at step %d: hash %016x != recorded %016x",
				e.Step, e.Hash, want)
		}
	}

	// 校验对外效应序列（部分重放时只比较已覆盖步数内的效应）。
	wantEffects := log.Effects()
	if upto > 0 {
		filtered := wantEffects[:0:0]
		for _, e := range wantEffects {
			if e.Step <= upto {
				filtered = append(filtered, e)
			}
		}
		wantEffects = filtered
	}
	gotEffects := s.replayEffects
	if len(gotEffects) != len(wantEffects) {
		return s, res, fmt.Errorf("sim: replay effects count %d != recorded %d",
			len(gotEffects), len(wantEffects))
	}
	for i := range wantEffects {
		if gotEffects[i].Effect != wantEffects[i].Effect ||
			fmt.Sprintf("%#v", gotEffects[i].Data) != fmt.Sprintf("%#v", wantEffects[i].Data) {
			return s, res, fmt.Errorf("sim: replay effect #%d mismatch: got %v, want %v",
				i, gotEffects[i], wantEffects[i])
		}
	}
	return s, res, nil
}

// ResumeFrom 从某个已记录的检查点快照恢复仿真器，
// 之后调用 Run 继续推进，其演化与从未中断的运行逐条一致。
func ResumeFrom(snapshotBytes []byte, factories map[string]func() Node) (*Simulator, error) {
	return Restore(snapshotBytes, factories)
}
