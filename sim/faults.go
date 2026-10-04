package sim

// faultInjector 依据 FaultConfig 与仿真随机源决定故障。
// 每一次判定都按固定顺序消耗随机数，因此同一种子下判定序列完全确定；
// 每个被注入的事件都带有 Fault 标注，可在日志中定位到注入点。
type faultInjector struct {
	cfg FaultConfig
}

// sendDecision 是一次消息发送的故障判定结果。
type sendDecision struct {
	drop     bool // 直接丢弃
	dups     int  // 额外复制的份数
	extraLag Time // 乱序追加的延迟
}

// decideSend 对一次发送做丢失/重复/乱序判定。
// 判定顺序固定为 drop -> dup -> reorder，保证可复现。
func (f *faultInjector) decideSend(r *RNG) sendDecision {
	var d sendDecision
	if r.Bool(f.cfg.DropRate) {
		d.drop = true
	}
	if r.Bool(f.cfg.DupRate) {
		d.dups = 1
	}
	if f.cfg.MaxReorderDelay > 0 && r.Bool(f.cfg.ReorderRate) {
		d.extraLag = Time(1 + r.Intn(int(f.cfg.MaxReorderDelay)))
	}
	return d
}

// maybeSchedule 在每一步之后按概率安排崩溃/分区/时钟跳变事件。
// 判定顺序固定为 crash -> partition -> clockJump。
func (f *faultInjector) maybeSchedule(s *Simulator) {
	r := s.rng
	if len(s.order) == 0 {
		return
	}
	if r.Bool(f.cfg.CrashRate) {
		victim := s.order[r.Intn(len(s.order))]
		s.schedule(&Event{Time: s.now, Kind: KindCrash, Dst: victim, Fault: "crash"})
	}
	if len(s.order) >= 2 && r.Bool(f.cfg.PartitionRate) {
		a := s.order[r.Intn(len(s.order))]
		b := s.order[r.Intn(len(s.order))]
		if a != b {
			s.schedule(&Event{Time: s.now, Kind: KindPartition, Src: a, Dst: b, Fault: "partition"})
		}
	}
	if f.cfg.MaxClockJump > 0 && r.Bool(f.cfg.ClockJumpRate) {
		delta := Time(1 + r.Intn(int(f.cfg.MaxClockJump)))
		s.schedule(&Event{Time: s.now, Kind: KindClockJump, Payload: delta, Fault: "clock-jump"})
	}
}
