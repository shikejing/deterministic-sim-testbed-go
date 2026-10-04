package sim

import "fmt"

// FaultKind 标记故障类型。
type FaultKind string

const (
	FaultDrop      FaultKind = "drop"       // 消息丢失
	FaultDuplicate FaultKind = "duplicate"  // 消息重复
	FaultReorder   FaultKind = "reorder"    // 消息乱序
	FaultPartition FaultKind = "partition"  // 网络分区
	FaultHeal      FaultKind = "heal"       // 分区恢复
	FaultCrash     FaultKind = "crash"      // 节点崩溃
	FaultRestart   FaultKind = "restart"    // 节点重启
	FaultClockJump FaultKind = "clock_jump" // 时钟跳变
)

// FaultAction 描述一次故障注入动作，作为事件进入调度队列。
type FaultAction struct {
	Kind     FaultKind
	Target   NodeID      // 崩溃/重启/时钟跳变的目标节点
	Pairs    [][2]NodeID // 分区/恢复涉及的节点对
	Duration int64       // 分区/崩溃持续时长
	Delta    int64       // 时钟跳变幅度
}

// FaultRecord 记录一次已发生的故障，可定位到具体事件。
type FaultRecord struct {
	EventID uint64
	Time    int64
	Kind    FaultKind
	Detail  string
}

func (f FaultRecord) String() string {
	return fmt.Sprintf("event=%d time=%d %s %s", f.EventID, f.Time, f.Kind, f.Detail)
}

// maybeInjectEnvFault 在每个事件处理后按配置概率注入环境类故障。
// 所有随机决策都来自引擎随机流，因此完全由种子决定。
func (e *Engine) maybeInjectEnvFault() {
	f := e.cfg.Faults
	n := len(e.order)
	if n == 0 {
		return
	}
	if f.CrashProb > 0 && e.r.Bool(f.CrashProb) {
		target := e.order[e.r.Intn(n)]
		down := f.CrashMinDown
		if f.CrashMaxDown > f.CrashMinDown {
			down += int64(e.r.Intn(int(f.CrashMaxDown - f.CrashMinDown)))
		}
		if down <= 0 {
			down = 1
		}
		e.schedule(Event{
			Time: e.now, Kind: EventFault,
			Fault: &FaultAction{Kind: FaultCrash, Target: target, Duration: down},
		})
	}
	if f.PartitionProb > 0 && n >= 2 && e.r.Bool(f.PartitionProb) {
		// 随机二划分：打乱节点顺序后切成两半，阻断跨切分的所有节点对。
		perm := e.rperm(n)
		cut := 1 + e.r.Intn(n-1)
		var pairs [][2]NodeID
		for i := 0; i < cut; i++ {
			for j := cut; j < n; j++ {
				a, b := e.order[perm[i]], e.order[perm[j]]
				if a > b {
					a, b = b, a
				}
				pairs = append(pairs, [2]NodeID{a, b})
			}
		}
		hold := f.PartMinHold
		if f.PartMaxHold > f.PartMinHold {
			hold += int64(e.r.Intn(int(f.PartMaxHold - f.PartMinHold)))
		}
		if hold <= 0 {
			hold = 1
		}
		e.schedule(Event{
			Time: e.now, Kind: EventFault,
			Fault: &FaultAction{Kind: FaultPartition, Pairs: pairs, Duration: hold},
		})
	}
	if f.ClockJumpProb > 0 && e.r.Bool(f.ClockJumpProb) {
		target := e.order[e.r.Intn(n)]
		maxJump := max64(f.ClockJumpMax, 1)
		delta := int64(e.r.Intn(int(2*maxJump))) - maxJump
		if delta == 0 {
			delta = maxJump
		}
		e.schedule(Event{
			Time: e.now, Kind: EventFault,
			Fault: &FaultAction{Kind: FaultClockJump, Target: target, Delta: delta},
		})
	}
}

// rperm 返回 [0,n) 的确定性随机排列（Fisher-Yates）。
func (e *Engine) rperm(n int) []int {
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	for i := n - 1; i > 0; i-- {
		j := e.r.Intn(i + 1)
		perm[i], perm[j] = perm[j], perm[i]
	}
	return perm
}
