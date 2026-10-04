package sim

import (
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// Simulator 是单进程确定性仿真器。
// 所有推进都发生在调用 Run 的 goroutine 内，内部无并发，
// 因此“并行度”配置体现为实体数量与事件密度，而非真实线程。
type Simulator struct {
	cfg  Config
	rng  *RNG
	now  Time
	seq  uint64 // 事件 ID 计数（即已创建事件总数）
	step uint64

	queue eventHeap
	nodes map[NodeID]*nodeState
	order []NodeID // 稳定遍历顺序

	net *network
	log *EventLog

	invariants []namedInvariant
	faults     faultInjector

	clockOffset Time // 已注入的时钟跳变累计

	checkpoints   []storedCheckpoint
	lastViolation *Violation

	replayMode    bool          // 重放中：Emit 只记录到 replayEffects 供校验
	replayEffects []EffectEntry //

	droppedBackpressure uint64 // 因 MaxInFlight 背压丢弃的事件数
}

type storedCheckpoint struct {
	step uint64
	data []byte
}

type nodeState struct {
	kind    string
	node    Node
	crashed bool
}

type namedInvariant struct {
	name string
	fn   Invariant
}

// New 创建一个仿真器。种子相同的两次 New 产生完全一致的运行。
func New(cfg Config) *Simulator {
	cfg = cfg.withDefaults()
	return &Simulator{
		cfg:    cfg,
		rng:    NewRNG(cfg.Seed),
		nodes:  map[NodeID]*nodeState{},
		net:    newNetwork(),
		log:    newEventLog(cfg.LogCap),
		faults: faultInjector{cfg: cfg.Faults},
	}
}

// RegisterNode 把一个节点加入仿真（kind 用于快照恢复时选择工厂）。
func (s *Simulator) RegisterNode(kind string, n Node) {
	id := n.ID()
	s.nodes[id] = &nodeState{kind: kind, node: n}
	s.order = append(s.order, id)
	n.Init(&Context{sim: s, from: id})
}

// AddInvariant 注册一个不变式，运行中按 Config.InvariantEvery 周期校验。
func (s *Simulator) AddInvariant(name string, inv Invariant) {
	s.invariants = append(s.invariants, namedInvariant{name, inv})
}

// InjectClient 在指定虚拟时间向某节点投递一个客户端请求。
func (s *Simulator) InjectClient(at Time, to NodeID, payload any) {
	s.schedule(&Event{Time: at, Kind: KindClient, Dst: to, Payload: payload})
}

// Now 返回当前虚拟时间（不含时钟偏移）。
func (s *Simulator) Now() Time { return s.now }

// Configure 允许在运行前/恢复后调整配置（如提高步数上限继续推进）。
// 修改的是“允许跑多远”的约束，不影响已确定的事件序列。
func (s *Simulator) Configure(mut func(*Config)) {
	mut(&s.cfg)
	s.faults.cfg = s.cfg.Faults
}

// Log 返回事件日志。
func (s *Simulator) Log() *EventLog { return s.log }

// Node 返回某节点实例（供不变式检查读取可见状态）。
func (s *Simulator) Node(id NodeID) Node {
	if ns, ok := s.nodes[id]; ok {
		return ns.node
	}
	return nil
}

// Crashed 报告节点当前是否崩溃。
func (s *Simulator) Crashed(id NodeID) bool {
	if ns, ok := s.nodes[id]; ok {
		return ns.crashed
	}
	return false
}

// Checkpoints 返回已保存检查点的步号列表。
func (s *Simulator) Checkpoints() []uint64 {
	out := make([]uint64, len(s.checkpoints))
	for i, c := range s.checkpoints {
		out[i] = c.step
	}
	return out
}

// CheckpointData 返回指定步号检查点的快照字节。
func (s *Simulator) CheckpointData(step uint64) ([]byte, bool) {
	for _, c := range s.checkpoints {
		if c.step == step {
			return c.data, true
		}
	}
	return nil, false
}

// schedule 分配事件 ID 并入堆；受 MaxInFlight 背压限制。
func (s *Simulator) schedule(ev *Event) {
	if s.cfg.MaxInFlight > 0 && len(s.queue) >= s.cfg.MaxInFlight {
		s.droppedBackpressure++
		return
	}
	s.seq++
	ev.ID = s.seq
	heap.Push(&s.queue, ev)
}

// sendMessage 是 Context.Send 的实现：在发送点做丢失/重复/乱序判定。
func (s *Simulator) sendMessage(from, to NodeID, m Message) {
	d := s.faults.decideSend(s.rng)
	lag := Time(1) + d.extraLag
	fault := ""
	if d.drop {
		fault = "drop"
	}
	s.schedule(&Event{Time: s.now + lag, Kind: KindDeliver, Src: from, Dst: to, Msg: m, Fault: fault})
	for i := 0; i < d.dups; i++ {
		f := fault
		if f != "" {
			f += "+dup"
		} else {
			f = "dup"
		}
		s.schedule(&Event{Time: s.now + lag, Kind: KindDeliver, Src: from, Dst: to, Msg: m, Fault: f})
	}
}

// setTimer 是 Context.After 的实现。
func (s *Simulator) setTimer(from NodeID, delay Time, payload any) {
	if delay < 1 {
		delay = 1
	}
	s.schedule(&Event{Time: s.now + delay, Kind: KindTimer, Dst: from, Payload: payload})
}

// Run 推进仿真直到事件耗尽、达到上限、被取消或不变式违反。
// 无论以何种原因停止，仿真器都停在一个自洽的、可快照的状态。
func (s *Simulator) Run(ctx context.Context) Result {
	for {
		if ctx.Err() != nil {
			return s.result(ReasonCanceled)
		}
		if s.cfg.MaxSteps > 0 && s.step >= s.cfg.MaxSteps {
			return s.result(ReasonMaxSteps)
		}
		if s.cfg.MaxEvents > 0 && s.seq >= s.cfg.MaxEvents {
			return s.result(ReasonMaxEvents)
		}
		if len(s.queue) == 0 {
			return s.result(ReasonQueueEmpty)
		}
		ev := heap.Pop(&s.queue).(*Event)
		if s.cfg.MaxTime > 0 && ev.Time > s.cfg.MaxTime {
			return s.result(ReasonMaxTime)
		}
		s.now = ev.Time
		s.step++
		note := s.dispatch(ev)
		s.faults.maybeSchedule(s)
		s.log.record(LogEntry{Step: s.step, Event: *ev, Hash: s.stateHash(), Note: note})
		if v := s.checkInvariants(); v != nil {
			s.lastViolation = v
			return s.result(ReasonInvariant)
		}
		if s.cfg.CheckpointEvery > 0 && s.step%s.cfg.CheckpointEvery == 0 {
			s.saveCheckpoint()
		}
	}
}

// dispatch 执行一个事件，返回可定位的说明（故障注入点等）。
func (s *Simulator) dispatch(ev *Event) string {
	switch ev.Kind {
	case KindDeliver:
		if strings.Contains(ev.Fault, "drop") {
			return fmt.Sprintf("fault:drop msg %s->%s kind=%s (event #%d)", ev.Src, ev.Dst, ev.Msg.Kind, ev.ID)
		}
		ns := s.nodes[ev.Dst]
		if ns == nil || ns.crashed {
			return fmt.Sprintf("dropped: dst %s crashed", ev.Dst)
		}
		if !s.net.reachable(ev.Src, ev.Dst) {
			return fmt.Sprintf("fault:partition-drop msg %s->%s kind=%s (event #%d)", ev.Src, ev.Dst, ev.Msg.Kind, ev.ID)
		}
		if ev.Fault != "" {
			// dup 副本照常投递，但保留注入点标注。
			ns.node.OnEvent(&Context{sim: s, from: ev.Dst}, *ev)
			return fmt.Sprintf("fault:%s delivered msg %s->%s kind=%s (event #%d)", ev.Fault, ev.Src, ev.Dst, ev.Msg.Kind, ev.ID)
		}
		ns.node.OnEvent(&Context{sim: s, from: ev.Dst}, *ev)
	case KindTimer, KindClient:
		ns := s.nodes[ev.Dst]
		if ns == nil || ns.crashed {
			return fmt.Sprintf("skipped: dst %s crashed", ev.Dst)
		}
		ns.node.OnEvent(&Context{sim: s, from: ev.Dst}, *ev)
	case KindCrash:
		if ns := s.nodes[ev.Dst]; ns != nil {
			ns.crashed = true
		}
		note := fmt.Sprintf("fault:crash node=%s (event #%d)", ev.Dst, ev.ID)
		if s.cfg.Faults.RestartDelay > 0 {
			s.schedule(&Event{Time: s.now + s.cfg.Faults.RestartDelay, Kind: KindRestart, Dst: ev.Dst, Fault: "restart"})
		}
		return note
	case KindRestart:
		if ns := s.nodes[ev.Dst]; ns != nil {
			ns.crashed = false
			ns.node.Init(&Context{sim: s, from: ev.Dst})
		}
		return fmt.Sprintf("fault:restart node=%s (event #%d)", ev.Dst, ev.ID)
	case KindPartition:
		s.net.cut(ev.Src, ev.Dst)
		s.net.cut(ev.Dst, ev.Src)
		if s.cfg.Faults.PartitionLen > 0 {
			s.schedule(&Event{Time: s.now + s.cfg.Faults.PartitionLen, Kind: KindHeal, Src: ev.Src, Dst: ev.Dst, Fault: "heal"})
		}
		return fmt.Sprintf("fault:partition %s<->%s (event #%d)", ev.Src, ev.Dst, ev.ID)
	case KindHeal:
		s.net.heal(ev.Src, ev.Dst)
		s.net.heal(ev.Dst, ev.Src)
		return fmt.Sprintf("fault:heal %s<->%s (event #%d)", ev.Src, ev.Dst, ev.ID)
	case KindClockJump:
		delta, _ := ev.Payload.(Time)
		s.clockOffset += delta
		return fmt.Sprintf("fault:clock-jump +%d ticks (event #%d)", delta, ev.ID)
	}
	return ""
}

// checkInvariants 按配置周期校验全部不变式。
func (s *Simulator) checkInvariants() *Violation {
	if s.cfg.InvariantEvery > 1 && s.step%s.cfg.InvariantEvery != 0 {
		return nil
	}
	for _, inv := range s.invariants {
		if err := inv.fn(s); err != nil {
			return &Violation{
				Name:   inv.name,
				Seed:   s.cfg.Seed,
				Step:   s.step,
				Detail: err.Error(),
				Recent: s.recentEvents(10),
			}
		}
	}
	return nil
}

// recentEvents 返回最近 n 条关键事件的人类可读描述，用于最小复现。
func (s *Simulator) recentEvents(n int) []string {
	entries := s.log.Entries()
	if len(entries) > n {
		entries = entries[len(entries)-n:]
	}
	out := make([]string, len(entries))
	for i, e := range entries {
		desc := fmt.Sprintf("step=%d event=#%d %s %s->%s", e.Step, e.Event.ID, e.Event.Kind, e.Event.Src, e.Event.Dst)
		if e.Note != "" {
			desc += " [" + e.Note + "]"
		}
		out[i] = desc
	}
	return out
}

// saveCheckpoint 保存当前状态的快照为一个检查点。
func (s *Simulator) saveCheckpoint() {
	data, err := s.Snapshot()
	if err != nil {
		return // 快照失败不中断仿真；调用方可显式 Snapshot 获取错误
	}
	s.checkpoints = append(s.checkpoints, storedCheckpoint{step: s.step, data: data})
	if s.cfg.MaxCheckpoints > 0 && len(s.checkpoints) > s.cfg.MaxCheckpoints {
		s.checkpoints = s.checkpoints[len(s.checkpoints)-s.cfg.MaxCheckpoints:]
	}
}

// result 汇总一次运行。
func (s *Simulator) result(reason StopReason) Result {
	return Result{
		Reason:              reason,
		Steps:               s.step,
		Events:              s.seq,
		Now:                 s.now,
		FinalHash:           s.stateHash(),
		Violation:           s.lastViolation,
		Checkpoints:         s.Checkpoints(),
		DroppedBackpressure: s.droppedBackpressure,
	}
}

// StateHasher 是节点可选实现的廉价状态哈希接口。
// 实现后 stateHash 每步不再对节点做完整序列化，长时间运行开销可控。
type StateHasher interface {
	StateHash() uint64
}

// stateHash 计算当前全状态哈希：时钟、随机源、节点状态、事件队列、网络。
// 哈希只依赖确定性数据，同一逻辑状态下任意进程计算结果一致。
// 实现使用二进制编码而非文本格式化，保证每步开销可控。
func (s *Simulator) stateHash() uint64 {
	h := fnv.New64a()
	var buf [8]byte
	put := func(v uint64) {
		binary.LittleEndian.PutUint64(buf[:], v)
		h.Write(buf[:])
	}
	put(uint64(s.now))
	put(s.seq)
	put(s.step)
	put(uint64(s.clockOffset))
	put(s.rng.State())
	for _, id := range s.order {
		ns := s.nodes[id]
		h.Write([]byte(id))
		if ns.crashed {
			put(1)
		} else {
			put(0)
		}
		if sh, ok := ns.node.(StateHasher); ok {
			put(sh.StateHash())
		} else if data, err := ns.node.Snapshot(); err == nil {
			h.Write(data)
		}
	}
	// 事件内容在创建后不可变，ID 唯一确定内容；
	// 队列的等价性由 (ID, Time, Kind, Fault) 序列完整刻画，避免每步反射式格式化。
	for _, ev := range s.queue {
		put(ev.ID)
		put(uint64(ev.Time))
		put(uint64(ev.Kind))
		h.Write([]byte(ev.Fault))
	}
	links := make([][2]NodeID, 0, len(s.net.blocked))
	for link := range s.net.blocked {
		links = append(links, link)
	}
	sort.Slice(links, func(i, j int) bool {
		if links[i][0] != links[j][0] {
			return links[i][0] < links[j][0]
		}
		return links[i][1] < links[j][1]
	})
	for _, link := range links {
		h.Write([]byte(link[0]))
		h.Write([]byte(link[1]))
	}
	return h.Sum64()
}
