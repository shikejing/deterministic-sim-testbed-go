package sim

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// StopReason 说明仿真为何停止。
type StopReason string

const (
	StopExhausted StopReason = "exhausted"  // 事件队列已空
	StopMaxSteps  StopReason = "max_steps"  // 达到步数上限
	StopMaxEvents StopReason = "max_events" // 达到事件数上限
	StopMaxTime   StopReason = "max_time"   // 达到虚拟时间上限
	StopCanceled  StopReason = "canceled"   // 外部取消
	StopInvariant StopReason = "invariant"  // 不变式违反
)

// Result 是一次运行的结果摘要。无论因何停止，引擎都停在一个
// 完整事件处理完的一致状态，不会留下写了一半的结果。
type Result struct {
	Reason    StopReason
	Steps     uint64
	Events    uint64
	Now       int64
	StateHash uint64
	Faults    []FaultRecord
}

// Invariant 是每次事件处理后校验的不变式。
type Invariant struct {
	Name  string
	Check func(e *Engine) error
}

// InvariantError 携带可定位的最小复现信息：种子、事件 ID 与关键事件序列。
type InvariantError struct {
	Seed         uint64
	EventID      uint64
	Invariant    string
	Cause        error
	RecentEvents []string
}

func (e *InvariantError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "invariant %q violated at event %d (seed=%d): %v\nrecent events:",
		e.Invariant, e.EventID, e.Seed, e.Cause)
	for _, ev := range e.RecentEvents {
		fmt.Fprintf(&b, "\n  %s", ev)
	}
	return b.String()
}

func (e *InvariantError) Unwrap() error { return e.Cause }

// nodeState 是引擎侧保存的节点运行时状态。
type nodeState struct {
	node    Node
	crashed bool
	offset  int64             // 时钟偏移（时钟跳变故障注入）
	timers  map[string]uint64 // 定时器名 -> 最新事件 ID（同名替换）
}

// Engine 是单进程确定性仿真引擎。所有调度都在单 goroutine 内完成，
// 事件处理是原子的：外部取消或达到上限时，状态永远停在事件边界上。
type Engine struct {
	cfg Config
	r   *rng
	now int64
	seq uint64

	queue eventHeap
	held  []Event // 被分区暂扣的消息

	nodes map[NodeID]*nodeState
	order []NodeID // 排序后的节点列表，保证遍历确定

	blocked map[[2]NodeID]bool // 分区中被阻断的（有序）节点对

	steps  uint64
	events uint64

	invariants []Invariant
	faults     []FaultRecord
	recent     []string // 最近事件描述环形缓冲

	log         *EventLog
	lastEventID uint64
}

const recentRingSize = 16

// NewEngine 创建引擎。每个引擎持有独立随机源，互不影响。
func NewEngine(cfg Config) *Engine {
	e := &Engine{
		cfg:     cfg,
		r:       newRNG(cfg.Seed),
		nodes:   make(map[NodeID]*nodeState),
		blocked: make(map[[2]NodeID]bool),
	}
	if cfg.RecordLog {
		e.log = &EventLog{Seed: cfg.Seed}
	}
	return e
}

// AddNode 注册节点并初始化。
func (e *Engine) AddNode(id NodeID, n Node) {
	if _, dup := e.nodes[id]; dup {
		panic(fmt.Sprintf("sim: duplicate node %q", id))
	}
	e.nodes[id] = &nodeState{node: n}
	if e.log != nil {
		e.log.NodeIDs = append(e.log.NodeIDs, id)
	}
	e.order = append(e.order, id)
	sort.Slice(e.order, func(i, j int) bool { return e.order[i] < e.order[j] })
	ctx := &Context{engine: e, self: id}
	if ca, ok := n.(ContextAttacher); ok {
		ca.AttachContext(ctx)
	}
	n.Init(ctx)
}

// AddInvariant 注册不变式，每个事件处理后都会校验。
func (e *Engine) AddInvariant(name string, check func(e *Engine) error) {
	e.invariants = append(e.invariants, Invariant{Name: name, Check: check})
}

// Now 返回当前虚拟时间。
func (e *Engine) Now() int64 { return e.now }

// Node 返回节点实例（供不变式检查读取状态）。
func (e *Engine) Node(id NodeID) Node {
	if ns, ok := e.nodes[id]; ok {
		return ns.node
	}
	return nil
}

// Nodes 返回排序后的节点 ID 列表。
func (e *Engine) Nodes() []NodeID { return append([]NodeID(nil), e.order...) }

// Crashed 报告节点当前是否处于崩溃状态。
func (e *Engine) Crashed(id NodeID) bool {
	ns, ok := e.nodes[id]
	return ok && ns.crashed
}

// Faults 返回已注入的故障记录。
func (e *Engine) Faults() []FaultRecord { return append([]FaultRecord(nil), e.faults...) }

// schedule 将事件按确定顺序入队，返回分配的事件 ID。
func (e *Engine) schedule(ev Event) uint64 {
	ev.ID = e.seq
	ev.Seq = e.seq
	e.seq++
	heapPush(&e.queue, ev)
	return ev.ID
}

// sendMessage 应用消息级故障（丢失/重复/乱序/分区暂扣）后排入队列。
func (e *Engine) sendMessage(from, to NodeID, payload []byte) {
	f := e.cfg.Faults
	if pairKey(from, to, e.blocked) {
		// 分区中：消息暂扣，恢复时统一补投。
		e.held = append(e.held, Event{
			Time: e.now, Kind: EventMessage, From: from, To: to, Payload: payload,
		})
		e.recordFault(FaultPartition, fmt.Sprintf("hold %s->%s", from, to))
		return
	}
	if f.DropProb > 0 && e.r.Bool(f.DropProb) {
		e.recordFault(FaultDrop, fmt.Sprintf("drop %s->%s", from, to))
		return
	}
	latency := e.latency()
	if f.ReorderProb > 0 && e.r.Bool(f.ReorderProb) {
		extra := int64(1 + e.r.Intn(int(max64(f.ReorderMaxDelay, 1))))
		latency += extra
		e.recordFault(FaultReorder, fmt.Sprintf("delay %s->%s by %d", from, to, extra))
	}
	copies := 1
	if f.DuplicateProb > 0 && e.r.Bool(f.DuplicateProb) {
		copies = 2
		e.recordFault(FaultDuplicate, fmt.Sprintf("duplicate %s->%s", from, to))
	}
	for i := 0; i < copies; i++ {
		e.schedule(Event{
			Time: e.now + latency, Kind: EventMessage,
			From: from, To: to, Payload: payload,
		})
	}
}

func (e *Engine) latency() int64 {
	lo, hi := e.cfg.MinLatency, e.cfg.MaxLatency
	if hi <= lo {
		return max64(lo, 1)
	}
	return lo + int64(e.r.Intn(int(hi-lo)))
}

// setTimer 注册定时器事件。同名定时器会被替换：
// 旧事件仍留在队列中，但触发时因 ID 过期而被忽略。
func (e *Engine) setTimer(to NodeID, name string, delay int64) {
	if delay < 0 {
		delay = 0
	}
	ev := Event{Time: e.now + delay, Kind: EventTimer, To: to, TimerName: name}
	id := e.schedule(ev)
	ns := e.nodes[to]
	if ns != nil {
		if ns.timers == nil {
			ns.timers = make(map[string]uint64)
		}
		ns.timers[name] = id
	}
}

// Run 运行仿真直到终止条件，返回可解释的结果。
func (e *Engine) Run(ctx context.Context) (Result, error) {
	for {
		if err := ctx.Err(); err != nil {
			return e.result(StopCanceled), nil
		}
		if e.cfg.MaxSteps > 0 && e.steps >= e.cfg.MaxSteps {
			return e.result(StopMaxSteps), nil
		}
		if e.cfg.MaxEvents > 0 && e.events >= e.cfg.MaxEvents {
			return e.result(StopMaxEvents), nil
		}
		if len(e.queue) == 0 {
			return e.result(StopExhausted), nil
		}
		if e.cfg.MaxTime > 0 && e.queue[0].Time > e.cfg.MaxTime {
			return e.result(StopMaxTime), nil
		}
		if err := e.step(); err != nil {
			return e.result(StopInvariant), err
		}
	}
}

func (e *Engine) result(reason StopReason) Result {
	return Result{
		Reason:    reason,
		Steps:     e.steps,
		Events:    e.events,
		Now:       e.now,
		StateHash: e.stateHash(),
		Faults:    e.Faults(),
	}
}

// step 处理一个事件：推进时钟、派发、注入环境故障、校验不变式、记日志。
func (e *Engine) step() error {
	ev := heapPop(&e.queue)
	e.now = ev.Time
	e.lastEventID = ev.ID
	e.steps++
	e.events++

	desc := e.describe(ev)
	e.pushRecent(desc)

	switch ev.Kind {
	case EventMessage:
		if ns, ok := e.nodes[ev.To]; ok && !ns.crashed {
			ns.node.OnMessage(Message{From: ev.From, To: ev.To, Payload: ev.Payload})
		}
	case EventTimer:
		if ns, ok := e.nodes[ev.To]; ok && !ns.crashed {
			if ns.timers == nil || ns.timers[ev.TimerName] == ev.ID {
				ns.node.OnTimer(ev.TimerName)
			}
			// 否则：该定时器已被同名新定时器替换，忽略。
		}
	case EventFault:
		e.applyFault(ev.Fault)
	}

	// 环境类故障（崩溃/分区/时钟跳变）以事件为粒度按概率注入。
	e.maybeInjectEnvFault()

	for _, inv := range e.invariants {
		if err := inv.Check(e); err != nil {
			return &InvariantError{
				Seed:         e.cfg.Seed,
				EventID:      ev.ID,
				Invariant:    inv.Name,
				Cause:        err,
				RecentEvents: append([]string(nil), e.recent...),
			}
		}
	}

	if e.log != nil {
		e.log.Entries = append(e.log.Entries, LogEntry{
			Index:     e.events,
			EventID:   ev.ID,
			Time:      ev.Time,
			Kind:      ev.Kind,
			Detail:    desc,
			StateHash: e.stateHash(),
		})
		if e.cfg.CheckpointEvery > 0 && e.events%e.cfg.CheckpointEvery == 0 {
			data, err := e.Snapshot()
			if err != nil {
				return fmt.Errorf("sim: checkpoint: %w", err)
			}
			e.log.Checkpoints = append(e.log.Checkpoints, Checkpoint{Index: e.events, Data: data})
		}
	}
	return nil
}

func (e *Engine) describe(ev Event) string {
	switch ev.Kind {
	case EventMessage:
		return fmt.Sprintf("#%d t=%d msg %s->%s (%dB)", ev.ID, ev.Time, ev.From, ev.To, len(ev.Payload))
	case EventTimer:
		return fmt.Sprintf("#%d t=%d timer %s:%s", ev.ID, ev.Time, ev.To, ev.TimerName)
	case EventFault:
		return fmt.Sprintf("#%d t=%d fault %s", ev.ID, ev.Time, ev.Fault.Kind)
	}
	return fmt.Sprintf("#%d t=%d unknown", ev.ID, ev.Time)
}

func (e *Engine) pushRecent(s string) {
	if len(e.recent) >= recentRingSize {
		e.recent = e.recent[1:]
	}
	e.recent = append(e.recent, s)
}

func (e *Engine) recordFault(kind FaultKind, detail string) {
	e.faults = append(e.faults, FaultRecord{
		EventID: e.lastEventID, Time: e.now, Kind: kind, Detail: detail,
	})
}

// applyFault 执行一个已调度的故障动作。
func (e *Engine) applyFault(a *FaultAction) {
	switch a.Kind {
	case FaultCrash:
		ns, ok := e.nodes[a.Target]
		if !ok || ns.crashed {
			return
		}
		ns.crashed = true
		ns.timers = nil // 定时器是易失状态，崩溃即失效
		ns.node.OnCrash()
		e.recordFault(FaultCrash, fmt.Sprintf("crash %s for %d", a.Target, a.Duration))
		e.schedule(Event{
			Time: e.now + a.Duration, Kind: EventFault,
			Fault: &FaultAction{Kind: FaultRestart, Target: a.Target},
		})
	case FaultRestart:
		ns, ok := e.nodes[a.Target]
		if !ok || !ns.crashed {
			return
		}
		ns.crashed = false
		ns.offset = 0
		ns.node.Init(&Context{engine: e, self: a.Target})
		e.recordFault(FaultRestart, fmt.Sprintf("restart %s", a.Target))
	case FaultPartition:
		for _, p := range a.Pairs {
			e.blocked[p] = true
		}
		e.recordFault(FaultPartition, fmt.Sprintf("partition %d pairs for %d", len(a.Pairs), a.Duration))
		// 调度恢复事件。
		e.schedule(Event{
			Time: e.now + a.Duration, Kind: EventFault,
			Fault: &FaultAction{Kind: FaultHeal, Pairs: a.Pairs},
		})
	case FaultHeal:
		for _, p := range a.Pairs {
			delete(e.blocked, p)
		}
		e.recordFault(FaultHeal, fmt.Sprintf("heal %d pairs", len(a.Pairs)))
		e.flushHeld()
	case FaultClockJump:
		if ns, ok := e.nodes[a.Target]; ok {
			ns.offset += a.Delta
			e.recordFault(FaultClockJump, fmt.Sprintf("clock %s %+d", a.Target, a.Delta))
		}
	}
}

func pairKey(a, b NodeID, blocked map[[2]NodeID]bool) bool {
	if a > b {
		a, b = b, a
	}
	return blocked[[2]NodeID{a, b}]
}

// flushHeld 把分区期间暂扣、现已恢复连通的消息重新入队。
// 仍被阻断的消息继续暂扣。
func (e *Engine) flushHeld() {
	var still []Event
	for _, ev := range e.held {
		if pairKey(ev.From, ev.To, e.blocked) {
			still = append(still, ev)
			continue
		}
		ev.Time = e.now + e.latency()
		e.schedule(ev)
	}
	e.held = still
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
