package sim

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
)

// SnapshotVersion 是快照格式版本；不兼容的旧版本会被明确拒绝。
const SnapshotVersion = 1

// ErrSnapshotVersion 表示快照版本不兼容。
var ErrSnapshotVersion = errors.New("sim: incompatible snapshot version")

// snapshotDisk 是快照的磁盘格式。字段齐全、显式版本化，
// 恢复旧版本或缺字段的数据会报错而不是悄悄读出半成品。
type snapshotDisk struct {
	Version     int
	Now         int64
	Seq         uint64
	Steps       uint64
	Events      uint64
	RNGState    uint64
	Queue       []Event
	Held        []Event
	Nodes       []nodeDisk
	Blocked     [][2]NodeID
	Faults      []FaultRecord
	Recent      []string
	LastEventID uint64
	// 注意：事件日志不进快照。日志含检查点，检查点含快照，
	// 嵌套序列化会指数膨胀；恢复时按配置开启新日志即可，
	// 事件序号从恢复点继续，不影响确定性。
}

type nodeDisk struct {
	ID      NodeID
	Crashed bool
	Offset  int64
	Timers  map[string]uint64
	Data    []byte
}

// Snapshot 返回引擎完整状态的可序列化快照。
// 快照是确定性的：同一状态在任何机器上产生相同字节。
func (e *Engine) Snapshot() ([]byte, error) {
	d := snapshotDisk{
		Version:     SnapshotVersion,
		Now:         e.now,
		Seq:         e.seq,
		Steps:       e.steps,
		Events:      e.events,
		RNGState:    e.r.state,
		Queue:       append([]Event(nil), e.queue...),
		Held:        append([]Event(nil), e.held...),
		Faults:      e.Faults(),
		Recent:      append([]string(nil), e.recent...),
		LastEventID: e.lastEventID,
	}
	for p := range e.blocked {
		d.Blocked = append(d.Blocked, p)
	}
	sort.Slice(d.Blocked, func(i, j int) bool {
		if d.Blocked[i][0] != d.Blocked[j][0] {
			return d.Blocked[i][0] < d.Blocked[j][0]
		}
		return d.Blocked[i][1] < d.Blocked[j][1]
	})
	for _, id := range e.order {
		ns := e.nodes[id]
		data, err := ns.node.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("sim: snapshot node %s: %w", id, err)
		}
		var timers map[string]uint64
		if len(ns.timers) > 0 {
			timers = make(map[string]uint64, len(ns.timers))
			for k, v := range ns.timers {
				timers[k] = v
			}
		}
		d.Nodes = append(d.Nodes, nodeDisk{
			ID: id, Crashed: ns.crashed, Offset: ns.offset, Timers: timers, Data: data,
		})
	}
	return json.Marshal(d)
}

// NodeFactory 按节点 ID 构造空节点实例用于恢复。
type NodeFactory func(id NodeID) Node

// RestoreEngine 从快照恢复引擎；版本不兼容时返回 ErrSnapshotVersion，
// 数据缺字段或节点恢复失败时返回具体错误，绝不返回不完整的引擎。
func RestoreEngine(cfg Config, data []byte, factory NodeFactory) (*Engine, error) {
	var d snapshotDisk
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("sim: decode snapshot: %w", err)
	}
	if d.Version != SnapshotVersion {
		return nil, fmt.Errorf("%w: got version %d, want %d", ErrSnapshotVersion, d.Version, SnapshotVersion)
	}
	if factory == nil {
		return nil, errors.New("sim: RestoreEngine requires a NodeFactory")
	}
	e := NewEngine(cfg)
	e.now = d.Now
	e.seq = d.Seq
	e.steps = d.Steps
	e.events = d.Events
	e.r.state = d.RNGState
	e.queue = append(eventHeap(nil), d.Queue...)
	e.held = append([]Event(nil), d.Held...)
	e.recent = append([]string(nil), d.Recent...)
	e.lastEventID = d.LastEventID
	e.faults = append([]FaultRecord(nil), d.Faults...)
	for _, p := range d.Blocked {
		e.blocked[p] = true
	}
	for _, nd := range d.Nodes {
		n := factory(nd.ID)
		if n == nil {
			return nil, fmt.Errorf("sim: factory returned nil for node %s", nd.ID)
		}
		if ca, ok := n.(ContextAttacher); ok {
			ca.AttachContext(&Context{engine: e, self: nd.ID})
		}
		if err := n.Restore(nd.Data); err != nil {
			return nil, fmt.Errorf("sim: restore node %s: %w", nd.ID, err)
		}
		e.nodes[nd.ID] = &nodeState{node: n, crashed: nd.Crashed, offset: nd.Offset, timers: nd.Timers}
		e.order = append(e.order, nd.ID)
	}
	sort.Slice(e.order, func(i, j int) bool { return e.order[i] < e.order[j] })
	if cfg.RecordLog {
		e.log = &EventLog{Seed: cfg.Seed, NodeIDs: append([]NodeID(nil), e.order...)}
	}
	return e, nil
}

// stateHash 计算引擎整体状态的确定性哈希。
// 只依赖状态本身，不依赖真实时间或运行环境。
func (e *Engine) stateHash() uint64 {
	h := fnv.New64a()
	var buf [8]byte
	putU64 := func(v uint64) { binary.LittleEndian.PutUint64(buf[:], v); h.Write(buf[:]) }
	putI64 := func(v int64) { putU64(uint64(v)) }
	putStr := func(s string) { h.Write([]byte(s)); h.Write([]byte{0}) }
	putBool := func(b bool) {
		if b {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	}

	putI64(e.now)
	putU64(e.seq)
	putU64(e.r.state)
	putU64(e.steps)
	putU64(e.events)

	putEvent := func(ev *Event) {
		putU64(ev.ID)
		putI64(ev.Time)
		putU64(uint64(ev.Kind))
		putStr(string(ev.From))
		putStr(string(ev.To))
		h.Write(ev.Payload)
		h.Write([]byte{0})
		putStr(ev.TimerName)
		if ev.Fault != nil {
			putStr(string(ev.Fault.Kind))
			putStr(string(ev.Fault.Target))
			putI64(ev.Fault.Duration)
			putI64(ev.Fault.Delta)
			for _, p := range ev.Fault.Pairs {
				putStr(string(p[0]))
				putStr(string(p[1]))
			}
		}
		putU64(ev.Seq)
	}
	for i := range e.queue {
		putEvent(&e.queue[i])
	}
	for i := range e.held {
		putEvent(&e.held[i])
	}

	for _, id := range e.order {
		ns := e.nodes[id]
		putStr(string(id))
		putBool(ns.crashed)
		putI64(ns.offset)
		names := make([]string, 0, len(ns.timers))
		for name := range ns.timers {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			putStr(name)
			putU64(ns.timers[name])
		}
		data, err := ns.node.Snapshot()
		if err != nil {
			// 快照失败不应让哈希崩溃；写入错误标记即可。
			putStr("snapshot-error")
			continue
		}
		h.Write(data)
		h.Write([]byte{0})
	}

	pairs := make([][2]NodeID, 0, len(e.blocked))
	for p := range e.blocked {
		pairs = append(pairs, p)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
	for _, p := range pairs {
		putStr(string(p[0]))
		putStr(string(p[1]))
	}
	return h.Sum64()
}
