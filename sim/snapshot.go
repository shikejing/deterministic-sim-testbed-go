package sim

import (
	"bytes"
	"container/heap"
	"encoding/gob"
	"errors"
	"fmt"
)

// snapshotVersion 是快照格式版本；结构变更时必须递增，
// 旧版本数据将被明确拒绝而不是悄悄读出残缺状态。
const snapshotVersion = 1

// ErrSnapshotVersion 表示快照由不兼容的版本产生。
var ErrSnapshotVersion = errors.New("sim: snapshot version mismatch")

func init() {
	// 框架自身的载荷类型；应用层载荷类型需用 RegisterTypes 注册。
	gob.Register(Time(0))
}

// RegisterTypes 注册可出现在 Event.Payload / Message.Body 中的应用层类型，
// 使快照能对它们编码。每种自定义载荷类型都必须注册。
func RegisterTypes(values ...any) {
	for _, v := range values {
		gob.Register(v)
	}
}

// snapshot 是仿真器完整状态的可序列化形式。
type snapshot struct {
	Version     uint64
	Config      Config
	RNGState    uint64
	Now         Time
	Seq         uint64
	Step        uint64
	ClockOffset Time
	Queue       []*Event
	Nodes       []nodeSnapshot
	Blocked     [][2]NodeID
}

type nodeSnapshot struct {
	ID      NodeID
	Kind    string
	Crashed bool
	State   []byte
}

// Snapshot 将当前仿真状态序列化为字节串。
// 序列化的是逻辑状态（不含日志与已产生的对外效应），
// 从快照继续推进与从未中断的运行逐条一致。
func (s *Simulator) Snapshot() ([]byte, error) {
	snap := snapshot{
		Version:     snapshotVersion,
		Config:      s.cfg,
		RNGState:    s.rng.State(),
		Now:         s.now,
		Seq:         s.seq,
		Step:        s.step,
		ClockOffset: s.clockOffset,
		Queue:       make([]*Event, len(s.queue)),
		Nodes:       make([]nodeSnapshot, 0, len(s.order)),
	}
	copy(snap.Queue, s.queue)
	for _, id := range s.order {
		ns := s.nodes[id]
		data, err := ns.node.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("sim: snapshot node %s: %w", id, err)
		}
		snap.Nodes = append(snap.Nodes, nodeSnapshot{
			ID: id, Kind: ns.kind, Crashed: ns.crashed, State: data,
		})
	}
	for link := range s.net.blocked {
		snap.Blocked = append(snap.Blocked, link)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snap); err != nil {
		return nil, fmt.Errorf("sim: encode snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// Restore 从快照字节串恢复一个可继续推进的仿真器。
// factories 按节点 kind 提供重建 Node 实例的工厂；
// 缺少工厂或版本不兼容都会返回明确错误。
func Restore(data []byte, factories map[string]func() Node) (*Simulator, error) {
	var snap snapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		return nil, fmt.Errorf("sim: decode snapshot: %w", err)
	}
	if snap.Version != snapshotVersion {
		return nil, fmt.Errorf("%w: snapshot has version %d, this build expects %d",
			ErrSnapshotVersion, snap.Version, snapshotVersion)
	}
	s := New(snap.Config)
	s.rng.SetState(snap.RNGState)
	s.now = snap.Now
	s.seq = snap.Seq
	s.step = snap.Step
	s.clockOffset = snap.ClockOffset
	for _, ev := range snap.Queue {
		s.queue = append(s.queue, ev)
	}
	heap.Init(&s.queue)
	for _, ns := range snap.Nodes {
		factory, ok := factories[ns.Kind]
		if !ok {
			return nil, fmt.Errorf("sim: no factory registered for node kind %q (node %s)", ns.Kind, ns.ID)
		}
		n := factory()
		if err := n.Restore(ns.State); err != nil {
			return nil, fmt.Errorf("sim: restore node %s: %w", ns.ID, err)
		}
		s.nodes[n.ID()] = &nodeState{kind: ns.Kind, node: n, crashed: ns.Crashed}
		s.order = append(s.order, n.ID())
	}
	blocked := make(map[[2]NodeID]bool, len(snap.Blocked))
	for _, link := range snap.Blocked {
		blocked[link] = true
	}
	s.net.restore(blocked)
	return s, nil
}
