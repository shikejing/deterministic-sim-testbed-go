package sim

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
)

// 本文件提供一个演示工作负载：多数派确认的复制 KV。
// 它既用于展示测试台用法，也作为自动化测试的被测系统。
//
// 协议：
//   - 客户端把 KVOp 发给任一节点（KindClient 事件）；
//   - 节点本地生效（幂等）后向所有对等节点广播 replicate；
//   - 收到 replicate 的节点幂等生效并回 ack；
//   - 发起节点收齐 quorum 个不同来源的 ack 后确认该操作（Emit "confirm"）。

// KVOp 是一次客户端写操作，OpID 全局唯一用于幂等与去重。
type KVOp struct {
	OpID  string
	Key   string
	Value string
}

type replMsg struct{ Op KVOp }
type ackMsg struct {
	OpID string
	From NodeID
}

func init() {
	RegisterTypes(KVOp{}, replMsg{}, ackMsg{})
}

type kvState struct {
	KV        map[string]string          // 可见数据
	Applied   map[string]int             // opID -> 状态变更生效次数（应 <=1）
	AckFrom   map[string]map[NodeID]bool // opID -> 已收到的 ack 来源（发起节点）
	Confirmed map[string]KVOp            // 已确认操作（发起节点）
}

// KVNode 是复制 KV 节点。崩溃不丢状态（模拟持久存储）。
type KVNode struct {
	id     NodeID
	peers  []NodeID
	quorum int
	st     kvState
}

func NewKVNode(id NodeID, peers []NodeID, quorum int) *KVNode {
	return &KVNode{id: id, peers: peers, quorum: quorum, st: newKVState()}
}

func newKVState() kvState {
	return kvState{
		KV:        map[string]string{},
		Applied:   map[string]int{},
		AckFrom:   map[string]map[NodeID]bool{},
		Confirmed: map[string]KVOp{},
	}
}

func (n *KVNode) ID() NodeID { return n.id }

func (n *KVNode) Init(ctx *Context) {}

func (n *KVNode) OnEvent(ctx *Context, ev Event) {
	switch ev.Kind {
	case KindClient:
		op, ok := ev.Payload.(KVOp)
		if !ok {
			return
		}
		n.apply(op)
		for _, p := range n.peers {
			ctx.Send(p, "replicate", replMsg{Op: op})
		}
		n.maybeConfirm(ctx, op.OpID) // 单节点集群时本地即法定人数
	case KindDeliver:
		switch body := ev.Msg.Body.(type) {
		case replMsg:
			n.apply(body.Op)
			ctx.Send(ev.Src, "ack", ackMsg{OpID: body.Op.OpID, From: n.id})
		case ackMsg:
			if n.st.AckFrom[body.OpID] == nil {
				n.st.AckFrom[body.OpID] = map[NodeID]bool{}
			}
			n.st.AckFrom[body.OpID][body.From] = true
			n.maybeConfirm(ctx, body.OpID)
		}
	}
}

// apply 幂等地让操作生效：同一 opID 的状态变更至多一次。
func (n *KVNode) apply(op KVOp) {
	if n.st.Applied[op.OpID] > 0 {
		return
	}
	n.st.KV[op.Key] = op.Value
	n.st.Applied[op.OpID]++
}

func (n *KVNode) maybeConfirm(ctx *Context, opID string) {
	if _, ok := n.st.Confirmed[opID]; ok {
		return
	}
	// 自己也算一票。
	if 1+len(n.st.AckFrom[opID]) >= n.quorum {
		n.st.Confirmed[opID] = KVOp{OpID: opID}
		ctx.Emit("confirm", opID)
	}
}

// --- 只读访问器，供不变式与测试使用 ---

// Value 返回某 key 当前可见值。
func (n *KVNode) Value(key string) (string, bool) {
	v, ok := n.st.KV[key]
	return v, ok
}

// AppliedCount 返回某操作在本节点上状态变更生效的次数。
func (n *KVNode) AppliedCount(opID string) int { return n.st.Applied[opID] }

// HasOp 报告某操作是否已在本节点生效。
func (n *KVNode) HasOp(opID string) bool { return n.st.Applied[opID] > 0 }

// ConfirmedOps 返回本节点已确认的操作 ID 列表（确定性顺序由调用方排序）。
func (n *KVNode) ConfirmedOps() []string {
	out := make([]string, 0, len(n.st.Confirmed))
	for id := range n.st.Confirmed {
		out = append(out, id)
	}
	return out
}

// StateHash 实现廉价状态哈希。
// 对每条目独立哈希后以 XOR 组合：结果与 map 迭代顺序无关，
// 无需排序，长时间运行下每步开销保持 O(条目数)。
func (n *KVNode) StateHash() uint64 {
	entry := func(parts ...string) uint64 {
		h := fnv.New64a()
		for _, p := range parts {
			h.Write([]byte(p))
			h.Write([]byte{0})
		}
		return h.Sum64()
	}
	var acc uint64
	for k, v := range n.st.KV {
		acc ^= entry("kv", k, v)
	}
	for k, v := range n.st.Applied {
		acc ^= entry("applied", k, fmt.Sprint(v))
	}
	for k, v := range n.st.AckFrom {
		var inner uint64
		for src := range v {
			inner ^= entry("acksrc", string(src))
		}
		acc ^= entry("acks", k) + inner
	}
	for k := range n.st.Confirmed {
		acc ^= entry("confirmed", k)
	}
	return acc
}

// kvPersist 是 KVNode 的完整可序列化状态：配置 + 运行态。
type kvPersist struct {
	ID     NodeID
	Peers  []NodeID
	Quorum int
	St     kvState
}

func (n *KVNode) Snapshot() ([]byte, error) {
	var buf bytes.Buffer
	p := kvPersist{ID: n.id, Peers: n.peers, Quorum: n.quorum, St: n.st}
	if err := gob.NewEncoder(&buf).Encode(p); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (n *KVNode) Restore(data []byte) error {
	p := kvPersist{St: newKVState()}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&p); err != nil {
		return err
	}
	n.id, n.peers, n.quorum, n.st = p.ID, p.Peers, p.Quorum, p.St
	return nil
}

// --- 配套不变式 ---

func kvNode(s *Simulator, id NodeID) (*KVNode, error) {
	n := s.Node(id)
	if n == nil {
		return nil, fmt.Errorf("node %s not found", id)
	}
	kv, ok := n.(*KVNode)
	if !ok {
		return nil, fmt.Errorf("node %s is not a KVNode", id)
	}
	return kv, nil
}

// ConfirmedDurable 不变式：任何已确认的操作必须已在至少 quorum 个节点上生效。
// （已确认的结果不能丢失。）
func ConfirmedDurable(quorum int, ids ...NodeID) Invariant {
	return func(s *Simulator) error {
		confirmed := map[string]bool{}
		for _, id := range ids {
			kv, err := kvNode(s, id)
			if err != nil {
				return err
			}
			for _, op := range kv.ConfirmedOps() {
				confirmed[op] = true
			}
		}
		for op := range confirmed {
			count := 0
			for _, id := range ids {
				kv, _ := kvNode(s, id)
				if kv.HasOp(op) {
					count++
				}
			}
			if count < quorum {
				return fmt.Errorf("confirmed op %s present on only %d/%d nodes (quorum=%d)",
					op, count, len(ids), quorum)
			}
		}
		return nil
	}
}

// NoDuplicateApply 不变式：同一操作在任一节点上的状态变更至多生效一次。
func NoDuplicateApply(ids ...NodeID) Invariant {
	return func(s *Simulator) error {
		for _, id := range ids {
			kv, err := kvNode(s, id)
			if err != nil {
				return err
			}
			for op, cnt := range kv.st.Applied {
				if cnt > 1 {
					return fmt.Errorf("op %s applied %d times on node %s", op, cnt, id)
				}
			}
		}
		return nil
	}
}

// ValuesConsistent 不变式：任意时刻，所有节点对同一 key 的可见值不得冲突。
func ValuesConsistent(ids ...NodeID) Invariant {
	return func(s *Simulator) error {
		seen := map[string]string{}
		for _, id := range ids {
			kv, err := kvNode(s, id)
			if err != nil {
				return err
			}
			for k, v := range kv.st.KV {
				if prev, ok := seen[k]; ok && prev != v {
					return fmt.Errorf("key %s has conflicting values %q and %q", k, prev, v)
				}
				seen[k] = v
			}
		}
		return nil
	}
}
