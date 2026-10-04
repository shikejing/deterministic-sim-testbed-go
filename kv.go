package sim

import (
	"encoding/json"
	"fmt"
	"sort"
)

// 本文件提供一个内置的确定性工作负载：单客户端 + 单服务器的
// 幂等键值写入。它既是使用示例，也是测试不变式、快照与重放的载体。

// KVRequest 是客户端发给服务器的写请求。OpID 全局唯一，服务器幂等应用。
type KVRequest struct {
	OpID  uint64
	Key   string
	Value string
}

// KVResponse 是服务器对一条写请求的确认。
type KVResponse struct {
	OpID uint64
}

// KVServer 是幂等应用写请求的服务器节点。
// Store 与 Applied 视为持久状态（崩溃重启后保留）。
type KVServer struct {
	Store        map[string]string
	Applied      map[uint64]bool
	AppliedCount int // 实际生效的写次数；幂等去重后每个 OpID 只计一次

	ctx *Context
}

func (s *KVServer) AttachContext(ctx *Context) { s.ctx = ctx }

func (s *KVServer) Init(ctx *Context) {
	s.ctx = ctx
	if s.Store == nil {
		s.Store = make(map[string]string)
	}
	if s.Applied == nil {
		s.Applied = make(map[uint64]bool)
	}
}

func (s *KVServer) OnMessage(msg Message) {
	var req KVRequest
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		return
	}
	if !s.Applied[req.OpID] {
		s.Applied[req.OpID] = true
		s.Store[req.Key] = req.Value
		s.AppliedCount++
	}
	resp, _ := json.Marshal(KVResponse{OpID: req.OpID})
	s.ctx.Send(msg.From, resp)
}

func (s *KVServer) OnTimer(name string) {}

func (s *KVServer) OnCrash() {}

type kvServerDisk struct {
	Store        map[string]string
	Applied      []uint64
	AppliedCount int
}

func (s *KVServer) Snapshot() ([]byte, error) {
	applied := make([]uint64, 0, len(s.Applied))
	for id := range s.Applied {
		applied = append(applied, id)
	}
	sort.Slice(applied, func(i, j int) bool { return applied[i] < applied[j] })
	return json.Marshal(kvServerDisk{Store: s.Store, Applied: applied, AppliedCount: s.AppliedCount})
}

func (s *KVServer) Restore(data []byte) error {
	var d kvServerDisk
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	s.Store = d.Store
	s.Applied = make(map[uint64]bool, len(d.Applied))
	for _, id := range d.Applied {
		s.Applied[id] = true
	}
	s.AppliedCount = d.AppliedCount
	return nil
}

// KVClient 是持续向服务器写入并等待确认的客户端节点。
// 未确认的写会按固定节奏重发，因此能容忍消息丢失与乱序。
type KVClient struct {
	Server   NodeID
	Total    int   // 计划发起的写总数
	Interval int64 // 重发周期（虚拟纳秒）

	Acked map[uint64]bool // 已确认集合（持久）

	ctx *Context
}

func (c *KVClient) AttachContext(ctx *Context) { c.ctx = ctx }

func (c *KVClient) Init(ctx *Context) {
	c.ctx = ctx
	if c.Acked == nil {
		c.Acked = make(map[uint64]bool)
	}
	ctx.SetTimer("tick", c.Interval)
}

func (c *KVClient) OnMessage(msg Message) {
	var resp KVResponse
	if err := json.Unmarshal(msg.Payload, &resp); err != nil {
		return
	}
	c.Acked[resp.OpID] = true
}

func (c *KVClient) OnTimer(name string) {
	if name != "tick" {
		return
	}
	for i := 0; i < c.Total; i++ {
		id := uint64(i)
		if c.Acked[id] {
			continue
		}
		req, _ := json.Marshal(KVRequest{
			OpID:  id,
			Key:   fmt.Sprintf("key-%d", id),
			Value: fmt.Sprintf("value-%d", id),
		})
		c.ctx.Send(c.Server, req)
	}
	if len(c.Acked) < c.Total {
		c.ctx.SetTimer("tick", c.Interval)
	}
}

func (c *KVClient) OnCrash() {}

type kvClientDisk struct {
	Acked []uint64
}

func (c *KVClient) Snapshot() ([]byte, error) {
	acked := make([]uint64, 0, len(c.Acked))
	for id := range c.Acked {
		acked = append(acked, id)
	}
	sort.Slice(acked, func(i, j int) bool { return acked[i] < acked[j] })
	return json.Marshal(kvClientDisk{Acked: acked})
}

func (c *KVClient) Restore(data []byte) error {
	var d kvClientDisk
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	c.Acked = make(map[uint64]bool, len(d.Acked))
	for _, id := range d.Acked {
		c.Acked[id] = true
	}
	return nil
}

// KVNodeFactory 返回能重建 KVClient/KVServer 的 NodeFactory。
func KVNodeFactory(serverID NodeID, total int, interval int64) NodeFactory {
	return func(id NodeID) Node {
		if id == serverID {
			return &KVServer{}
		}
		return &KVClient{Server: serverID, Total: total, Interval: interval}
	}
}
