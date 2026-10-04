package sim

import "container/heap"

// EventKind 标记事件类型。
type EventKind int

const (
	EventMessage EventKind = iota // 消息投递
	EventTimer                    // 定时器触发
	EventFault                    // 故障注入动作
)

func (k EventKind) String() string {
	switch k {
	case EventMessage:
		return "message"
	case EventTimer:
		return "timer"
	case EventFault:
		return "fault"
	}
	return "unknown"
}

// Event 是仿真中的最小调度单元。
type Event struct {
	ID        uint64 // 全局单调递增，可定位
	Time      int64  // 计划执行的虚拟时间（纳秒）
	Kind      EventKind
	From      NodeID // 消息来源（消息事件有效）
	To        NodeID // 目标节点
	Payload   []byte // 消息内容
	TimerName string // 定时器名字（定时器事件有效）
	Fault     *FaultAction
	Seq       uint64 // 入队序号，保证同一时间的事件顺序确定
}

// eventHeap 是按 (Time, seq) 排序的最小堆，保证弹出顺序完全确定。
type eventHeap []Event

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	if h[i].Time != h[j].Time {
		return h[i].Time < h[j].Time
	}
	return h[i].Seq < h[j].Seq
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) { *h = append(*h, x.(Event)) }

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	ev := old[n-1]
	old[n-1] = Event{} // 避免内存泄漏
	*h = old[:n-1]
	return ev
}

// 下面两个函数包装 container/heap，避免在引擎里散落接口断言。
func heapPush(h *eventHeap, ev Event) { heap.Push(h, ev) }
func heapPop(h *eventHeap) Event      { return heap.Pop(h).(Event) }
