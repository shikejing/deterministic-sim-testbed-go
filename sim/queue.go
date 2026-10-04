package sim

// eventHeap 是按 (Time, ID) 排序的最小堆：
// 同刻事件严格按创建顺序（ID 递增）执行，保证调度顺序确定。
type eventHeap []*Event

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	if h[i].Time != h[j].Time {
		return h[i].Time < h[j].Time
	}
	return h[i].ID < h[j].ID
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) {
	*h = append(*h, x.(*Event))
}

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	ev := old[n-1]
	old[n-1] = nil // 避免内存泄漏
	*h = old[:n-1]
	return ev
}
