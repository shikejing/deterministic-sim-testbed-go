package sim

import (
	"context"
	"fmt"
	"testing"
)

// TestManyEntities 验证大量实体下运行保持确定且可控。
func TestManyEntities(t *testing.T) {
	const clients = 200
	build := func() *Engine {
		cfg := testConfig(88)
		cfg.Faults.DropProb = 0.05
		cfg.Faults.ReorderProb = 0.1
		cfg.Faults.ReorderMaxDelay = 100
		e := NewEngine(cfg)
		e.AddNode(testServer, &KVServer{})
		for i := 0; i < clients; i++ {
			e.AddNode(NodeID(fmt.Sprintf("client-%03d", i)),
				&KVClient{Server: testServer, Total: 5, Interval: 100})
		}
		return e
	}
	run := func() Result {
		e := build()
		res, err := e.Run(context.Background())
		if err != nil {
			t.Fatalf("scale run: %v", err)
		}
		return res
	}
	a, b := run(), run()
	if a.StateHash != b.StateHash {
		t.Fatal("200-client runs diverged")
	}
	s := a.StateHash
	_ = s
	if a.Events == 0 {
		t.Fatal("no events processed")
	}
	t.Logf("clients=%d events=%d time=%d", clients, a.Events, a.Now)
}
