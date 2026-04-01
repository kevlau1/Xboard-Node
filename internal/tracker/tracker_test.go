package tracker

import (
	"testing"
)

func TestProcess_InitialTraffic(t *testing.T) {
	tr := New()
	cumTraffic := map[int][2]int64{
		1: {100, 200},
		2: {50, 80},
	}
	aliveIPs := map[int]map[string]bool{
		1: {"1.1.1.1": true},
		2: {"2.2.2.2": true},
	}
	tr.Process(cumTraffic, aliveIPs, 2)

	flushed := tr.FlushTraffic()
	if flushed[1] != [2]int64{100, 200} {
		t.Errorf("user 1 traffic: got %v", flushed[1])
	}
	if flushed[2] != [2]int64{50, 80} {
		t.Errorf("user 2 traffic: got %v", flushed[2])
	}
}

func TestProcess_DeltaCalculation(t *testing.T) {
	tr := New()

	// First tick
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)

	// Second tick — counters advanced
	tr.Process(map[int][2]int64{1: {300, 500}}, nil, 1)

	flushed := tr.FlushTraffic()
	// Total: first tick 100/200 + delta 200/300 = 300/500
	if flushed[1] != [2]int64{300, 500} {
		t.Errorf("accumulated: got %v, want [300,500]", flushed[1])
	}
}

func TestProcess_CounterReset(t *testing.T) {
	tr := New()

	// First tick with high counters
	tr.Process(map[int][2]int64{1: {1000, 2000}}, nil, 1)
	tr.FlushTraffic() // clear accumulator

	// Counter reset (new value < old value) — treat as fresh
	tr.Process(map[int][2]int64{1: {50, 80}}, nil, 1)

	flushed := tr.FlushTraffic()
	if flushed[1] != [2]int64{50, 80} {
		t.Errorf("counter reset: got %v, want [50,80]", flushed[1])
	}
}

func TestProcess_UserDisappears(t *testing.T) {
	tr := New()

	// Tick 1: user present
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)

	// Tick 2: user gone
	tr.Process(map[int][2]int64{}, nil, 0)

	// Tick 3: user reappears — should count full bytes (treated as new)
	tr.Process(map[int][2]int64{1: {50, 30}}, nil, 1)

	flushed := tr.FlushTraffic()
	// First tick: 100/200, tick 2: 0, tick 3: 50/30 = total 150/230
	if flushed[1] != [2]int64{150, 230} {
		t.Errorf("after disappear: got %v, want [150,230]", flushed[1])
	}
}

func TestProcess_ZeroTraffic(t *testing.T) {
	tr := New()
	tr.Process(map[int][2]int64{}, nil, 0)

	if tr.HasTraffic() {
		t.Error("expected no traffic for empty input")
	}
}

func TestFlushTraffic(t *testing.T) {
	tr := New()
	tr.Process(map[int][2]int64{
		1: {100, 200},
		2: {50, 80},
	}, nil, 2)

	flushed := tr.FlushTraffic()
	if flushed[1] != [2]int64{100, 200} {
		t.Errorf("flushed user 1: got %v", flushed[1])
	}
	if flushed[2] != [2]int64{50, 80} {
		t.Errorf("flushed user 2: got %v", flushed[2])
	}

	// After flush, should be empty
	if tr.HasTraffic() {
		t.Error("expected no traffic after flush")
	}
	flushed2 := tr.FlushTraffic()
	if len(flushed2) != 0 {
		t.Errorf("expected empty after second flush, got %v", flushed2)
	}
}

func TestRestoreTraffic(t *testing.T) {
	tr := New()
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)

	flushed := tr.FlushTraffic()
	tr.RestoreTraffic(flushed)

	if !tr.HasTraffic() {
		t.Error("expected traffic after restore")
	}

	restored := tr.FlushTraffic()
	if restored[1] != [2]int64{100, 200} {
		t.Errorf("restored: got %v", restored[1])
	}
}

func TestRestoreTraffic_Additive(t *testing.T) {
	tr := New()

	// Two ticks accumulate
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	tr.Process(map[int][2]int64{1: {200, 400}}, nil, 1)

	// Flush and restore
	first := tr.FlushTraffic()
	tr.RestoreTraffic(first)

	// Generate additional traffic
	tr.Process(map[int][2]int64{1: {250, 500}}, nil, 1)

	final := tr.FlushTraffic()
	// Restored: 200/400 (initial 100/200 + delta 100/200) + new delta 50/100 = 250/500
	if final[1][0] != 250 || final[1][1] != 500 {
		t.Errorf("additive restore: got %v", final[1])
	}
}

func TestFlushAliveIPs(t *testing.T) {
	tr := New()
	aliveIPs := map[int]map[string]bool{
		1: {"1.1.1.1": true, "2.2.2.2": true},
		2: {"3.3.3.3": true},
	}
	tr.Process(map[int][2]int64{1: {100, 200}, 2: {30, 40}}, aliveIPs, 3)

	flushed := tr.FlushAliveIPs()
	if len(flushed[1]) != 2 {
		t.Errorf("user 1 IPs: got %d, want 2", len(flushed[1]))
	}
	if len(flushed[2]) != 1 {
		t.Errorf("user 2 IPs: got %d, want 1", len(flushed[2]))
	}

	// Without another Process call, hash is same → returns nil (skip duplicate)
	flushed2 := tr.FlushAliveIPs()
	if flushed2 != nil {
		t.Errorf("expected nil on duplicate flush, got %v", flushed2)
	}
}

func TestFlushAliveIPs_GoingOffline(t *testing.T) {
	tr := New()

	// Tick 1: user 1 and user 2 are online
	tr.Process(map[int][2]int64{1: {100, 200}, 2: {30, 40}}, map[int]map[string]bool{
		1: {"1.1.1.1": true},
		2: {"2.2.2.2": true},
	}, 2)
	flushed := tr.FlushAliveIPs()
	if flushed == nil {
		t.Fatal("expected non-nil on first flush")
	}
	if len(flushed[1]) != 1 || len(flushed[2]) != 1 {
		t.Errorf("tick1: user1=%d IPs, user2=%d IPs, want 1,1", len(flushed[1]), len(flushed[2]))
	}

	// Tick 2: user 1 goes offline, user 2 stays
	tr.Process(map[int][2]int64{2: {60, 80}}, map[int]map[string]bool{
		2: {"2.2.2.2": true},
	}, 1)
	flushed = tr.FlushAliveIPs()
	if flushed == nil {
		t.Fatal("expected non-nil when user went offline")
	}
	// User 1 should have empty IP list (going-offline signal)
	if ips, ok := flushed[1]; !ok {
		t.Error("user 1 missing from flush (should be present with empty IPs)")
	} else if len(ips) != 0 {
		t.Errorf("user 1 IPs: got %d, want 0 (going-offline)", len(ips))
	}
	// User 2 should still have their IP
	if len(flushed[2]) != 1 {
		t.Errorf("user 2 IPs: got %d, want 1", len(flushed[2]))
	}

	// Tick 3: same state (user 2 online) → should return nil (no change)
	tr.Process(map[int][2]int64{2: {90, 120}}, map[int]map[string]bool{
		2: {"2.2.2.2": true},
	}, 1)
	flushed = tr.FlushAliveIPs()
	if flushed != nil {
		t.Errorf("expected nil (no change), got %v", flushed)
	}

	// Tick 4: user 2 also goes offline → should include user 2 with empty IPs
	tr.Process(map[int][2]int64{}, map[int]map[string]bool{}, 0)
	flushed = tr.FlushAliveIPs()
	if flushed == nil {
		t.Fatal("expected non-nil when user 2 went offline")
	}
	if ips, ok := flushed[2]; !ok {
		t.Error("user 2 missing from flush (should be present with empty IPs)")
	} else if len(ips) != 0 {
		t.Errorf("user 2 IPs: got %d, want 0 (going-offline)", len(ips))
	}
	// User 1 should NOT reappear (already reported as offline in tick 2)
	if _, ok := flushed[1]; ok {
		t.Error("user 1 should not reappear (was already reported offline)")
	}

	// Tick 5: still all offline → nil (no change)
	tr.Process(map[int][2]int64{}, map[int]map[string]bool{}, 0)
	flushed = tr.FlushAliveIPs()
	if flushed != nil {
		t.Errorf("expected nil (all offline, no change), got %v", flushed)
	}
}

func TestFlushAliveIPs_RestoreAfterFailure(t *testing.T) {
	tr := New()

	// User comes online
	tr.Process(map[int][2]int64{1: {100, 200}}, map[int]map[string]bool{
		1: {"1.1.1.1": true},
	}, 1)
	flushed := tr.FlushAliveIPs()
	if flushed == nil || len(flushed[1]) != 1 {
		t.Fatal("expected user 1 online")
	}

	// User goes offline
	tr.Process(map[int][2]int64{}, map[int]map[string]bool{}, 0)
	flushed = tr.FlushAliveIPs()
	if flushed == nil {
		t.Fatal("expected going-offline data")
	}
	if len(flushed[1]) != 0 {
		t.Errorf("user 1 IPs: got %d, want 0", len(flushed[1]))
	}

	// Simulate push failure → restore
	tr.RestoreAliveIPs(flushed)

	// Next flush should re-emit the going-offline data
	tr.Process(map[int][2]int64{}, map[int]map[string]bool{}, 0)
	flushed = tr.FlushAliveIPs()
	if flushed == nil {
		t.Fatal("expected re-emit after restore, got nil")
	}
	if ips, ok := flushed[1]; !ok {
		t.Error("user 1 missing after restore")
	} else if len(ips) != 0 {
		t.Errorf("user 1 IPs after restore: got %d, want 0", len(ips))
	}
}

func TestFlushAliveIPs_DedupSameIP(t *testing.T) {
	tr := New()
	aliveIPs := map[int]map[string]bool{
		1: {"1.1.1.1": true},
	}
	tr.Process(map[int][2]int64{1: {100, 200}}, aliveIPs, 2)

	flushed := tr.FlushAliveIPs()
	if len(flushed[1]) != 1 {
		t.Errorf("expected 1 IP, got %d", len(flushed[1]))
	}
}

func TestHasTraffic(t *testing.T) {
	tr := New()
	if tr.HasTraffic() {
		t.Error("new tracker should not have traffic")
	}

	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	if !tr.HasTraffic() {
		t.Error("should have traffic after process")
	}

	tr.FlushTraffic()
	if tr.HasTraffic() {
		t.Error("should not have traffic after flush")
	}
}

func TestProcess_NoTrafficDelta(t *testing.T) {
	tr := New()

	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	tr.FlushTraffic() // clear

	// Same values — no delta
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	if tr.HasTraffic() {
		t.Error("expected no traffic for zero delta")
	}
}

func TestTrafficAccumulation(t *testing.T) {
	tr := New()

	// Tick 1
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	// Tick 2 (cumulative 300, 500 → delta 200, 300)
	tr.Process(map[int][2]int64{1: {300, 500}}, nil, 1)
	// Tick 3 (cumulative 350, 600 → delta 50, 100)
	tr.Process(map[int][2]int64{1: {350, 600}}, nil, 1)

	flushed := tr.FlushTraffic()
	// Total: 100+200+50 = 350 upload, 200+300+100 = 600 download
	if flushed[1] != [2]int64{350, 600} {
		t.Errorf("accumulated: got %v, want [350,600]", flushed[1])
	}
}
