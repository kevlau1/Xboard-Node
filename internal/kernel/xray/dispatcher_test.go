package xray

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport"
)

func newTestDispatcher() *LimitDispatcher {
	return &LimitDispatcher{
		limitedIPs: make(map[string]map[string]int),
	}
}

type nopReader struct{}

func (nopReader) ReadMultiBuffer() (buf.MultiBuffer, error) { return nil, nil }

func TestLimitDispatcher_DeviceLimitCheck(t *testing.T) {
	ld := newTestDispatcher()

	users := []model.UserSpec{
		{ID: 1, UUID: "uuid-1", DeviceLimit: 2, SpeedLimit: 0},
		{ID: 2, UUID: "uuid-2", DeviceLimit: 0, SpeedLimit: 10},
	}

	emailToUID := make(map[string]int)
	deviceLimits := make(map[string]int)
	speedLimits := make(map[string]int)
	for _, u := range users {
		email := userEmail(u.ID)
		emailToUID[email] = u.ID
		if u.DeviceLimit > 0 {
			deviceLimits[email] = u.DeviceLimit
		}
		if u.SpeedLimit > 0 {
			speedLimits[email] = u.SpeedLimit
		}
	}
	ld.UpdateLimits(emailToUID, deviceLimits, speedLimits)

	email1 := userEmail(1)

	// First IP should be allowed
	if ld.checkDeviceLimit(email1, "1.1.1.1", true) {
		t.Error("first IP should be allowed")
	}

	// Second IP should be allowed (limit=2)
	if ld.checkDeviceLimit(email1, "2.2.2.2", true) {
		t.Error("second IP should be allowed")
	}

	// Third unique IP should be rejected
	if !ld.checkDeviceLimit(email1, "3.3.3.3", true) {
		t.Error("third IP should be rejected (limit=2)")
	}

	// Same IP as first should be allowed (already connected)
	if ld.checkDeviceLimit(email1, "1.1.1.1", true) {
		t.Error("same IP should always be allowed")
	}

	// User 2 has no device limit — should always be allowed
	email2 := userEmail(2)
	for i := 0; i < 10; i++ {
		ip := "10.0.0." + string(rune('0'+i))
		if ld.checkDeviceLimit(email2, ip, true) {
			t.Errorf("user with no device limit should always be allowed (ip=%s)", ip)
		}
	}
}

func TestLimitDispatcher_DelConn(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	deviceLimits := map[string]int{email: 2}
	ld.UpdateLimits(map[string]int{email: 1}, deviceLimits, nil)

	// Add 2 IPs
	ld.checkDeviceLimit(email, "1.1.1.1", true)
	ld.checkDeviceLimit(email, "2.2.2.2", true)

	// Third should be rejected
	if !ld.checkDeviceLimit(email, "3.3.3.3", true) {
		t.Error("third IP should be rejected")
	}

	// Remove first IP
	ld.delConn(email, "1.1.1.1")

	// Now third IP should be allowed
	if ld.checkDeviceLimit(email, "3.3.3.3", true) {
		t.Error("after deleting one IP, new IP should be allowed")
	}
}

func TestLimitDispatcher_GetConnectionState(t *testing.T) {
	ld := newTestDispatcher()

	email1 := userEmail(1)
	email2 := userEmail(2)
	ld.UpdateLimits(map[string]int{email1: 1, email2: 2}, nil, nil)

	ic1 := &ipCounter{}
	r1 := &atomic.Int64{}
	r1.Store(1)
	ic1.ips.Store("1.1.1.1", r1)
	r2 := &atomic.Int64{}
	r2.Store(1)
	ic1.ips.Store("2.2.2.2", r2)
	ld.unlimitedIPs.Store(email1, ic1)

	ic2 := &ipCounter{}
	r3 := &atomic.Int64{}
	r3.Store(1)
	ic2.ips.Store("3.3.3.3", r3)
	ld.unlimitedIPs.Store(email2, ic2)

	ld.connCount.Store(5)

	aliveIPs, connCount := ld.GetConnectionState()

	if connCount != 5 {
		t.Errorf("expected connCount=5, got %d", connCount)
	}
	if len(aliveIPs[1]) != 2 {
		t.Errorf("user 1 IPs: got %d, want 2", len(aliveIPs[1]))
	}
	if len(aliveIPs[2]) != 1 {
		t.Errorf("user 2 IPs: got %d, want 1", len(aliveIPs[2]))
	}
}

func TestLimitDispatcher_ResetConns(t *testing.T) {
	ld := newTestDispatcher()

	ld.mu.Lock()
	ld.limitedIPs["user@1"] = map[string]int{"1.1.1.1": 1}
	ld.mu.Unlock()
	ld.connCount.Store(3)

	ld.ResetConns()

	ld.mu.RLock()
	ipCount := len(ld.limitedIPs)
	ld.mu.RUnlock()
	if ipCount != 0 {
		t.Error("limitedIPs should be empty after reset")
	}

	if ld.connCount.Load() != 0 {
		t.Error("connCount should be 0 after reset")
	}
}

func TestLimitDispatcher_UnlimitedUserFastPath(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	// No device limit set for this user
	ld.UpdateLimits(map[string]int{email: 1}, nil, nil)

	// Should use fast path (sync.Map), no lock needed
	for i := 0; i < 100; i++ {
		ip := "10.0.0." + string(rune('0'+i%10))
		if ld.checkDeviceLimit(email, ip, true) {
			t.Errorf("unlimited user should always be allowed (ip=%s)", ip)
		}
	}

	// Verify IPs are tracked in unlimitedIPs
	v, ok := ld.unlimitedIPs.Load(email)
	if !ok {
		t.Error("unlimited user should have entry in unlimitedIPs")
	}
	ic := v.(*ipCounter)
	ips := ic.aliveIPs()
	if len(ips) == 0 {
		t.Error("should have tracked some IPs")
	}
}


func TestLimitDispatcher_TrackLinkPreservesReader(t *testing.T) {
	ld := newTestDispatcher()
	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 1}, nil)

	origReader := nopReader{}
	origWriter := &closeTrackingWriter{Writer: buf.Discard, onClose: func() {}}
	link := &transport.Link{Reader: origReader, Writer: origWriter}

	ld.trackLink(link, email, "1.1.1.1", true)

	if link.Reader != origReader {
		t.Fatal("trackLink must not replace link.Reader")
	}
	if link.Writer == origWriter {
		t.Fatal("trackLink should wrap link.Writer for lifecycle callbacks")
	}
}

func TestLimitDispatcher_FilterDomains(t *testing.T) {
	ld := newTestDispatcher()
	ld.SetFilterDomains([]string{
		"cp.cloudflare.com",
		"gstatic.com",
		"Speed.Cloudflare.Com", // case insensitive
	})

	tests := []struct {
		domain   string
		filtered bool
	}{
		{"cp.cloudflare.com", true},
		{"CP.CLOUDFLARE.COM", true},                // case insensitive
		{"www.gstatic.com", true},                   // suffix match
		{"connectivitycheck.gstatic.com", true},     // suffix match
		{"gstatic.com", true},                       // exact match
		{"speed.cloudflare.com", true},              // case insensitive
		{"www.youtube.com", false},                  // not filtered
		{"cloudflare.com", false},                   // not a suffix of "cp.cloudflare.com"
		{"fakegstatic.com", false},                  // not a suffix match (no dot boundary)
		{"evil.cp.cloudflare.com", true},            // suffix match
	}

	for _, tc := range tests {
		dest := net.Destination{
			Address: net.DomainAddress(tc.domain),
			Port:    443,
			Network: net.Network_TCP,
		}
		got := ld.isFilteredDomain(dest)
		if got != tc.filtered {
			t.Errorf("isFilteredDomain(%q) = %v, want %v", tc.domain, got, tc.filtered)
		}
	}
}

func TestLimitDispatcher_FilterDomainIPNotFiltered(t *testing.T) {
	ld := newTestDispatcher()
	ld.SetFilterDomains([]string{"cp.cloudflare.com"})

	dest := net.Destination{
		Address: net.IPAddress([]byte{1, 1, 1, 1}),
		Port:    443,
		Network: net.Network_TCP,
	}
	if ld.isFilteredDomain(dest) {
		t.Error("IP destinations should never be filtered")
	}
}

func TestLimitDispatcher_FilterDomainEmptyList(t *testing.T) {
	ld := newTestDispatcher()

	dest := net.Destination{
		Address: net.DomainAddress("cp.cloudflare.com"),
		Port:    443,
		Network: net.Network_TCP,
	}
	if ld.isFilteredDomain(dest) {
		t.Error("empty filter list should not filter anything")
	}
}

// ─── Global device limit tests ──────────────────────────────────────────────

func TestGlobalDevices_MergedLimitCheck(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 3}, nil)

	// Local: 1.1.1.1
	if ld.checkDeviceLimit(email, "1.1.1.1", true) {
		t.Fatal("first local IP should be allowed")
	}

	// Global state: user 1 has 2.2.2.2 and 3.3.3.3 from other nodes
	ld.UpdateGlobalDevices(map[int][]string{
		1: {"2.2.2.2", "3.3.3.3"},
	})

	// local(1) + global(2) = 3 = limit, new IP 4.4.4.4 should be rejected
	if !ld.checkDeviceLimit(email, "4.4.4.4", true) {
		t.Error("4th IP should be rejected (local=1 + global=2 = 3 = limit)")
	}

	// IP already local should still be allowed
	if ld.checkDeviceLimit(email, "1.1.1.1", true) {
		t.Error("already-local IP should always be allowed")
	}
}

func TestGlobalDevices_GlobalIPAllowed(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 2}, nil)

	// Local: 1.1.1.1
	if ld.checkDeviceLimit(email, "1.1.1.1", true) {
		t.Fatal("first local IP should be allowed")
	}

	// Global says this user also has 2.2.2.2 on another node
	ld.UpdateGlobalDevices(map[int][]string{
		1: {"1.1.1.1", "2.2.2.2"},
	})

	// 2.2.2.2 is already globally known → should be allowed even though
	// local count would be at limit
	if ld.checkDeviceLimit(email, "2.2.2.2", true) {
		t.Error("globally known IP should be allowed")
	}
}

func TestGlobalDevices_StaleDataFallsBackToLocal(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 2}, nil)

	// Set global state and then make it stale
	ld.UpdateGlobalDevices(map[int][]string{
		1: {"9.9.9.9"},
	})
	ld.globalMu.Lock()
	ld.globalLastUpdate = time.Now().Add(-120 * time.Second) // 2 minutes ago → stale
	ld.globalMu.Unlock()

	// Local: 1.1.1.1
	if ld.checkDeviceLimit(email, "1.1.1.1", true) {
		t.Fatal("first local IP should be allowed")
	}

	// With stale global, only local count matters; limit=2, local=1 → allow
	if ld.checkDeviceLimit(email, "2.2.2.2", true) {
		t.Error("with stale global data, should fallback to local-only; local=1 < limit=2")
	}

	// local=2 = limit → reject
	if !ld.checkDeviceLimit(email, "3.3.3.3", true) {
		t.Error("local=2 = limit, third IP should be rejected")
	}
}

func TestGlobalDevices_ClearResetsState(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 2}, nil)

	ld.UpdateGlobalDevices(map[int][]string{
		1: {"9.9.9.9"},
	})
	ld.ClearGlobalDevices()

	// After clear, global state should be gone → local only
	if ld.checkDeviceLimit(email, "1.1.1.1", true) {
		t.Fatal("first local IP should be allowed")
	}
	if ld.checkDeviceLimit(email, "2.2.2.2", true) {
		t.Error("local=1 < limit=2, should be allowed after global clear")
	}
}

func TestGlobalDevices_LexicographicSelection(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 2}, nil)

	// Global has 5.5.5.5 from another node
	ld.UpdateGlobalDevices(map[int][]string{
		1: {"5.5.5.5"},
	})

	// Local: 3.3.3.3
	if ld.checkDeviceLimit(email, "3.3.3.3", true) {
		t.Fatal("first local IP should be allowed")
	}

	// merged = {3.3.3.3, 5.5.5.5} = 2 = limit
	// New IP 1.1.1.1 → merged+new = {1.1.1.1, 3.3.3.3, 5.5.5.5}
	// Lex sort → pick first 2: {1.1.1.1, 3.3.3.3}
	// 1.1.1.1 is in allowed set → should be allowed
	if ld.checkDeviceLimit(email, "1.1.1.1", true) {
		t.Error("1.1.1.1 is lexicographically first, should be allowed")
	}

	// New IP 9.9.9.9 → merged+new = {1.1.1.1, 3.3.3.3, 5.5.5.5, 9.9.9.9}
	// Lex sort → pick first 2: {1.1.1.1, 3.3.3.3}
	// 9.9.9.9 is NOT in allowed set → should be rejected
	if !ld.checkDeviceLimit(email, "9.9.9.9", true) {
		t.Error("9.9.9.9 is lexicographically last, should be rejected")
	}
}

func TestGlobalDevices_NoGlobalDataForUser(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 2}, nil)

	// Global state exists but NOT for user 1
	ld.UpdateGlobalDevices(map[int][]string{
		999: {"8.8.8.8"},
	})

	// Should fallback to local-only
	if ld.checkDeviceLimit(email, "1.1.1.1", true) {
		t.Fatal("should be allowed (no global data for this user)")
	}
	if ld.checkDeviceLimit(email, "2.2.2.2", true) {
		t.Error("should be allowed (local=1 < limit=2, no global data)")
	}
	if !ld.checkDeviceLimit(email, "3.3.3.3", true) {
		t.Error("should be rejected (local=2 = limit)")
	}
}

func TestGlobalDevices_UDPDoesNotChangeRefcount(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 2}, nil)

	ld.UpdateGlobalDevices(map[int][]string{
		1: {"5.5.5.5"},
	})

	// UDP check should not add to local state
	if ld.checkDeviceLimit(email, "1.1.1.1", false) {
		t.Fatal("UDP should be allowed")
	}

	// Verify no local IP was added
	ld.mu.RLock()
	localIPs := ld.limitedIPs[email]
	ld.mu.RUnlock()
	if localIPs != nil && len(localIPs) > 0 {
		t.Error("UDP should not add IPs to local state")
	}
}

func TestLimitDispatcher_CloseTrackingWriterReleasesConn(t *testing.T) {
	ld := newTestDispatcher()
	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 1}, nil)
	if ld.checkDeviceLimit(email, "1.1.1.1", true) {
		t.Fatal("first connection should be allowed")
	}

	link := &transport.Link{Reader: nopReader{}, Writer: buf.Discard}
	ld.trackLink(link, email, "1.1.1.1", true)

	if got := ld.connCount.Load(); got != 1 {
		t.Fatalf("expected connCount=1 after tracking, got %d", got)
	}
	cw, ok := link.Writer.(*closeTrackingWriter)
	if !ok {
		t.Fatal("expected closeTrackingWriter wrapper")
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("closeTrackingWriter.Close() error = %v", err)
	}
	if got := ld.connCount.Load(); got != 0 {
		t.Fatalf("expected connCount=0 after close, got %d", got)
	}
	if ld.checkDeviceLimit(email, "2.2.2.2", true) {
		t.Fatal("device slot should be released after writer close")
	}
}
