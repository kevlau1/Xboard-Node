package xray

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe"

	xrayDispatcher "github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

// Access xray's internal config creator registry so we can replace the
// default dispatcher factory with ours. This runs AFTER xray's init()
// functions because our package imports xray (dependency order guarantee).
//
//go:linkname typeCreatorRegistry github.com/xtls/xray-core/common.typeCreatorRegistry
var typeCreatorRegistry map[reflect.Type]common.ConfigCreator

var origDispatcherFactory common.ConfigCreator

// globalLimitDispatcher is set when the factory creates a LimitDispatcher.
// The Xray kernel reads it to configure limits and get connections.
var globalLimitDispatcher atomic.Pointer[LimitDispatcher]

func init() {
	configType := reflect.TypeOf((*xrayDispatcher.Config)(nil))
	origDispatcherFactory = typeCreatorRegistry[configType]
	typeCreatorRegistry[configType] = limitDispatcherFactory
}

func limitDispatcherFactory(ctx context.Context, config interface{}) (interface{}, error) {
	orig, err := origDispatcherFactory(ctx, config)
	if err != nil {
		return nil, err
	}
	inner, ok := orig.(routing.Dispatcher)
	if !ok {
		return orig, nil
	}
	ld := &LimitDispatcher{
		inner:        orig,
		innerDisp:    inner,
		limitedIPs:   make(map[string]map[string]int),
		trackedConns: make(map[string]map[string]map[uint64]*closeTrackingWriter),
	}
	globalLimitDispatcher.Store(ld)
	nlog.Core().Debug("xray: limit dispatcher installed")
	return ld, nil
}

// LimitDispatcher wraps xray's DefaultDispatcher to enforce per-user
// admission checks before a request is dispatched into xray-core.
//
// It intentionally does NOT mutate transport.Link.Reader/Writer. Xray's
// mux/XUDP close path requires the original concrete *pipe.Reader to remain
// intact, so the dispatcher is limited to gate-keeping and safe connection
// lifecycle bookkeeping.
type LimitDispatcher struct {
	inner     interface{}        // original DefaultDispatcher (Feature + Dispatcher)
	innerDisp routing.Dispatcher // same object, typed as Dispatcher

	// limitedUsers: users with device limit > 0, protected by mu.
	// Needs deterministic IP ordering for kick decisions.
	mu           sync.RWMutex
	limitedIPs   map[string]map[string]int // email → sourceIP → refcount
	deviceLimits map[string]int            // email → max devices
	emailToUID   map[string]int            // email → panel user ID
	trackedConns map[string]map[string]map[uint64]*closeTrackingWriter
	connSeq      atomic.Uint64

	// unlimitedIPs: users without device limit — sync.Map for lock-free access.
	// Each entry is *ipCounter{ips sync.Map}.
	unlimitedIPs sync.Map // email → *ipCounter

	connCount atomic.Int64 // total active connections tracked by dispatcher

	// Multi-node global device state from panel (via sync.devices WS event).
	// Maps userID → set of source IPs seen across ALL nodes.
	globalDevices    map[int]map[string]bool
	globalMu         sync.RWMutex
	globalLastUpdate time.Time

	// filterDomains holds lowercased destination domains whose connections
	// should be excluded from device tracking (but still proxied normally).
	filterMu      sync.RWMutex
	filterDomains map[string]bool
}

// ipCounter tracks IPs for unlimited users without any lock.
type ipCounter struct {
	ips sync.Map // sourceIP → *atomic.Int64 (refcount)
}

// aliveIPs returns a snapshot of distinct IPs.
func (ic *ipCounter) aliveIPs() map[string]bool {
	result := make(map[string]bool)
	ic.ips.Range(func(key, _ interface{}) bool {
		if rv, ok := ic.ips.Load(key); ok && rv.(*atomic.Int64).Load() > 0 {
			result[key.(string)] = true
		}
		return true
	})
	return result
}

// ─── routing.Dispatcher ──────────────────────────────────────────────────────

func (d *LimitDispatcher) Dispatch(ctx context.Context, dest net.Destination) (*transport.Link, error) {
	email, sourceIP, isTCP, err := d.identifyAndCheck(ctx, dest)
	if err != nil {
		return nil, err
	}

	link, err := d.innerDisp.Dispatch(ctx, dest)
	if err != nil {
		if email != "" && isTCP {
			d.delConn(email, sourceIP)
		}
		return nil, err
	}

	if email != "" {
		d.trackLink(link, email, sourceIP, isTCP)
	}
	return link, nil
}

func (d *LimitDispatcher) DispatchLink(ctx context.Context, dest net.Destination, link *transport.Link) error {
	email, sourceIP, isTCP, err := d.identifyAndCheck(ctx, dest)
	if err != nil {
		return err
	}

	if email != "" {
		d.trackLink(link, email, sourceIP, isTCP)
	}
	err = d.innerDisp.DispatchLink(ctx, dest, link)
	if err != nil {
		if writer, ok := link.Writer.(*closeTrackingWriter); ok {
			writer.Interrupt()
		}
		return err
	}
	return nil
}

// identifyAndCheck extracts user identity from the session context, enforces
// device limits, and returns the user's email, source IP, and TCP flag.
// Returns a non-nil error only when the connection should be rejected.
func (d *LimitDispatcher) identifyAndCheck(ctx context.Context, dest net.Destination) (email, sourceIP string, isTCP bool, err error) {
	si := session.InboundFromContext(ctx)
	if si == nil || si.User == nil || len(si.User.Email) == 0 {
		return "", "", false, nil
	}

	if d.isFilteredDomain(dest) {
		return "", "", false, nil
	}

	email = si.User.Email
	sourceIP = si.Source.Address.IP().String()
	isTCP = dest.Network == net.Network_TCP

	if d.checkDeviceLimit(email, sourceIP, isTCP) {
		nlog.Core().Debug("xray: device limit exceeded", "email", email, "ip", sourceIP)
		return "", "", false, errors.New("device limit exceeded for " + email)
	}
	return email, sourceIP, isTCP, nil
}

// trackLink records connection lifecycle by wrapping Writer only. Reader stays
// untouched because mux/XUDP close paths require the original concrete reader.
func (d *LimitDispatcher) trackLink(link *transport.Link, email, sourceIP string, isTCP bool) {
	d.connCount.Add(1)
	connID := d.connSeq.Add(1)

	onClose := func() {
		if isTCP {
			d.unregisterTrackedConn(email, sourceIP, connID)
			d.delConn(email, sourceIP)
		}
		d.connCount.Add(-1)
	}

	writer := &closeTrackingWriter{
		Writer:  link.Writer,
		onClose: onClose,
	}
	link.Writer = writer

	if isTCP {
		d.registerTrackedConn(email, sourceIP, connID, writer)
	}
}

// ─── features.Feature (delegated) ───────────────────────────────────────────

func (d *LimitDispatcher) Type() interface{} { return routing.DispatcherType() }

func (d *LimitDispatcher) Start() error {
	if s, ok := d.inner.(interface{ Start() error }); ok {
		return s.Start()
	}
	return nil
}

func (d *LimitDispatcher) Close() error {
	if c, ok := d.inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// ─── Limit management (called by Xray kernel) ──────────────────────────────

func (d *LimitDispatcher) UpdateLimits(emailToUID map[string]int, deviceLimits, _ map[string]int) {
	d.mu.Lock()
	d.emailToUID = emailToUID
	d.deviceLimits = deviceLimits
	d.mu.Unlock()
	d.enforceDeviceLimits()
}

// UpdateGlobalDevices stores the aggregated device state pushed by the panel.
// The data contains IPs from ALL nodes for each user, enabling cross-node
// device limit enforcement.
func (d *LimitDispatcher) UpdateGlobalDevices(users map[int][]string) {
	d.globalMu.Lock()
	d.globalDevices = make(map[int]map[string]bool, len(users))
	for uid, ips := range users {
		m := make(map[string]bool, len(ips))
		for _, ip := range ips {
			m[ip] = true
		}
		d.globalDevices[uid] = m
	}
	d.globalLastUpdate = time.Now()
	d.globalMu.Unlock()
	nlog.Core().Debug("xray: global device state updated", "users", len(users))
	d.enforceDeviceLimits()
}

// ClearGlobalDevices resets global device state (called on WS disconnect).
func (d *LimitDispatcher) ClearGlobalDevices() {
	d.globalMu.Lock()
	d.globalDevices = make(map[int]map[string]bool)
	d.globalLastUpdate = time.Time{}
	d.globalMu.Unlock()
	nlog.Core().Debug("xray: global device state cleared")
}

// SetFilterDomains replaces the set of destination domains whose connections
// are excluded from device tracking. Domains are matched case-insensitively
// and support suffix matching (e.g. "gstatic.com" matches both
// "www.gstatic.com" and "connectivitycheck.gstatic.com").
func (d *LimitDispatcher) SetFilterDomains(domains []string) {
	m := make(map[string]bool, len(domains))
	for _, domain := range domains {
		domain = strings.TrimSpace(strings.ToLower(domain))
		if domain != "" {
			m[domain] = true
		}
	}
	d.filterMu.Lock()
	d.filterDomains = m
	d.filterMu.Unlock()
	if len(m) > 0 {
		nlog.Core().Info("xray: device filter domains configured", "count", len(m))
	}
}

// isFilteredDomain returns true if the destination is a domain that should be
// excluded from device tracking. Supports exact and suffix matching.
func (d *LimitDispatcher) isFilteredDomain(dest net.Destination) bool {
	if !dest.Address.Family().IsDomain() {
		return false
	}
	d.filterMu.RLock()
	domains := d.filterDomains
	d.filterMu.RUnlock()
	if len(domains) == 0 {
		return false
	}
	host := strings.ToLower(dest.Address.Domain())
	if domains[host] {
		return true
	}
	for domain := range domains {
		if strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

func (d *LimitDispatcher) ResetConns() {
	d.mu.Lock()
	d.limitedIPs = make(map[string]map[string]int)
	d.trackedConns = make(map[string]map[string]map[uint64]*closeTrackingWriter)
	d.mu.Unlock()

	// Clear unlimited IPs
	d.unlimitedIPs.Range(func(key, _ interface{}) bool {
		d.unlimitedIPs.Delete(key)
		return true
	})

	d.connCount.Store(0)
}

// GetConnectionState returns dispatcher-tracked alive IPs and connection count.
// Traffic bytes are intentionally left to xray's built-in stats pipeline.
func (d *LimitDispatcher) GetConnectionState() (aliveIPs map[int]map[string]bool, connCount int) {
	d.mu.RLock()
	emailToUID := make(map[string]int, len(d.emailToUID))
	for email, uid := range d.emailToUID {
		emailToUID[email] = uid
	}
	limitedIPs := make(map[string]map[string]bool, len(d.limitedIPs))
	for email, ipsMap := range d.limitedIPs {
		ipSet := make(map[string]bool, len(ipsMap))
		for ip, count := range ipsMap {
			if count > 0 {
				ipSet[ip] = true
			}
		}
		if len(ipSet) > 0 {
			limitedIPs[email] = ipSet
		}
	}
	d.mu.RUnlock()

	aliveIPs = make(map[int]map[string]bool)

	// Collect IPs from limited users (under RLock snapshot).
	for email, ipsMap := range limitedIPs {
		uid := emailToUID[email]
		if uid == 0 {
			continue
		}
		aliveIPs[uid] = ipsMap
	}

	// Collect IPs from unlimited users (lock-free).
	d.unlimitedIPs.Range(func(key, value interface{}) bool {
		email := key.(string)
		uid := emailToUID[email]
		if uid == 0 {
			return true
		}
		ic := value.(*ipCounter)
		if ips := ic.aliveIPs(); len(ips) > 0 {
			// Merge with limited IPs if any
			if existing, ok := aliveIPs[uid]; ok {
				for ip := range ips {
					existing[ip] = true
				}
			} else {
				aliveIPs[uid] = ips
			}
		}
		return true
	})

	connCount = int(d.connCount.Load())
	return
}

// ─── Internal helpers ───────────────────────────────────────────────────────

// checkDeviceLimit enforces per-user device limits.
// Fast path: unlimited users use lock-free sync.Map.
// Slow path: limited users use RWMutex with deterministic IP ordering,
// merging local IPs with global device state from the panel when fresh.
func (d *LimitDispatcher) checkDeviceLimit(email, sourceIP string, isTCP bool) bool {
	d.mu.RLock()
	limit, hasLimit := d.deviceLimits[email]
	uid := d.emailToUID[email]

	// Fast path: no device limit — use lock-free sync.Map.
	if !hasLimit || limit <= 0 {
		d.mu.RUnlock()
		if isTCP {
			v, _ := d.unlimitedIPs.LoadOrStore(email, &ipCounter{})
			ic := v.(*ipCounter)
			rv, _ := ic.ips.LoadOrStore(sourceIP, &atomic.Int64{})
			rv.(*atomic.Int64).Add(1)
		}
		return false
	}

	ips := d.limitedIPs[email]
	localIPs := make(map[string]bool, len(ips))
	for ip, count := range ips {
		if count > 0 {
			localIPs[ip] = true
		}
	}
	d.mu.RUnlock()

	// Read global device state (separate lock, never held with mu).
	var globalIPs map[string]bool
	d.globalMu.RLock()
	if time.Since(d.globalLastUpdate) <= 60*time.Second {
		if ips := d.globalDevices[uid]; ips != nil {
			globalIPs = make(map[string]bool, len(ips))
			for ip := range ips {
				globalIPs[ip] = true
			}
		}
	}
	d.globalMu.RUnlock()

	// Fresh global data is authoritative. All nodes compute the same
	// deterministic allow-list and reject IPs outside it, even if that IP was
	// previously known globally.
	if globalIPs != nil {
		allIPs := mergeIPSets(localIPs, globalIPs)
		if allIPs[sourceIP] {
			if !isIPAllowed(allIPs, sourceIP, limit) {
				return true
			}
			d.addLimitedIP(email, sourceIP, isTCP)
			return false
		}
		if len(allIPs) >= limit {
			return true
		}
		d.addLimitedIP(email, sourceIP, isTCP)
		return false
	}

	// No fresh global data → local-only check.
	if localIPs[sourceIP] {
		d.addLimitedIP(email, sourceIP, isTCP)
		return false
	}
	if len(localIPs) < limit {
		d.addLimitedIP(email, sourceIP, isTCP)
		return false
	}
	return true
}

// addLimitedIP registers sourceIP for a limited user (TCP only).
func (d *LimitDispatcher) addLimitedIP(email, sourceIP string, isTCP bool) {
	if !isTCP {
		return
	}
	d.mu.Lock()
	if d.limitedIPs == nil {
		d.limitedIPs = make(map[string]map[string]int)
	}
	if d.limitedIPs[email] == nil {
		d.limitedIPs[email] = make(map[string]int)
	}
	d.limitedIPs[email][sourceIP]++
	d.mu.Unlock()
}

// delConn decrements the IP refcount when a connection closes.
func (d *LimitDispatcher) delConn(email, sourceIP string) {
	// Check if this is an unlimited user first (lock-free).
	if v, ok := d.unlimitedIPs.Load(email); ok {
		ic := v.(*ipCounter)
		if rv, ok := ic.ips.Load(sourceIP); ok {
			counter := rv.(*atomic.Int64)
			if counter.Add(-1) <= 0 {
				ic.ips.Delete(sourceIP)
			}
		}
		return
	}

	// Limited user — use write lock.
	d.mu.Lock()
	defer d.mu.Unlock()
	if ips, ok := d.limitedIPs[email]; ok {
		ips[sourceIP]--
		if ips[sourceIP] <= 0 {
			delete(ips, sourceIP)
		}
		if len(ips) == 0 {
			delete(d.limitedIPs, email)
		}
	}
}

func (d *LimitDispatcher) registerTrackedConn(email, sourceIP string, connID uint64, writer *closeTrackingWriter) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.trackedConns == nil {
		d.trackedConns = make(map[string]map[string]map[uint64]*closeTrackingWriter)
	}
	if d.trackedConns[email] == nil {
		d.trackedConns[email] = make(map[string]map[uint64]*closeTrackingWriter)
	}
	if d.trackedConns[email][sourceIP] == nil {
		d.trackedConns[email][sourceIP] = make(map[uint64]*closeTrackingWriter)
	}
	d.trackedConns[email][sourceIP][connID] = writer
}

func (d *LimitDispatcher) unregisterTrackedConn(email, sourceIP string, connID uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ipConns := d.trackedConns[email]
	if ipConns == nil {
		return
	}
	conns := ipConns[sourceIP]
	if conns == nil {
		return
	}
	delete(conns, connID)
	if len(conns) == 0 {
		delete(ipConns, sourceIP)
	}
	if len(ipConns) == 0 {
		delete(d.trackedConns, email)
	}
}

func (d *LimitDispatcher) enforceDeviceLimits() {
	globalDevices, hasFreshGlobal := d.globalDeviceSnapshot()

	var victims []*closeTrackingWriter
	staleRemovals := make(map[string][]string)
	overflowIPs := 0

	d.mu.RLock()
	for email, limit := range d.deviceLimits {
		if limit <= 0 {
			continue
		}

		limitedIPs := d.limitedIPs[email]
		ipConns := d.trackedConns[email]
		if len(limitedIPs) == 0 && len(ipConns) == 0 {
			continue
		}

		localIPs := make(map[string]bool, len(limitedIPs)+len(ipConns))
		for ip, count := range limitedIPs {
			if count > 0 {
				localIPs[ip] = true
			}
		}
		for ip := range ipConns {
			localIPs[ip] = true
		}

		candidateIPs := localIPs
		if hasFreshGlobal {
			uid := d.emailToUID[email]
			candidateIPs = mergeIPSets(candidateIPs, globalDevices[uid])
		}
		if len(candidateIPs) <= limit {
			continue
		}

		allowed := firstNAllowedIPs(candidateIPs, limit)
		for ip := range localIPs {
			if allowed[ip] {
				continue
			}
			overflowIPs++
			if conns := ipConns[ip]; len(conns) > 0 {
				for _, writer := range conns {
					victims = append(victims, writer)
				}
			}
			if limitedIPs[ip] > 0 {
				staleRemovals[email] = append(staleRemovals[email], ip)
			}
		}
	}
	d.mu.RUnlock()

	if len(staleRemovals) > 0 {
		d.mu.Lock()
		for email, ips := range staleRemovals {
			for _, ip := range ips {
				if current := d.limitedIPs[email]; current != nil {
					delete(current, ip)
					if len(current) == 0 {
						delete(d.limitedIPs, email)
					}
				}
			}
		}
		d.mu.Unlock()
	}

	for _, writer := range victims {
		writer.Interrupt()
	}

	if len(victims) > 0 || len(staleRemovals) > 0 {
		nlog.Core().Info("xray: device limit enforced, kicked overflow connections",
			"connections", len(victims), "ips", overflowIPs, "stale_ips", countStaleRemovals(staleRemovals))
	}
}

func countStaleRemovals(m map[string][]string) int {
	total := 0
	for _, ips := range m {
		total += len(ips)
	}
	return total
}

func (d *LimitDispatcher) globalDeviceSnapshot() (map[int]map[string]bool, bool) {
	d.globalMu.RLock()
	defer d.globalMu.RUnlock()

	if d.globalLastUpdate.IsZero() || time.Since(d.globalLastUpdate) > 60*time.Second {
		return nil, false
	}

	result := make(map[int]map[string]bool, len(d.globalDevices))
	for uid, ips := range d.globalDevices {
		copied := make(map[string]bool, len(ips))
		for ip := range ips {
			copied[ip] = true
		}
		result[uid] = copied
	}
	return result, true
}

func mergeIPSets(sets ...map[string]bool) map[string]bool {
	total := 0
	for _, set := range sets {
		total += len(set)
	}
	result := make(map[string]bool, total)
	for _, set := range sets {
		for ip := range set {
			result[ip] = true
		}
	}
	return result
}

func isIPAllowed(ips map[string]bool, sourceIP string, limit int) bool {
	if limit <= 0 {
		return false
	}
	if len(ips) <= limit {
		return true
	}
	return firstNAllowedIPs(ips, limit)[sourceIP]
}

func firstNAllowedIPs(ips map[string]bool, limit int) map[string]bool {
	if limit <= 0 {
		return map[string]bool{}
	}

	ipList := make([]string, 0, len(ips))
	for ip := range ips {
		ipList = append(ipList, ip)
	}
	sort.Strings(ipList)

	if limit > len(ipList) {
		limit = len(ipList)
	}
	allowed := make(map[string]bool, limit)
	for i := 0; i < limit; i++ {
		allowed[ipList[i]] = true
	}
	return allowed
}

type closeTrackingWriter struct {
	buf.Writer
	onClose func()
	closed  atomic.Bool
}

func (w *closeTrackingWriter) Close() error {
	if w.closed.CompareAndSwap(false, true) {
		w.onClose()
	}
	return common.Close(w.Writer)
}

func (w *closeTrackingWriter) Interrupt() {
	if w.closed.CompareAndSwap(false, true) {
		w.onClose()
	}
	common.Interrupt(w.Writer)
}
