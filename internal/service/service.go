package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/cert"
	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/kernel/singbox"
	"github.com/cedar2025/xboard-node/internal/kernel/xray"
	"github.com/cedar2025/xboard-node/internal/limiter"
	"github.com/cedar2025/xboard-node/internal/monitor"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/cedar2025/xboard-node/internal/panel"
	"github.com/cedar2025/xboard-node/internal/tracker"
)

type Service struct {
	cfg          *config.Config
	panel        *panel.Client
	kernel       kernel.Kernel
	tracker      *tracker.Tracker
	limiter      *limiter.Limiter
	speedTracker *limiter.SpeedTracker
	cert         *cert.Manager

	lastConfig *panel.NodeConfig
	lastUsers  []panel.User

	// nodeLog is the logger with node context for this service instance.
	nodeLog *nlog.NodeLog

	// appliedState tracks the configuration and users that are currently
	// successfully running in the kernel.
	appliedState struct {
		Config *panel.NodeConfig
		Users  []panel.User
	}

	pushInterval int // seconds
	pullInterval int // seconds

	lastUserHash   string     // hash of user list for change detection
	lastConfigHash string     // hash of full config for change detection
	pullBackoff    apiBackoff // backoff for panel pull failures
	pushBackoff    apiBackoff // backoff for panel push failures

	// pushActive prevents overlapping push/pull goroutines.
	pushActive atomic.Bool
	pullActive atomic.Bool
	// pullResults delivers async pullViaAPI results back to the main goroutine.
	pullResults chan pullResult

	wsClient       *panel.WSClient           // WebSocket client (nil if WS not enabled)
	wsEvents       chan panel.WSEvent        // receives data events from WS client
	wsStatusCh     chan panel.WSStatusChange // receives WS connect/disconnect notifications
	wsCancel       context.CancelFunc        // cancels the WS client goroutine
	wsDisconnectAt time.Time                 // when WS last disconnected (zero if connected)

	// metricsMu: lastUsers, lastConfig, wsClient, wsDisconnectAt (buildMetrics vs main loop).
	metricsMu sync.RWMutex
}

// pullResult carries the outcome of an async pullViaAPI back to the main goroutine.
type pullResult struct {
	config      *panel.NodeConfig
	users       []panel.User
	configHash  string
	userHash    string
	certChanged bool
}

// apiBackoff implements simple exponential backoff for API failures.
type apiBackoff struct {
	mu            sync.Mutex
	skipRemaining int
}

func (b *apiBackoff) shouldSkip() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.skipRemaining > 0 {
		b.skipRemaining--
		return true
	}
	return false
}

func (b *apiBackoff) onSuccess() {
	b.mu.Lock()
	b.skipRemaining = 0
	b.mu.Unlock()
}

func (b *apiBackoff) onFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.skipRemaining <= 0 {
		b.skipRemaining = 1
	} else if b.skipRemaining < 8 {
		b.skipRemaining *= 2
	}
}

func New(cfg *config.Config) *Service {
	panelClient := panel.NewClient(cfg.Panel)
	certMgr := cert.NewManager(cfg.Cert)

	var k kernel.Kernel
	switch cfg.Kernel.Type {
	case "singbox":
		k = singbox.New(cfg.Kernel)
	case "xray":
		k = xray.New(cfg.Kernel)
	default:
		nlog.Core().Error("unsupported kernel type, defaulting to sing-box", "type", cfg.Kernel.Type)
		k = singbox.New(cfg.Kernel)
	}

	l := limiter.New()
	st := limiter.NewSpeedTracker(l)

	return &Service{
		cfg:          cfg,
		panel:        panelClient,
		kernel:       k,
		tracker:      tracker.New(),
		limiter:      l,
		speedTracker: st,
		cert:         certMgr,
		wsEvents:     make(chan panel.WSEvent, 16),
		wsStatusCh:   make(chan panel.WSStatusChange, 4),
		pullResults:  make(chan pullResult, 1),
	}
}

func (s *Service) Run(ctx context.Context) error {
	// Start cert manager (handles auto-TLS or manual cert verification)
	if err := s.cert.Start(ctx); err != nil {
		return fmt.Errorf("cert manager: %w", err)
	}
	defer s.cert.Stop()

	// Handshake: get WS config + initial data in one call
	if err := s.initialSetup(ctx); err != nil {
		return fmt.Errorf("initial setup: %w", err)
	}
	defer s.kernel.Stop()

	// Set up tickers
	trackTicker := time.NewTicker(time.Duration(s.cfg.Node.TrackInterval) * time.Second)
	pushInterval := time.Duration(math.Max(float64(s.pushInterval), 5)) * time.Second
	pullInterval := time.Duration(s.pullInterval) * time.Second
	reportTicker := time.NewTicker(pushInterval)
	pullTicker := time.NewTicker(pullInterval)
	deviceReportTicker := time.NewTicker(time.Duration(s.cfg.Node.DeviceReportInterval) * time.Second)

	// WS discovery: when in REST-only mode, periodically re-handshake to check
	// if WS has been enabled. When WS is disconnected for too long, re-check
	// if it's still available.
	wsDiscoveryTicker := time.NewTicker(time.Duration(s.cfg.WS.DiscoveryInterval) * time.Second)

	defer trackTicker.Stop()
	defer reportTicker.Stop()
	defer pullTicker.Stop()
	defer deviceReportTicker.Stop()
	defer wsDiscoveryTicker.Stop()

	s.startWSClient(ctx)

	for {
		select {
		case <-ctx.Done():
			s.pushReportSync()
			return nil

		case <-trackTicker.C:
			s.trackAndEnforce(ctx)

		case <-reportTicker.C:
			s.pushReportAsync()

		case <-deviceReportTicker.C:
			s.reportDevices()

		case <-pullTicker.C:
			// When WebSocket is connected, skip REST polling entirely.
			// Config/user updates arrive via WS push.
			if s.wsClient != nil && s.wsClient.IsConnected() {
				continue
			}
			nlog.Core().Debug("polling from API (ws not connected)")
			s.pullViaAPIAsync(ctx)

		case result := <-s.pullResults:
			s.applyPullResult(ctx, result)

		case <-wsDiscoveryTicker.C:
			s.wsDiscovery(ctx)

		case status := <-s.wsStatusCh:
			s.handleWSStatus(ctx, status)

		case event := <-s.wsEvents:
			s.handleWSEvent(ctx, event)
		}
	}
}

func (s *Service) initialSetup(ctx context.Context) error {
	// Register speed limit lookup with kernel unconditionally (before WS/V1 branch).
	// This ensures speedLimitFunc is set regardless of which code path applies config.
	s.kernel.SetSpeedLimitFunc(s.speedTracker.GetLimiter)
	s.kernel.SetDeviceLimitFunc(s.limiter.GetDeviceLimitByUUID)

	var hs *panel.HandshakeResponse
	var err error

	// Retry loop for initial handshake
	for attempt := 1; ; attempt++ {
		hs, err = s.panel.Handshake()
		if err != nil {
			nlog.Core().Error(fmt.Sprintf("handshake failed (attempt %d): %v", attempt, err))
			if attempt >= 5 {
				return fmt.Errorf("handshake failed after %d attempts: %w", attempt, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*5) * time.Second):
				continue
			}
		}
		break
	}

	// Apply settings from panel handshake (Handshake provides default intervals)
	if s.cfg.Node.PushInterval == 0 && hs.Settings.PushInterval > 0 {
		s.pushInterval = hs.Settings.PushInterval
	} else {
		s.pushInterval = s.cfg.Node.PushInterval
	}
	if s.pushInterval == 0 {
		s.pushInterval = 60
	}

	if s.cfg.Node.PullInterval == 0 && hs.Settings.PullInterval > 0 {
		s.pullInterval = hs.Settings.PullInterval
	} else {
		s.pullInterval = s.cfg.Node.PullInterval
	}
	if s.pullInterval == 0 {
		s.pullInterval = 60
	}

	// If WebSocket is enabled, skip initial V1 APIs fetch and wait for push
	if hs.WebSocket.Enabled && hs.WebSocket.WSURL != "" {
		s.wsClient = s.newWSClient(hs.WebSocket.WSURL)
		return nil
	}

	// Falls back to V1 APIs only if WebSocket is disabled in panel
	nlog.Core().Info("websocket disabled, using REST API")

	// Fetch initial config and users via V1 APIs
	nodeConfig, err := s.panel.GetConfig()
	if err != nil {
		return fmt.Errorf("initial config fetch: %w", err)
	}
	if nodeConfig == nil {
		return fmt.Errorf("initial config is nil")
	}

	users, err := s.panel.GetUsers()
	if err != nil {
		return fmt.Errorf("initial user fetch: %w", err)
	}

	// Apply initial data
	s.lastConfig = nodeConfig
	s.lastUsers = users
	s.lastUserHash = computeUserHash(users)
	s.lastConfigHash = computeConfigHash(nodeConfig)
	s.limiter.UpdateUsers(users)
	s.speedTracker.UpdateBuckets()

	nlog.Core().Info("handshake complete (V1 fallback)",
		"protocol", nodeConfig.Protocol,
		"port", nodeConfig.ServerPort,
		"users", len(users),
	)

	if len(users) == 0 {
		nlog.Core().Warn("no users, kernel will not start until users are available")
		return nil
	}

	// Update service-level config overrides from remote NodeConfig if present
	s.applyRemoteOverrides(ctx, nodeConfig)

	if err := s.kernel.Start(nodeConfig, users, s.cert.CertFile(), s.cert.KeyFile()); err != nil {
		return fmt.Errorf("start kernel: %w", err)
	}

	// record applied state on success
	s.appliedState.Config = nodeConfig
	s.appliedState.Users = users

	return nil
}

// applyRemoteOverrides updates service-level settings (log level, cert config)
// from the panel's NodeConfig. Returns true if cert paths changed (kernel restart needed).
func (s *Service) applyRemoteOverrides(ctx context.Context, nc *panel.NodeConfig) bool {
	if nc == nil {
		return false
	}

	// Dynamic Log Level (Kernel)
	if nc.KernelLogLevel != "" && nc.KernelLogLevel != s.cfg.Kernel.LogLevel {
		nlog.Core().Info("cert: kernel log level override", "old", s.cfg.Kernel.LogLevel, "new", nc.KernelLogLevel)
		s.cfg.Kernel.LogLevel = nc.KernelLogLevel
	}

	// Certificate configuration from panel (panel-first: takes precedence over local config)
	if nc.CertConfig != nil {
		return s.applyPanelCert(ctx, nc.CertConfig)
	}

	// Legacy fields (deprecated: prefer cert_config)
	if nc.AutoTLS != s.cfg.Cert.AutoTLS {
		nlog.Core().Info("cert: auto_tls policy changed (deprecated field)", "new", nc.AutoTLS)
		s.cfg.Cert.AutoTLS = nc.AutoTLS
	}
	if nc.Domain != "" && nc.Domain != s.cfg.Cert.Domain {
		s.cfg.Cert.Domain = nc.Domain
	}

	return false
}

// applyPanelCert converts a panel CertConfig into the local config format and
// reconfigures the cert manager. Reports whether cert paths changed.
func (s *Service) applyPanelCert(ctx context.Context, pc *panel.CertConfig) bool {
	newCfg := config.CertConfig{
		CertMode:    pc.CertMode,
		Domain:      pc.Domain,
		Email:       pc.Email,
		DNSProvider: pc.DNSProvider,
		DNSEnv:      pc.DNSEnv,
		HTTPPort:    pc.HTTPPort,
		CertFile:    pc.CertFile,
		KeyFile:     pc.KeyFile,
		CertContent: pc.CertContent,
		KeyContent:  pc.KeyContent,
		// Preserve local storage dir — only the operator controls where certs live.
		CertDir: s.cfg.Cert.CertDir,
	}

	changed, err := s.cert.Reconfigure(ctx, newCfg)
	if err != nil {
		nlog.Core().Error("failed to apply panel cert config", "mode", pc.CertMode, "error", err)
		return false
	}
	s.cfg.Cert = newCfg
	if changed {
		msg := fmt.Sprintf("cert: paths updated from panel cert=%s key=%s", s.cert.CertFile(), s.cert.KeyFile())
		if s.nodeLog != nil {
			s.nodeLog.Info(msg)
		} else {
			nlog.Core().Info(msg)
		}
	}
	return changed
}

// startWSClient starts the WS client goroutine if a client is configured.
func (s *Service) startWSClient(ctx context.Context) {
	if s.wsClient == nil {
		return
	}
	wsCtx, wsCancel := context.WithCancel(ctx)
	s.wsCancel = wsCancel
	go s.wsClient.Run(wsCtx)
}

// newWSClient creates a WSClient with standard event/status callbacks.
func (s *Service) newWSClient(wsURL string) *panel.WSClient {
	wsCfg := panel.WSClientConfig{
		StatusInterval:   time.Duration(s.cfg.WS.StatusInterval) * time.Second,
		HandshakeTimeout: time.Duration(s.cfg.WS.HandshakeTimeout) * time.Second,
		BackoffInitial:   time.Duration(s.cfg.WS.BackoffInitial) * time.Second,
		BackoffMax:       time.Duration(s.cfg.WS.BackoffMax) * time.Second,
	}
	return panel.NewWSClient(
		wsURL,
		s.cfg.Panel.Token,
		s.cfg.Panel.NodeID,
		wsCfg,
		func(event panel.WSEvent) {
			select {
			case s.wsEvents <- event:
			default:
				nlog.Core().Warn("ws event channel full, dropping event", "type", event.Type)
			}
		},
		func(status panel.WSStatusChange) {
			select {
			case s.wsStatusCh <- status:
			default:
			}
		},
		func() map[string]interface{} {
			status := monitor.Collect()
			m := s.buildMetrics(status)
			m["kernel_status"] = s.kernel.IsRunning()
			return m
		},
	)
}

// handleWSStatus reacts to WS connectivity changes.
//
// - On disconnect: record timestamp, immediately REST poll.
// - On reconnect: clear disconnect timestamp, REST poll to catch missed events.
func (s *Service) handleWSStatus(ctx context.Context, status panel.WSStatusChange) {
	if status.Connected {
		s.metricsMu.Lock()
		s.wsDisconnectAt = time.Time{}
		s.metricsMu.Unlock()
		// Use nodeLog if available, otherwise core
		if s.nodeLog != nil {
			s.nodeLog.Info("ws connected")
		} else {
			nlog.Core().Info("ws connected")
		}
		// After reconnect, proactively pull once to ensure we haven't missed
		// any updates during the disconnection window.
		s.pullViaAPIAsync(ctx)
	} else {
		s.metricsMu.Lock()
		if s.wsDisconnectAt.IsZero() {
			s.wsDisconnectAt = time.Now()
		}
		s.metricsMu.Unlock()
		if s.nodeLog != nil {
			s.nodeLog.Info("ws disconnected")
		} else {
			nlog.Core().Info("ws disconnected")
		}
		// Clear global device state on disconnect
		s.kernel.ClearGlobalDevices()
		s.pullViaAPIAsync(ctx)
	}
}

// wsDiscovery periodically checks WS availability:
//
//  1. REST-only mode (wsClient == nil): Re-handshake to check if panel now has
//     WS enabled. If so, create and start a WS client. This handles the case
//     where WS was not enabled at startup but enabled later.
//
//  2. WS disconnected for >10 min: Re-handshake to check if WS config changed.
//     If WS is now disabled, stop the WS client and switch to REST-only.
//     If WS config changed (different URL/channel), restart with new config.
func (s *Service) wsDiscovery(ctx context.Context) {
	needsCheck := false

	if s.wsClient == nil {
		needsCheck = true
		nlog.Core().Debug("ws discovery: no WS client, checking if panel enabled WS")
	} else if !s.wsDisconnectAt.IsZero() && time.Since(s.wsDisconnectAt) > 10*time.Minute {
		needsCheck = true
		nlog.Core().Debug("ws discovery: WS disconnected for >10min, re-checking WS config")
	}

	if !needsCheck {
		return
	}

	hs, err := s.panel.Handshake()
	if err != nil {
		nlog.Core().Debug("ws discovery: handshake failed", "error", err)
		return
	}

	// Apply any latest config/user changes via dedicated APIs
	s.pullViaAPIAsync(ctx)

	if hs.WebSocket.Enabled && hs.WebSocket.WSURL != "" {
		if s.wsClient == nil {
			nlog.Core().Info("ws discovery: panel has WS enabled, creating WS client")
			wsc := s.newWSClient(hs.WebSocket.WSURL)
			s.metricsMu.Lock()
			s.wsClient = wsc
			s.wsDisconnectAt = time.Time{}
			s.metricsMu.Unlock()
			s.startWSClient(ctx)
		}
	} else if s.wsClient != nil {
		nlog.Core().Info("ws discovery: panel disabled WS, switching to REST-only")
		if s.wsCancel != nil {
			s.wsCancel()
		}
		s.metricsMu.Lock()
		s.wsClient = nil
		s.wsDisconnectAt = time.Time{}
		s.metricsMu.Unlock()
		s.wsCancel = nil
	}
}

// handleWSEvent processes data events received via WebSocket
func (s *Service) handleWSEvent(ctx context.Context, event panel.WSEvent) {
	switch event.Type {
	case panel.WSEventSyncConfig:
		if event.Config == nil {
			return
		}
		newConfigHash := computeConfigHash(event.Config)
		if newConfigHash == s.lastConfigHash {
			return
		}
		// Initialize nodeLog on first config
		if s.nodeLog == nil {
			s.nodeLog = nlog.ForNode(event.Config.Protocol, event.Config.ServerPort)
		}
		s.nodeLog.Info(fmt.Sprintf("config updated, %d users", len(event.Users)))
		s.metricsMu.Lock()
		s.lastConfig = event.Config
		s.metricsMu.Unlock()
		s.lastConfigHash = newConfigHash
		s.applyRemoteOverrides(ctx, event.Config)
		s.applyChanges(ctx, true, false)

	case panel.WSEventSyncUsers:
		if event.Users == nil {
			return
		}
		newHash := computeUserHash(event.Users)
		if newHash == s.lastUserHash {
			return
		}
		if s.nodeLog != nil {
			s.nodeLog.Info(fmt.Sprintf("users updated, %d users", len(event.Users)))
		}
		s.applyUserUpdate(ctx, event.Users, newHash)

	case panel.WSEventSyncUserDelta:
		if len(event.DeltaUsers) == 0 {
			return
		}
		if s.nodeLog != nil {
			s.nodeLog.Info(fmt.Sprintf("users delta: %s, %d users", event.DeltaAction, len(event.DeltaUsers)))
		}
		s.applyUserDelta(ctx, event.DeltaAction, event.DeltaUsers)

	case panel.WSEventSyncDevices:
		// Sync global device state
		if event.DeviceUsers != nil {
			s.kernel.UpdateGlobalDevices(event.DeviceUsers)
		}

	default:
		nlog.Core().Debug(fmt.Sprintf("unknown ws event: %v", event.Type))
	}
}

// pullViaAPIAsync fetches config/users from the panel API in a background
// goroutine and sends the result to pullResults for the main goroutine to apply.
func (s *Service) pullViaAPIAsync(ctx context.Context) {
	if !s.pullActive.CompareAndSwap(false, true) {
		nlog.Core().Debug("pull already in progress, skipping")
		return
	}

	if s.pullBackoff.shouldSkip() {
		nlog.Core().Debug("skipping pull due to backoff")
		s.pullActive.Store(false)
		return
	}

	// Capture current hashes for comparison in the goroutine.
	currentConfigHash := s.lastConfigHash
	certChanged := s.cert.CertRenewed()

	go func() {
		defer s.pullActive.Store(false)

		config, err := s.panel.GetConfig()
		if err != nil {
			nlog.Core().Error("poll config failed", "error", err)
			s.pullBackoff.onFailure()
			return
		}

		users, err := s.panel.GetUsers()
		if err != nil {
			nlog.Core().Error("poll users failed", "error", err)
			s.pullBackoff.onFailure()
			return
		}

		s.pullBackoff.onSuccess()

		result := pullResult{
			config:      config,
			users:       users,
			certChanged: certChanged,
		}
		if config != nil {
			result.configHash = computeConfigHash(config)
		}
		if users != nil {
			result.userHash = computeUserHash(users)
		}

		// Detect config change using captured hash.
		if config != nil && result.configHash == currentConfigHash && !certChanged {
			result.config = nil // signal no change
		}

		select {
		case s.pullResults <- result:
		case <-ctx.Done():
		}
	}()
}

// applyPullResult processes the result of an async pullViaAPI on the main goroutine.
func (s *Service) applyPullResult(ctx context.Context, result pullResult) {
	configChanged := false

	if result.certChanged {
		nlog.Core().Info("certificate renewed, kernel restart needed")
		configChanged = true
	}

	if result.config != nil {
		configChanged = true
		// Initialize or update node logger
		if s.nodeLog == nil {
			s.nodeLog = nlog.ForNode(result.config.Protocol, result.config.ServerPort)
		}
		s.nodeLog.Info(fmt.Sprintf("config updated, %d users", len(s.lastUsers)))
		s.metricsMu.Lock()
		s.lastConfig = result.config
		s.metricsMu.Unlock()
		s.lastConfigHash = result.configHash
		if s.applyRemoteOverrides(ctx, result.config) {
			configChanged = true
		}
	}

	if result.users != nil {
		usersChanged := result.userHash != s.lastUserHash

		if usersChanged && !configChanged {
			s.applyUserUpdate(ctx, result.users, result.userHash)
		} else if usersChanged {
			s.updateUserState(result.users)
		}
	}

	if configChanged {
		s.applyChanges(ctx, true, false)
	}
}

// ─── User state helpers ─────────────────────────────────────────────────────

func (s *Service) updateUserState(users []panel.User) {
	// Defensive nil check
	if users == nil {
		users = []panel.User{}
	}

	s.limiter.UpdateUsers(users)
	s.speedTracker.UpdateBuckets()

	s.metricsMu.Lock()
	s.lastUsers = users
	s.metricsMu.Unlock()

	s.lastUserHash = computeUserHash(users)
}

// startKernel starts (or restarts) the kernel with the given config/users and
// records the successfully applied state. Returns false on error.
func (s *Service) startKernel(nc *panel.NodeConfig, users []panel.User) bool {
	if err := s.kernel.Start(nc, users, s.cert.CertFile(), s.cert.KeyFile()); err != nil {
		nlog.Core().Error("failed to start kernel", "error", err)
		return false
	}

	s.appliedState.Config = nc
	s.appliedState.Users = users

	// Initialize node logger on first successful start
	if s.nodeLog == nil {
		s.nodeLog = nlog.ForNode(nc.Protocol, nc.ServerPort)
	}
	s.speedTracker.SetLogCallback(func(msg string) {
		fullMsg := fmt.Sprintf("speedtracker: %s active_limiters=%d", msg, s.speedTracker.LimitedUserCount())
		s.nodeLog.Info(fullMsg)
	})
	s.nodeLog.Info(fmt.Sprintf("started, %d users", len(users)))
	return true
}

// ensureRunning starts the kernel if it is not running and there are users +
// config available. Returns true if the kernel is running afterwards.
func (s *Service) ensureRunning() bool {
	if s.kernel.IsRunning() {
		return true
	}
	if len(s.lastUsers) > 0 && s.lastConfig != nil {
		return s.startKernel(s.lastConfig, s.lastUsers)
	}
	return false
}

// ─── User update entry points ───────────────────────────────────────────────

// applyUserUpdate replaces the full user set and hot-swaps the kernel.
// Called from WS sync.users and REST polling.
func (s *Service) applyUserUpdate(ctx context.Context, users []panel.User, newHash string) {
	if !s.ensureRunning() {
		return
	}

	added, removed, err := s.kernel.UpdateUsers(users)
	if err != nil {
		nlog.Core().Warn(fmt.Sprintf("UpdateUsers failed, restarting kernel: %v", err))
		s.startKernel(s.lastConfig, users)
		return
	}
	s.updateUserState(users)
	if s.nodeLog != nil && (added > 0 || removed > 0) {
		s.nodeLog.Info(fmt.Sprintf("users updated: +%d -%d", added, removed))
	}
}

// applyUserDelta applies an incremental user change (add or remove) directly
// via the kernel's atomic user API. Kernel updates run before updateUserState.
func (s *Service) applyUserDelta(ctx context.Context, action string, deltaUsers []panel.User) {
	switch action {
	case "add":
		if len(deltaUsers) == 0 {
			return
		}
		merged := mergeUsers(s.lastUsers, deltaUsers)

		if !s.ensureRunning() {
			return
		}

		for _, delta := range deltaUsers {
			for _, old := range s.lastUsers {
				if old.ID == delta.ID && old.UUID != delta.UUID {
					s.kernel.RemoveUsers([]panel.User{old})
					break
				}
			}
		}

		added, err := s.kernel.AddUsers(deltaUsers)
		if err != nil {
			nlog.Core().Warn(fmt.Sprintf("AddUsers failed: %v, falling back to UpdateUsers", err))
			if _, _, err := s.kernel.UpdateUsers(merged); err != nil {
				nlog.Core().Error(fmt.Sprintf("UpdateUsers fallback failed: %v", err))
				return
			}
		}
		s.updateUserState(merged)
		if s.nodeLog != nil && added > 0 {
			s.nodeLog.Info(fmt.Sprintf("users added: +%d", added))
		}

	case "remove":
		if len(deltaUsers) == 0 {
			return
		}
		filtered := subtractUsers(s.lastUsers, deltaUsers)

		if !s.kernel.IsRunning() {
			return
		}

		removed, err := s.kernel.RemoveUsers(deltaUsers)
		if err != nil {
			nlog.Core().Warn(fmt.Sprintf("RemoveUsers failed: %v, falling back to UpdateUsers", err))
			if _, _, err := s.kernel.UpdateUsers(filtered); err != nil {
				nlog.Core().Error(fmt.Sprintf("UpdateUsers fallback failed: %v", err))
				return
			}
		}
		s.updateUserState(filtered)
		if s.nodeLog != nil && removed > 0 {
			s.nodeLog.Info(fmt.Sprintf("users removed: -%d", removed))
		}

	default:
		nlog.Core().Warn(fmt.Sprintf("unknown user delta action: %s", action))
	}
}

// mergeUsers overlays deltaUsers onto base (keyed by ID). New users are
// appended, existing users have their properties overwritten.
func mergeUsers(base, delta []panel.User) []panel.User {
	// Handle nil slices
	if base == nil {
		base = []panel.User{}
	}
	if delta == nil {
		return base
	}

	m := make(map[int]panel.User, len(base))
	for _, u := range base {
		m[u.ID] = u
	}
	for _, u := range delta {
		m[u.ID] = u
	}
	out := make([]panel.User, 0, len(m))
	for _, u := range m {
		out = append(out, u)
	}
	return out
}

// subtractUsers returns base with all users in delta removed.
func subtractUsers(base, delta []panel.User) []panel.User {
	if base == nil {
		return nil
	}
	if len(delta) == 0 {
		return base
	}
	removeSet := make(map[int]struct{}, len(delta))
	for _, u := range delta {
		removeSet[u.ID] = struct{}{}
	}
	out := make([]panel.User, 0, len(base))
	for _, u := range base {
		if _, ok := removeSet[u.ID]; !ok {
			out = append(out, u)
		}
	}
	return out
}

// applyChanges applies config changes to the kernel. User-only changes are
// handled by applyUserUpdate/applyUserDelta directly via the atomic user API.
func (s *Service) applyChanges(ctx context.Context, configChanged, usersChanged bool) {
	if !configChanged {
		return
	}

	if s.lastConfig == nil || len(s.lastUsers) == 0 {
		if len(s.lastUsers) == 0 {
			s.kernel.Stop()
			s.appliedState.Users = nil
		}
		return
	}

	// If config changed, delegate to kernel.Reload. The kernel implementation
	// decides whether to hot-swap users, reconstruct inbounds, or restart itself.
	if configChanged && s.kernel.IsRunning() {
		if err := s.kernel.Reload(s.lastConfig, s.lastUsers, s.cert.CertFile(), s.cert.KeyFile()); err != nil {
			nlog.Core().Warn(fmt.Sprintf("reload failed, restarting: %v", err))
			s.startKernel(s.lastConfig, s.lastUsers)
		} else {
			s.appliedState.Config = s.lastConfig
			s.appliedState.Users = s.lastUsers
			if s.nodeLog != nil {
				s.nodeLog.Info(fmt.Sprintf("config updated, %d users", len(s.lastUsers)))
			}
		}
	} else if !s.kernel.IsRunning() {
		s.startKernel(s.lastConfig, s.lastUsers)
	}
}

func (s *Service) trackAndEnforce(ctx context.Context) {
	if !s.kernel.IsRunning() {
		return
	}

	traffic, aliveIPs, connCount, err := s.kernel.GetUserTraffic(ctx)
	if err != nil {
		nlog.Core().Debug("get user traffic failed", "error", err)
		return
	}

	s.tracker.Process(traffic, aliveIPs, connCount)

	// Only log stats if there's actual traffic or connections
	if connCount > 0 || len(traffic) > 0 {
		if s.nodeLog != nil {
			s.nodeLog.Debug(fmt.Sprintf("tracker: %d conns, %d users online", connCount, len(traffic)))
		} else {
			nlog.TrackerStats(connCount, len(traffic))
		}
	}
}

// pushReportAsync sends the report in a background goroutine so the select
// loop is never blocked by slow HTTP. Only one push runs at a time.
func (s *Service) pushReportAsync() {
	if !s.pushActive.CompareAndSwap(false, true) {
		nlog.Core().Debug("push already in progress, skipping")
		return
	}

	if s.pushBackoff.shouldSkip() {
		nlog.Core().Debug("skipping report due to backoff")
		s.pushActive.Store(false)
		return
	}

	// Snapshot data under the select goroutine (fast, mutex-only).
	traffic := s.tracker.FlushTraffic()
	aliveIPs := s.tracker.FlushAliveIPs()
	online := s.tracker.CurrentOnline()
	status := monitor.Collect()
	metrics := s.buildMetrics(status)
	metrics["kernel_status"] = s.kernel.IsRunning()

	go func() {
		defer s.pushActive.Store(false)

		if err := s.panel.Report(
			traffic, aliveIPs, online,
			status.CPU,
			[2]uint64{status.MemTotal, status.MemUsed},
			[2]uint64{status.SwapTotal, status.SwapUsed},
			[2]uint64{status.DiskTotal, status.DiskUsed},
			metrics,
		); err != nil {
			nlog.Core().Error("failed to push report", "error", err)
			if len(traffic) > 0 {
				s.tracker.RestoreTraffic(traffic)
			}
			if aliveIPs != nil {
				s.tracker.RestoreAliveIPs(aliveIPs)
			}
			s.pushBackoff.onFailure()
			return
		}

		s.pushBackoff.onSuccess()
		nlog.ReportPushed(len(traffic), len(online))
	}()
}

// pushReportSync is used only during shutdown to ensure final data is sent.
func (s *Service) pushReportSync() {
	traffic := s.tracker.FlushTraffic()
	aliveIPs := s.tracker.FlushAliveIPs()
	online := s.tracker.CurrentOnline()
	status := monitor.Collect()
	metrics := s.buildMetrics(status)
	metrics["kernel_status"] = s.kernel.IsRunning()

	if err := s.panel.Report(
		traffic, aliveIPs, online,
		status.CPU,
		[2]uint64{status.MemTotal, status.MemUsed},
		[2]uint64{status.SwapTotal, status.SwapUsed},
		[2]uint64{status.DiskTotal, status.DiskUsed},
		metrics,
	); err != nil {
		nlog.Core().Error("failed to push final report", "error", err)
	}
}

// buildMetrics aggregates node-level metrics to be reported to the panel.
// This includes active connections, per-core CPU, GC stats, API call stats,
// WebSocket status, and limiter hit counts.
func (s *Service) buildMetrics(status monitor.Status) map[string]interface{} {
	s.metricsMu.RLock()
	lastUsers := s.lastUsers
	wsClient := s.wsClient
	s.metricsMu.RUnlock()

	m := make(map[string]interface{})
	online := s.tracker.CurrentOnline()

	m["uptime"] = status.Uptime
	m["goroutines"] = status.Goroutines

	// Active connections (last measured during tracker.Process()).
	m["active_connections"] = s.tracker.ActiveConnections()
	m["total_connections"] = s.tracker.TotalConnections()
	m["active_users"] = len(online)
	m["total_users"] = len(lastUsers)

	// Speed
	m["inbound_speed"] = s.tracker.InboundSpeed()
	m["outbound_speed"] = s.tracker.OutboundSpeed()

	// Per-core CPU usage (if available).
	if len(status.CPUPerCore) > 0 {
		m["cpu_per_core"] = status.CPUPerCore
	}

	m["load"] = map[string]interface{}{
		"load1":  status.Load1,
		"load5":  status.Load5,
		"load15": status.Load15,
	}

	// Speed Limiter metrics
	m["speed_limiter"] = map[string]interface{}{
		"has_limits":    s.speedTracker.HasLimits(),
		"limited_users": s.speedTracker.LimitedUserCount(),
	}

	// GC metrics.
	m["gc"] = map[string]interface{}{
		"num_gc":        status.NumGC,
		"last_pause_ms": status.LastPauseMS,
	}

	// API metrics.
	api := s.panel.SnapshotMetrics()
	m["api"] = map[string]interface{}{
		"success": api.Success,
		"failure": api.Failure,
	}

	// WebSocket status.
	wsEnabled := wsClient != nil
	wsConnected := wsEnabled && wsClient.IsConnected()
	m["ws"] = map[string]interface{}{
		"enabled":   wsEnabled,
		"connected": wsConnected,
	}

	// Limiter metrics.
	lm := s.limiter.SnapshotMetrics()
	m["limits"] = map[string]interface{}{
		"device_limit_events": lm.DeviceLimitEvents,
		"speed_limited_users": s.speedTracker.LimitedUserCount(),
	}

	return m
}

// computeConfigHash returns a deterministic hash of the node config.
// It uses JSON marshaling to ensure all fields are captured, ensuring that
// any configuration change correctly triggers a kernel reload.
func computeConfigHash(cfg *panel.NodeConfig) string {
	if cfg == nil {
		return ""
	}
	h := sha256.New()
	// We marshal the entire config to be safe. Node config updates are low-frequency,
	// so the robustness of capturing all fields outweighs the micro-performance of manual hashing.
	data, _ := json.Marshal(cfg)
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// computeUserHash returns a deterministic hash of the user list for change detection.
// Uses direct byte encoding instead of binary.Write to avoid reflection overhead.
func computeUserHash(users []panel.User) string {
	sorted := make([]panel.User, len(users))
	copy(sorted, users)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	h := sha256.New()
	var buf [8]byte
	for _, u := range sorted {
		binary.LittleEndian.PutUint64(buf[:], uint64(u.ID))
		h.Write(buf[:])
		io.WriteString(h, u.UUID)
		binary.LittleEndian.PutUint64(buf[:], uint64(u.SpeedLimit))
		h.Write(buf[:])
		binary.LittleEndian.PutUint64(buf[:], uint64(u.DeviceLimit))
		h.Write(buf[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ─── Device management ──────────────────────────────────────────────────

// sendDeviceBatch reports local device snapshot to panel via WS.
func (s *Service) sendDeviceBatch() {
	if s.wsClient == nil || !s.wsClient.IsConnected() {
		return
	}

	devices := s.tracker.FlushAliveIPs()
	// FlushAliveIPs returns nil if no changes since last flush
	if devices == nil {
		nlog.Core().Debug("device snapshot unchanged, skipping")
		return
	}
	s.wsClient.SendDeviceReport(devices)
	nlog.Core().Debug("device snapshot sent", "users", len(devices))
}

// reportDevices periodically reports device snapshot to panel.
func (s *Service) reportDevices() {
	s.sendDeviceBatch()
}
