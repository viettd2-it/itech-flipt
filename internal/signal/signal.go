package signal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.flipt.io/flipt/internal/pubsub"
	"go.uber.org/zap"
)

// Handler defines the interface for signal handling
type Handler interface {
	// Start starts the signal handler
	Start(ctx context.Context) error
	// Stop stops the signal handler
	Stop(ctx context.Context) error
	// RegisterSignalHandler registers a handler for a specific signal type
	RegisterSignalHandler(signalType string, handler SignalHandlerFunc) error
	// SendSignal sends a signal
	SendSignal(ctx context.Context, signal Signal) error
	// IsRunning returns whether the handler is currently running
	IsRunning() bool
	fmt.Stringer
}

// SignalHandlerFunc is a function type for handling signals
type SignalHandlerFunc func(ctx context.Context, signal Signal) error

// Signal represents a signal message
type Signal struct {
	Type      string                 `json:"type"`
	Data      interface{}            `json:"data,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Source    string                 `json:"source"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// SignalType constants
const (
	TypeCacheInvalidation = "cache.invalidation"
	TypeConfigReload      = "config.reload"
	TypeFeatureFlagUpdate = "flag.update"
	TypeHealthCheck       = "health.check"
	TypeShutdown          = "system.shutdown"
	TypeRestart           = "system.restart"
	TypeCustom            = "custom"
)

// CacheInvalidationData represents cache invalidation signal data
type CacheInvalidationData struct {
	Keys      []string `json:"keys,omitempty"`
	Pattern   string   `json:"pattern,omitempty"`
	Namespace string   `json:"namespace,omitempty"`
	All       bool     `json:"all,omitempty"`
}

// ConfigReloadData represents configuration reload signal data
type ConfigReloadData struct {
	ConfigPath string                 `json:"config_path,omitempty"`
	Changes    map[string]interface{} `json:"changes,omitempty"`
	Force      bool                   `json:"force,omitempty"`
}

// FeatureFlagUpdateData represents feature flag update signal data
type FeatureFlagUpdateData struct {
	FlagKey   string `json:"flag_key"`
	Namespace string `json:"namespace,omitempty"`
	Action    string `json:"action"` // created, updated, deleted, enabled, disabled
}

// HealthCheckData represents health check signal data
type HealthCheckData struct {
	Status    string                 `json:"status"` // healthy, unhealthy, degraded
	Component string                 `json:"component"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// SystemData represents system signal data
type SystemData struct {
	Reason   string                 `json:"reason,omitempty"`
	Graceful bool                   `json:"graceful"`
	Timeout  time.Duration          `json:"timeout,omitempty"`
	Details  map[string]interface{} `json:"details,omitempty"`
}

// NewSignal creates a new signal with the given type and data
func NewSignal(signalType string, data interface{}) Signal {
	return Signal{
		Type:      signalType,
		Data:      data,
		Timestamp: time.Now().UTC(),
		Source:    "flipt",
		Metadata:  make(map[string]interface{}),
	}
}

// WithSource sets the source of the signal
func (s Signal) WithSource(source string) Signal {
	s.Source = source
	return s
}

// WithMetadata adds metadata to the signal
func (s Signal) WithMetadata(key string, value interface{}) Signal {
	if s.Metadata == nil {
		s.Metadata = make(map[string]interface{})
	}
	s.Metadata[key] = value
	return s
}

// Registry manages signal handlers
type Registry struct {
	mu       sync.RWMutex
	handlers map[string][]SignalHandlerFunc
	logger   *zap.Logger
}

// NewRegistry creates a new signal handler registry
func NewRegistry(logger *zap.Logger) *Registry {
	return &Registry{
		handlers: make(map[string][]SignalHandlerFunc),
		logger:   logger.With(zap.String("component", "signal-registry")),
	}
}

// Register registers a handler for a specific signal type
func (r *Registry) Register(signalType string, handler SignalHandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.handlers[signalType] = append(r.handlers[signalType], handler)
	r.logger.Info("registered signal handler", zap.String("signal_type", signalType))
}

// Handle processes a signal by calling all registered handlers
func (r *Registry) Handle(ctx context.Context, signal Signal) error {
	r.mu.RLock()
	handlers, exists := r.handlers[signal.Type]
	r.mu.RUnlock()

	if !exists {
		r.logger.Debug("no handlers registered for signal type", zap.String("signal_type", signal.Type))
		return nil
	}

	r.logger.Debug("processing signal",
		zap.String("signal_type", signal.Type),
		zap.String("source", signal.Source),
		zap.Int("handler_count", len(handlers)))

	for i, handler := range handlers {
		if err := handler(ctx, signal); err != nil {
			r.logger.Error("signal handler error",
				zap.String("signal_type", signal.Type),
				zap.Int("handler_index", i),
				zap.Error(err))
			return fmt.Errorf("handler %d for signal type %s: %w", i, signal.Type, err)
		}
	}

	return nil
}

// GetSignalTypes returns all registered signal types
func (r *Registry) GetSignalTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.handlers))
	for signalType := range r.handlers {
		types = append(types, signalType)
	}
	return types
}

// HandlerCount returns the number of handlers for a signal type
func (r *Registry) HandlerCount(signalType string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers[signalType])
}

// Manager coordinates signal handling across the system
type Manager struct {
	logger    *zap.Logger
	registry  *Registry
	publisher pubsub.Publisher
	handlers  []Handler

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
	running bool
}

// NewManager creates a new signal manager
func NewManager(logger *zap.Logger, publisher pubsub.Publisher) *Manager {
	return &Manager{
		logger:    logger.With(zap.String("component", "signal-manager")),
		registry:  NewRegistry(logger),
		publisher: publisher,
		handlers:  make([]Handler, 0),
	}
}

// AddHandler adds a signal handler to the manager
func (m *Manager) AddHandler(handler Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers = append(m.handlers, handler)
	m.logger.Info("added signal handler", zap.Stringer("handler", handler))
}

// RegisterSignalHandler registers a signal handler function
func (m *Manager) RegisterSignalHandler(signalType string, handler SignalHandlerFunc) {
	m.registry.Register(signalType, handler)
}

// Start starts the signal manager and all registered handlers
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("signal manager is already running")
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.running = true

	// Start all handlers
	for _, handler := range m.handlers {
		if err := handler.Start(m.ctx); err != nil {
			m.logger.Error("failed to start signal handler",
				zap.Stringer("handler", handler),
				zap.Error(err))
			// Continue starting other handlers
		}
	}

	m.logger.Info("signal manager started", zap.Int("handler_count", len(m.handlers)))
	return nil
}

// Stop stops the signal manager and all handlers
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	m.cancel()

	// Stop all handlers
	for _, handler := range m.handlers {
		if err := handler.Stop(ctx); err != nil {
			m.logger.Error("failed to stop signal handler",
				zap.Stringer("handler", handler),
				zap.Error(err))
		}
	}

	m.wg.Wait()
	m.running = false

	m.logger.Info("signal manager stopped")
	return nil
}

// SendSignal sends a signal through the manager
func (m *Manager) SendSignal(ctx context.Context, signal Signal) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.running {
		return fmt.Errorf("signal manager is not running")
	}

	// Process signal locally first
	if err := m.registry.Handle(ctx, signal); err != nil {
		m.logger.Error("error processing signal locally",
			zap.String("signal_type", signal.Type),
			zap.Error(err))
	}

	// Send signal to other instances via pubsub if publisher is available
	if m.publisher != nil {
		// Convert signal to pubsub message format
		// This will be handled by the specific handler implementations
		for _, handler := range m.handlers {
			if err := handler.SendSignal(ctx, signal); err != nil {
				m.logger.Error("error sending signal via handler",
					zap.Stringer("handler", handler),
					zap.String("signal_type", signal.Type),
					zap.Error(err))
			}
		}
	}

	return nil
}

// IsRunning returns whether the manager is running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// GetStats returns statistics about the signal manager
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"running":       m.running,
		"handler_count": len(m.handlers),
		"signal_types":  m.registry.GetSignalTypes(),
	}

	handlerStats := make(map[string]interface{})
	for _, signalType := range m.registry.GetSignalTypes() {
		handlerStats[signalType] = m.registry.HandlerCount(signalType)
	}
	stats["handlers_by_type"] = handlerStats

	return stats
}
