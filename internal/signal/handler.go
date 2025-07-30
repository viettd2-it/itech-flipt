package signal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"go.flipt.io/flipt/internal/cache"
	"go.flipt.io/flipt/internal/pubsub"
	"go.uber.org/zap"
)

// BaseHandler provides a base implementation for signal handlers
type BaseHandler struct {
	logger    *zap.Logger
	registry  *Registry
	publisher pubsub.Publisher

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
	running bool
	name    string
}

// NewBaseHandler creates a new base handler
func NewBaseHandler(name string, logger *zap.Logger, publisher pubsub.Publisher) *BaseHandler {
	return &BaseHandler{
		name:      name,
		logger:    logger.With(zap.String("handler", name)),
		registry:  NewRegistry(logger),
		publisher: publisher,
	}
}

// Start starts the base handler
func (h *BaseHandler) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.running {
		return fmt.Errorf("handler %s is already running", h.name)
	}

	h.ctx, h.cancel = context.WithCancel(ctx)
	h.running = true

	h.logger.Info("signal handler started")
	return nil
}

// Stop stops the base handler
func (h *BaseHandler) Stop(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.running {
		return nil
	}

	h.cancel()
	h.wg.Wait()
	h.running = false

	h.logger.Info("signal handler stopped")
	return nil
}

// RegisterSignalHandler registers a signal handler
func (h *BaseHandler) RegisterSignalHandler(signalType string, handler SignalHandlerFunc) error {
	h.registry.Register(signalType, handler)
	return nil
}

// SendSignal sends a signal (base implementation does nothing)
func (h *BaseHandler) SendSignal(ctx context.Context, signal Signal) error {
	// Base implementation - override in specific handlers
	return nil
}

// IsRunning returns whether the handler is running
func (h *BaseHandler) IsRunning() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.running
}

// String returns the handler name
func (h *BaseHandler) String() string {
	return h.name
}

// CacheHandler handles cache-related signals
type CacheHandler struct {
	*BaseHandler
	cache cache.Cacher
}

// NewCacheHandler creates a new cache signal handler
func NewCacheHandler(logger *zap.Logger, publisher pubsub.Publisher, cacher cache.Cacher) *CacheHandler {
	handler := &CacheHandler{
		BaseHandler: NewBaseHandler("cache-handler", logger, publisher),
		cache:       cacher,
	}

	// Register cache invalidation handler
	handler.RegisterSignalHandler(TypeCacheInvalidation, handler.handleCacheInvalidation)

	return handler
}

// handleCacheInvalidation handles cache invalidation signals
func (h *CacheHandler) handleCacheInvalidation(ctx context.Context, signal Signal) error {
	var data CacheInvalidationData
	if signal.Data != nil {
		dataBytes, err := json.Marshal(signal.Data)
		if err != nil {
			return fmt.Errorf("marshaling cache invalidation data: %w", err)
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return fmt.Errorf("unmarshaling cache invalidation data: %w", err)
		}
	}

	h.logger.Info("processing cache invalidation signal",
		zap.Strings("keys", data.Keys),
		zap.String("pattern", data.Pattern),
		zap.String("namespace", data.Namespace),
		zap.Bool("all", data.All))

	if h.cache == nil {
		h.logger.Warn("no cache configured, ignoring cache invalidation signal")
		return nil
	}

	// Handle different invalidation types
	if data.All {
		// For memory cache, we can't clear all, so we'll log a warning
		h.logger.Warn("cache invalidation 'all' not supported for current cache type")
		return nil
	}

	// Invalidate specific keys
	for _, key := range data.Keys {
		if err := h.cache.Delete(ctx, key); err != nil {
			h.logger.Error("failed to delete cache key",
				zap.String("key", key),
				zap.Error(err))
		} else {
			h.logger.Debug("cache key invalidated", zap.String("key", key))
		}
	}

	// Pattern-based invalidation is not directly supported by the current cache interface
	// This would need to be implemented based on the specific cache backend
	if data.Pattern != "" {
		h.logger.Warn("pattern-based cache invalidation not implemented",
			zap.String("pattern", data.Pattern))
	}

	return nil
}

// SendCacheInvalidation sends a cache invalidation signal
func (h *CacheHandler) SendCacheInvalidation(ctx context.Context, data CacheInvalidationData) error {
	signal := NewSignal(TypeCacheInvalidation, data).WithSource("cache-handler")
	return h.SendSignal(ctx, signal)
}

// ConfigHandler handles configuration-related signals
type ConfigHandler struct {
	*BaseHandler
	reloadFunc func(ctx context.Context) error
}

// NewConfigHandler creates a new config signal handler
func NewConfigHandler(logger *zap.Logger, publisher pubsub.Publisher, reloadFunc func(ctx context.Context) error) *ConfigHandler {
	handler := &ConfigHandler{
		BaseHandler: NewBaseHandler("config-handler", logger, publisher),
		reloadFunc:  reloadFunc,
	}

	// Register config reload handler
	handler.RegisterSignalHandler(TypeConfigReload, handler.handleConfigReload)

	return handler
}

// handleConfigReload handles configuration reload signals
func (h *ConfigHandler) handleConfigReload(ctx context.Context, signal Signal) error {
	var data ConfigReloadData
	if signal.Data != nil {
		dataBytes, err := json.Marshal(signal.Data)
		if err != nil {
			return fmt.Errorf("marshaling config reload data: %w", err)
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return fmt.Errorf("unmarshaling config reload data: %w", err)
		}
	}

	h.logger.Info("processing config reload signal",
		zap.String("config_path", data.ConfigPath),
		zap.Bool("force", data.Force))

	if h.reloadFunc != nil {
		if err := h.reloadFunc(ctx); err != nil {
			h.logger.Error("failed to reload configuration", zap.Error(err))
			return fmt.Errorf("reloading configuration: %w", err)
		}
		h.logger.Info("configuration reloaded successfully")
	} else {
		h.logger.Warn("no config reload function configured")
	}

	return nil
}

// SendConfigReload sends a configuration reload signal
func (h *ConfigHandler) SendConfigReload(ctx context.Context, data ConfigReloadData) error {
	signal := NewSignal(TypeConfigReload, data).WithSource("config-handler")
	return h.SendSignal(ctx, signal)
}

// FeatureFlagHandler handles feature flag-related signals
type FeatureFlagHandler struct {
	*BaseHandler
	updateFunc func(ctx context.Context, data FeatureFlagUpdateData) error
}

// NewFeatureFlagHandler creates a new feature flag signal handler
func NewFeatureFlagHandler(logger *zap.Logger, publisher pubsub.Publisher, updateFunc func(ctx context.Context, data FeatureFlagUpdateData) error) *FeatureFlagHandler {
	handler := &FeatureFlagHandler{
		BaseHandler: NewBaseHandler("flag-handler", logger, publisher),
		updateFunc:  updateFunc,
	}

	// Register feature flag update handler
	handler.RegisterSignalHandler(TypeFeatureFlagUpdate, handler.handleFeatureFlagUpdate)

	return handler
}

// handleFeatureFlagUpdate handles feature flag update signals
func (h *FeatureFlagHandler) handleFeatureFlagUpdate(ctx context.Context, signal Signal) error {
	var data FeatureFlagUpdateData
	if signal.Data != nil {
		dataBytes, err := json.Marshal(signal.Data)
		if err != nil {
			return fmt.Errorf("marshaling feature flag update data: %w", err)
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return fmt.Errorf("unmarshaling feature flag update data: %w", err)
		}
	}

	h.logger.Info("processing feature flag update signal",
		zap.String("flag_key", data.FlagKey),
		zap.String("namespace", data.Namespace),
		zap.String("action", data.Action))

	if h.updateFunc != nil {
		if err := h.updateFunc(ctx, data); err != nil {
			h.logger.Error("failed to process feature flag update", zap.Error(err))
			return fmt.Errorf("processing feature flag update: %w", err)
		}
		h.logger.Debug("feature flag update processed successfully")
	} else {
		h.logger.Debug("no feature flag update function configured")
	}

	return nil
}

// SendFeatureFlagUpdate sends a feature flag update signal
func (h *FeatureFlagHandler) SendFeatureFlagUpdate(ctx context.Context, data FeatureFlagUpdateData) error {
	signal := NewSignal(TypeFeatureFlagUpdate, data).WithSource("flag-handler")
	return h.SendSignal(ctx, signal)
}

// HealthHandler handles health-related signals
type HealthHandler struct {
	*BaseHandler
	healthFunc func(ctx context.Context, data HealthCheckData) error
}

// NewHealthHandler creates a new health signal handler
func NewHealthHandler(logger *zap.Logger, publisher pubsub.Publisher, healthFunc func(ctx context.Context, data HealthCheckData) error) *HealthHandler {
	handler := &HealthHandler{
		BaseHandler: NewBaseHandler("health-handler", logger, publisher),
		healthFunc:  healthFunc,
	}

	// Register health check handler
	handler.RegisterSignalHandler(TypeHealthCheck, handler.handleHealthCheck)

	return handler
}

// handleHealthCheck handles health check signals
func (h *HealthHandler) handleHealthCheck(ctx context.Context, signal Signal) error {
	var data HealthCheckData
	if signal.Data != nil {
		dataBytes, err := json.Marshal(signal.Data)
		if err != nil {
			return fmt.Errorf("marshaling health check data: %w", err)
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return fmt.Errorf("unmarshaling health check data: %w", err)
		}
	}

	h.logger.Debug("processing health check signal",
		zap.String("status", data.Status),
		zap.String("component", data.Component))

	if h.healthFunc != nil {
		if err := h.healthFunc(ctx, data); err != nil {
			h.logger.Error("failed to process health check", zap.Error(err))
			return fmt.Errorf("processing health check: %w", err)
		}
	}

	return nil
}

// SendHealthCheck sends a health check signal
func (h *HealthHandler) SendHealthCheck(ctx context.Context, data HealthCheckData) error {
	signal := NewSignal(TypeHealthCheck, data).WithSource("health-handler")
	return h.SendSignal(ctx, signal)
}
