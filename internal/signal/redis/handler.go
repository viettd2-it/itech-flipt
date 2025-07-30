package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.flipt.io/flipt/internal/config"
	redispubsub "go.flipt.io/flipt/internal/pubsub/redis"
	"go.flipt.io/flipt/internal/signal"
	"go.uber.org/zap"
)

// Handler implements signal.Handler using Redis pubsub
type Handler struct {
	logger     *zap.Logger
	publisher  *redispubsub.Publisher
	subscriber *redispubsub.Subscriber
	registry   *signal.Registry

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
	running bool
}

// NewHandler creates a new Redis signal handler
func NewHandler(logger *zap.Logger, cfg config.RedisCacheConfig) (*Handler, error) {
	publisher, err := redispubsub.NewPublisher(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating redis publisher: %w", err)
	}

	subscriber, err := redispubsub.NewSubscriber(logger, cfg)
	if err != nil {
		publisher.Close()
		return nil, fmt.Errorf("creating redis subscriber: %w", err)
	}

	return &Handler{
		logger:     logger.With(zap.String("component", "redis-signal-handler")),
		publisher:  publisher,
		subscriber: subscriber,
		registry:   signal.NewRegistry(logger),
	}, nil
}

// Start starts the Redis signal handler
func (h *Handler) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.running {
		return fmt.Errorf("redis signal handler is already running")
	}

	h.ctx, h.cancel = context.WithCancel(ctx)
	h.running = true

	// Register signal handlers with the subscriber
	h.setupSignalHandlers()

	// Start processing messages
	if err := h.subscriber.StartProcessing(); err != nil {
		h.running = false
		h.cancel()
		return fmt.Errorf("starting subscriber processing: %w", err)
	}

	h.logger.Info("redis signal handler started")
	return nil
}

// Stop stops the Redis signal handler
func (h *Handler) Stop(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.running {
		return nil
	}

	h.cancel()

	// Stop subscriber processing
	if err := h.subscriber.StopProcessing(); err != nil {
		h.logger.Error("error stopping subscriber processing", zap.Error(err))
	}

	// Close connections
	if err := h.subscriber.Close(); err != nil {
		h.logger.Error("error closing subscriber", zap.Error(err))
	}

	if err := h.publisher.Close(); err != nil {
		h.logger.Error("error closing publisher", zap.Error(err))
	}

	h.wg.Wait()
	h.running = false

	h.logger.Info("redis signal handler stopped")
	return nil
}

// setupSignalHandlers sets up the Redis pubsub signal handlers
func (h *Handler) setupSignalHandlers() {
	// Cache invalidation signals
	h.subscriber.RegisterCacheInvalidationHandler(func(ctx context.Context, redisSignal redispubsub.CacheInvalidationSignal) error {
		sig := signal.Signal{
			Type: signal.TypeCacheInvalidation,
			Data: signal.CacheInvalidationData{
				Keys:      redisSignal.Keys,
				Pattern:   redisSignal.Pattern,
				Namespace: redisSignal.Namespace,
			},
			Timestamp: time.Now().UTC(),
			Source:    "redis",
		}
		return h.registry.Handle(ctx, sig)
	})

	// Config reload signals
	h.subscriber.RegisterConfigReloadHandler(func(ctx context.Context, redisSignal redispubsub.ConfigReloadSignal) error {
		sig := signal.Signal{
			Type: signal.TypeConfigReload,
			Data: signal.ConfigReloadData{
				ConfigPath: redisSignal.ConfigPath,
				Changes:    redisSignal.Changes,
			},
			Timestamp: time.Now().UTC(),
			Source:    "redis",
		}
		return h.registry.Handle(ctx, sig)
	})

	// Feature flag update signals
	h.subscriber.RegisterFeatureFlagUpdateHandler(func(ctx context.Context, redisSignal redispubsub.FeatureFlagUpdateSignal) error {
		sig := signal.Signal{
			Type: signal.TypeFeatureFlagUpdate,
			Data: signal.FeatureFlagUpdateData{
				FlagKey:   redisSignal.FlagKey,
				Namespace: redisSignal.Namespace,
				Action:    redisSignal.Action,
			},
			Timestamp: time.Now().UTC(),
			Source:    "redis",
		}
		return h.registry.Handle(ctx, sig)
	})

	// Health check signals
	h.subscriber.RegisterHealthCheckHandler(func(ctx context.Context, redisSignal redispubsub.HealthCheckSignal) error {
		sig := signal.Signal{
			Type: signal.TypeHealthCheck,
			Data: signal.HealthCheckData{
				Status:    redisSignal.Status,
				Component: redisSignal.Component,
				Details:   redisSignal.Details,
			},
			Timestamp: time.Now().UTC(),
			Source:    "redis",
		}
		return h.registry.Handle(ctx, sig)
	})

	// Custom signals
	h.subscriber.RegisterCustomSignalHandler("", func(ctx context.Context, redisSignal redispubsub.SignalMessage) error {
		sig := signal.Signal{
			Type:      redisSignal.Type,
			Data:      redisSignal.Data,
			Timestamp: redisSignal.Timestamp,
			Source:    redisSignal.Source,
			Metadata:  redisSignal.Metadata,
		}
		return h.registry.Handle(ctx, sig)
	})
}

// RegisterSignalHandler registers a signal handler
func (h *Handler) RegisterSignalHandler(signalType string, handler signal.SignalHandlerFunc) error {
	h.registry.Register(signalType, handler)
	return nil
}

// SendSignal sends a signal via Redis pubsub
func (h *Handler) SendSignal(ctx context.Context, sig signal.Signal) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if !h.running {
		return fmt.Errorf("redis signal handler is not running")
	}

	// Convert signal to appropriate Redis pubsub signal type and send
	switch sig.Type {
	case signal.TypeCacheInvalidation:
		return h.sendCacheInvalidationSignal(ctx, sig)
	case signal.TypeConfigReload:
		return h.sendConfigReloadSignal(ctx, sig)
	case signal.TypeFeatureFlagUpdate:
		return h.sendFeatureFlagUpdateSignal(ctx, sig)
	case signal.TypeHealthCheck:
		return h.sendHealthCheckSignal(ctx, sig)
	default:
		return h.sendCustomSignal(ctx, sig)
	}
}

// sendCacheInvalidationSignal sends a cache invalidation signal
func (h *Handler) sendCacheInvalidationSignal(ctx context.Context, sig signal.Signal) error {
	var data signal.CacheInvalidationData
	if sig.Data != nil {
		dataBytes, err := json.Marshal(sig.Data)
		if err != nil {
			return fmt.Errorf("marshaling cache invalidation data: %w", err)
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return fmt.Errorf("unmarshaling cache invalidation data: %w", err)
		}
	}

	redisSignal := redispubsub.CacheInvalidationSignal{
		Keys:      data.Keys,
		Pattern:   data.Pattern,
		Namespace: data.Namespace,
	}

	return h.publisher.PublishCacheInvalidation(ctx, redisSignal)
}

// sendConfigReloadSignal sends a config reload signal
func (h *Handler) sendConfigReloadSignal(ctx context.Context, sig signal.Signal) error {
	var data signal.ConfigReloadData
	if sig.Data != nil {
		dataBytes, err := json.Marshal(sig.Data)
		if err != nil {
			return fmt.Errorf("marshaling config reload data: %w", err)
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return fmt.Errorf("unmarshaling config reload data: %w", err)
		}
	}

	redisSignal := redispubsub.ConfigReloadSignal{
		ConfigPath: data.ConfigPath,
		Changes:    data.Changes,
	}

	return h.publisher.PublishConfigReload(ctx, redisSignal)
}

// sendFeatureFlagUpdateSignal sends a feature flag update signal
func (h *Handler) sendFeatureFlagUpdateSignal(ctx context.Context, sig signal.Signal) error {
	var data signal.FeatureFlagUpdateData
	if sig.Data != nil {
		dataBytes, err := json.Marshal(sig.Data)
		if err != nil {
			return fmt.Errorf("marshaling feature flag update data: %w", err)
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return fmt.Errorf("unmarshaling feature flag update data: %w", err)
		}
	}

	redisSignal := redispubsub.FeatureFlagUpdateSignal{
		FlagKey:   data.FlagKey,
		Namespace: data.Namespace,
		Action:    data.Action,
	}

	return h.publisher.PublishFeatureFlagUpdate(ctx, redisSignal)
}

// sendHealthCheckSignal sends a health check signal
func (h *Handler) sendHealthCheckSignal(ctx context.Context, sig signal.Signal) error {
	var data signal.HealthCheckData
	if sig.Data != nil {
		dataBytes, err := json.Marshal(sig.Data)
		if err != nil {
			return fmt.Errorf("marshaling health check data: %w", err)
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return fmt.Errorf("unmarshaling health check data: %w", err)
		}
	}

	redisSignal := redispubsub.HealthCheckSignal{
		Status:    data.Status,
		Component: data.Component,
		Details:   data.Details,
	}

	return h.publisher.PublishHealthCheck(ctx, redisSignal)
}

// sendCustomSignal sends a custom signal
func (h *Handler) sendCustomSignal(ctx context.Context, sig signal.Signal) error {
	return h.publisher.PublishCustomSignal(ctx, sig.Type, sig.Data)
}

// IsRunning returns whether the handler is running
func (h *Handler) IsRunning() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.running
}

// String returns the handler name
func (h *Handler) String() string {
	return "redis-signal-handler"
}

// GetStats returns statistics about the Redis signal handler
func (h *Handler) GetStats() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return map[string]interface{}{
		"running":             h.running,
		"subscribed_channels": h.subscriber.GetSubscribedChannels(),
		"processing":          h.subscriber.IsProcessing(),
		"signal_types":        h.registry.GetSignalTypes(),
	}
}
