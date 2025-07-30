package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.flipt.io/flipt/internal/config"
	"go.flipt.io/flipt/internal/pubsub"
	"go.uber.org/zap"
)

// Subscriber is a Redis-specific subscriber implementation
type Subscriber struct {
	client   *Client
	logger   *zap.Logger
	registry *pubsub.HandlerRegistry

	// Processing control
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	processing bool
	mu         sync.RWMutex
}

// NewSubscriber creates a new Redis subscriber
func NewSubscriber(logger *zap.Logger, cfg config.RedisCacheConfig) (*Subscriber, error) {
	client, err := NewClient(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating redis client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Subscriber{
		client:   client,
		logger:   logger.With(zap.String("component", "redis-subscriber")),
		registry: pubsub.NewHandlerRegistry(),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Subscribe subscribes to channels and returns a message channel
func (s *Subscriber) Subscribe(ctx context.Context, channels ...string) (<-chan pubsub.Message, error) {
	return s.client.Subscribe(ctx, channels...)
}

// Unsubscribe unsubscribes from channels
func (s *Subscriber) Unsubscribe(ctx context.Context, channels ...string) error {
	return s.client.Unsubscribe(ctx, channels...)
}

// RegisterHandler registers a message handler for a specific channel
func (s *Subscriber) RegisterHandler(channel string, handler pubsub.MessageHandler) {
	s.registry.Register(channel, handler)
	s.logger.Info("registered handler for channel", zap.String("channel", channel))
}

// RegisterSignalHandler registers a handler for a specific signal type
func (s *Subscriber) RegisterSignalHandler(channel string, signalType string, handler SignalHandler) {
	s.RegisterHandler(channel, func(ctx context.Context, msg pubsub.Message) error {
		var signal SignalMessage
		if err := json.Unmarshal(msg.Payload, &signal); err != nil {
			return fmt.Errorf("unmarshaling signal message: %w", err)
		}

		if signal.Type != signalType {
			// Ignore signals that don't match the expected type
			return nil
		}

		return handler(ctx, signal)
	})
}

// SignalHandler is a function type for handling signal messages
type SignalHandler func(ctx context.Context, signal SignalMessage) error

// StartProcessing starts processing messages from subscribed channels
func (s *Subscriber) StartProcessing() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.processing {
		return fmt.Errorf("subscriber is already processing")
	}

	channels := s.registry.GetChannels()
	if len(channels) == 0 {
		return fmt.Errorf("no channels registered for processing")
	}

	msgChan, err := s.Subscribe(s.ctx, channels...)
	if err != nil {
		return fmt.Errorf("subscribing to channels: %w", err)
	}

	s.processing = true
	s.wg.Add(1)

	go s.processMessages(msgChan)

	s.logger.Info("started processing messages", zap.Strings("channels", channels))
	return nil
}

// StopProcessing stops processing messages
func (s *Subscriber) StopProcessing() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.processing {
		return nil
	}

	s.cancel()
	s.wg.Wait()
	s.processing = false

	s.logger.Info("stopped processing messages")
	return nil
}

// processMessages processes incoming messages in a separate goroutine
func (s *Subscriber) processMessages(msgChan <-chan pubsub.Message) {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Debug("message processing stopped")
			return
		case msg, ok := <-msgChan:
			if !ok {
				s.logger.Debug("message channel closed")
				return
			}

			if err := s.handleMessage(msg); err != nil {
				s.logger.Error("error handling message",
					zap.String("channel", msg.Channel),
					zap.Error(err))
			}
		}
	}
}

// handleMessage handles a single message
func (s *Subscriber) handleMessage(msg pubsub.Message) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		s.logger.Debug("message processed",
			zap.String("channel", msg.Channel),
			zap.Duration("duration", duration))
	}()

	return s.registry.Handle(s.ctx, msg)
}

// Close closes the subscriber
func (s *Subscriber) Close() error {
	if err := s.StopProcessing(); err != nil {
		s.logger.Error("error stopping processing", zap.Error(err))
	}
	return s.client.Close()
}

// String returns the subscriber type
func (s *Subscriber) String() string {
	return "redis-subscriber"
}

// GetSubscribedChannels returns the list of subscribed channels
func (s *Subscriber) GetSubscribedChannels() []string {
	return s.client.GetSubscribedChannels()
}

// IsProcessing returns whether the subscriber is currently processing messages
func (s *Subscriber) IsProcessing() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.processing
}

// RegisterCacheInvalidationHandler registers a handler for cache invalidation signals
func (s *Subscriber) RegisterCacheInvalidationHandler(handler func(ctx context.Context, signal CacheInvalidationSignal) error) {
	s.RegisterSignalHandler(ChannelCacheInvalidation, SignalTypeCacheInvalidation, func(ctx context.Context, signal SignalMessage) error {
		var cacheSignal CacheInvalidationSignal
		if signal.Data != nil {
			data, err := json.Marshal(signal.Data)
			if err != nil {
				return fmt.Errorf("marshaling cache invalidation data: %w", err)
			}
			if err := json.Unmarshal(data, &cacheSignal); err != nil {
				return fmt.Errorf("unmarshaling cache invalidation signal: %w", err)
			}
		}
		return handler(ctx, cacheSignal)
	})
}

// RegisterConfigReloadHandler registers a handler for config reload signals
func (s *Subscriber) RegisterConfigReloadHandler(handler func(ctx context.Context, signal ConfigReloadSignal) error) {
	s.RegisterSignalHandler(ChannelConfigReload, SignalTypeConfigReload, func(ctx context.Context, signal SignalMessage) error {
		var configSignal ConfigReloadSignal
		if signal.Data != nil {
			data, err := json.Marshal(signal.Data)
			if err != nil {
				return fmt.Errorf("marshaling config reload data: %w", err)
			}
			if err := json.Unmarshal(data, &configSignal); err != nil {
				return fmt.Errorf("unmarshaling config reload signal: %w", err)
			}
		}
		return handler(ctx, configSignal)
	})
}

// RegisterFeatureFlagUpdateHandler registers a handler for feature flag update signals
func (s *Subscriber) RegisterFeatureFlagUpdateHandler(handler func(ctx context.Context, signal FeatureFlagUpdateSignal) error) {
	s.RegisterSignalHandler(ChannelFeatureFlagUpdate, SignalTypeFeatureFlagUpdate, func(ctx context.Context, signal SignalMessage) error {
		var flagSignal FeatureFlagUpdateSignal
		if signal.Data != nil {
			data, err := json.Marshal(signal.Data)
			if err != nil {
				return fmt.Errorf("marshaling feature flag update data: %w", err)
			}
			if err := json.Unmarshal(data, &flagSignal); err != nil {
				return fmt.Errorf("unmarshaling feature flag update signal: %w", err)
			}
		}
		return handler(ctx, flagSignal)
	})
}

// RegisterHealthCheckHandler registers a handler for health check signals
func (s *Subscriber) RegisterHealthCheckHandler(handler func(ctx context.Context, signal HealthCheckSignal) error) {
	s.RegisterSignalHandler(ChannelHealthCheck, SignalTypeHealthCheck, func(ctx context.Context, signal SignalMessage) error {
		var healthSignal HealthCheckSignal
		if signal.Data != nil {
			data, err := json.Marshal(signal.Data)
			if err != nil {
				return fmt.Errorf("marshaling health check data: %w", err)
			}
			if err := json.Unmarshal(data, &healthSignal); err != nil {
				return fmt.Errorf("unmarshaling health check signal: %w", err)
			}
		}
		return handler(ctx, healthSignal)
	})
}

// RegisterCustomSignalHandler registers a handler for custom signals
func (s *Subscriber) RegisterCustomSignalHandler(signalType string, handler func(ctx context.Context, signal SignalMessage) error) {
	s.RegisterSignalHandler(ChannelCustomSignals, signalType, handler)
}
