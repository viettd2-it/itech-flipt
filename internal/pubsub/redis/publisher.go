package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.flipt.io/flipt/internal/config"
	"go.uber.org/zap"
)

// Publisher is a Redis-specific publisher implementation
type Publisher struct {
	client *Client
	logger *zap.Logger
}

// NewPublisher creates a new Redis publisher
func NewPublisher(logger *zap.Logger, cfg config.RedisCacheConfig) (*Publisher, error) {
	client, err := NewClient(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating redis client: %w", err)
	}

	return &Publisher{
		client: client,
		logger: logger.With(zap.String("component", "redis-publisher")),
	}, nil
}

// Publish publishes a message to the specified channel
func (p *Publisher) Publish(ctx context.Context, channel string, message []byte) error {
	return p.client.Publish(ctx, channel, message)
}

// PublishJSON publishes a JSON-encoded message to the specified channel
func (p *Publisher) PublishJSON(ctx context.Context, channel string, data interface{}) error {
	message, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshaling message to JSON: %w", err)
	}

	return p.Publish(ctx, channel, message)
}

// PublishSignal publishes a signal message with metadata
func (p *Publisher) PublishSignal(ctx context.Context, channel string, signalType string, data interface{}) error {
	signal := SignalMessage{
		Type:      signalType,
		Data:      data,
		Timestamp: time.Now().UTC(),
		Source:    "flipt",
	}

	return p.PublishJSON(ctx, channel, signal)
}

// Close closes the publisher
func (p *Publisher) Close() error {
	return p.client.Close()
}

// String returns the publisher type
func (p *Publisher) String() string {
	return "redis-publisher"
}

// SignalMessage represents a structured signal message
type SignalMessage struct {
	Type      string                 `json:"type"`
	Data      interface{}            `json:"data,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Source    string                 `json:"source"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// CacheInvalidationSignal represents a cache invalidation signal
type CacheInvalidationSignal struct {
	Keys      []string `json:"keys,omitempty"`
	Pattern   string   `json:"pattern,omitempty"`
	Namespace string   `json:"namespace,omitempty"`
}

// ConfigReloadSignal represents a configuration reload signal
type ConfigReloadSignal struct {
	ConfigPath string                 `json:"config_path,omitempty"`
	Changes    map[string]interface{} `json:"changes,omitempty"`
}

// FeatureFlagUpdateSignal represents a feature flag update signal
type FeatureFlagUpdateSignal struct {
	FlagKey   string `json:"flag_key"`
	Namespace string `json:"namespace,omitempty"`
	Action    string `json:"action"` // created, updated, deleted
}

// HealthCheckSignal represents a health check signal
type HealthCheckSignal struct {
	Status    string                 `json:"status"` // healthy, unhealthy, degraded
	Component string                 `json:"component"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// Signal type constants
const (
	SignalTypeCacheInvalidation = "cache.invalidation"
	SignalTypeConfigReload      = "config.reload"
	SignalTypeFeatureFlagUpdate = "flag.update"
	SignalTypeHealthCheck       = "health.check"
	SignalTypeCustom            = "custom"
)

// Channel constants for common signal types
const (
	ChannelCacheInvalidation = "flipt:cache:invalidation"
	ChannelConfigReload      = "flipt:config:reload"
	ChannelFeatureFlagUpdate = "flipt:flags:update"
	ChannelHealthCheck       = "flipt:health:check"
	ChannelCustomSignals     = "flipt:signals:custom"
)

// PublishCacheInvalidation publishes a cache invalidation signal
func (p *Publisher) PublishCacheInvalidation(ctx context.Context, signal CacheInvalidationSignal) error {
	p.logger.Info("publishing cache invalidation signal",
		zap.Strings("keys", signal.Keys),
		zap.String("pattern", signal.Pattern),
		zap.String("namespace", signal.Namespace))

	return p.PublishSignal(ctx, ChannelCacheInvalidation, SignalTypeCacheInvalidation, signal)
}

// PublishConfigReload publishes a configuration reload signal
func (p *Publisher) PublishConfigReload(ctx context.Context, signal ConfigReloadSignal) error {
	p.logger.Info("publishing config reload signal",
		zap.String("config_path", signal.ConfigPath))

	return p.PublishSignal(ctx, ChannelConfigReload, SignalTypeConfigReload, signal)
}

// PublishFeatureFlagUpdate publishes a feature flag update signal
func (p *Publisher) PublishFeatureFlagUpdate(ctx context.Context, signal FeatureFlagUpdateSignal) error {
	p.logger.Info("publishing feature flag update signal",
		zap.String("flag_key", signal.FlagKey),
		zap.String("namespace", signal.Namespace),
		zap.String("action", signal.Action))

	return p.PublishSignal(ctx, ChannelFeatureFlagUpdate, SignalTypeFeatureFlagUpdate, signal)
}

// PublishHealthCheck publishes a health check signal
func (p *Publisher) PublishHealthCheck(ctx context.Context, signal HealthCheckSignal) error {
	p.logger.Debug("publishing health check signal",
		zap.String("status", signal.Status),
		zap.String("component", signal.Component))

	return p.PublishSignal(ctx, ChannelHealthCheck, SignalTypeHealthCheck, signal)
}

// PublishCustomSignal publishes a custom signal
func (p *Publisher) PublishCustomSignal(ctx context.Context, signalType string, data interface{}) error {
	p.logger.Info("publishing custom signal",
		zap.String("signal_type", signalType))

	return p.PublishSignal(ctx, ChannelCustomSignals, signalType, data)
}

// BatchPublisher allows publishing multiple messages in a batch
type BatchPublisher struct {
	publisher *Publisher
	messages  []batchMessage
}

type batchMessage struct {
	channel string
	message []byte
}

// NewBatchPublisher creates a new batch publisher
func (p *Publisher) NewBatchPublisher() *BatchPublisher {
	return &BatchPublisher{
		publisher: p,
		messages:  make([]batchMessage, 0),
	}
}

// Add adds a message to the batch
func (bp *BatchPublisher) Add(channel string, message []byte) {
	bp.messages = append(bp.messages, batchMessage{
		channel: channel,
		message: message,
	})
}

// AddJSON adds a JSON message to the batch
func (bp *BatchPublisher) AddJSON(channel string, data interface{}) error {
	message, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshaling message to JSON: %w", err)
	}
	bp.Add(channel, message)
	return nil
}

// Publish publishes all messages in the batch
func (bp *BatchPublisher) Publish(ctx context.Context) error {
	for _, msg := range bp.messages {
		if err := bp.publisher.Publish(ctx, msg.channel, msg.message); err != nil {
			return fmt.Errorf("publishing batch message to channel %s: %w", msg.channel, err)
		}
	}

	bp.publisher.logger.Debug("batch published", zap.Int("message_count", len(bp.messages)))
	bp.messages = bp.messages[:0] // Clear the batch
	return nil
}

// Size returns the number of messages in the batch
func (bp *BatchPublisher) Size() int {
	return len(bp.messages)
}
