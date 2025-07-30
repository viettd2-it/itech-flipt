package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.flipt.io/flipt/internal/config"
	"go.flipt.io/flipt/internal/pubsub"
	"go.uber.org/zap"
)

const pubsubType = "redis"

// Client implements both Publisher and Subscriber interfaces for Redis
type Client struct {
	logger *zap.Logger
	client goredis.UniversalClient
	pubsub *goredis.PubSub
	cfg    config.RedisCacheConfig

	// Subscription management
	mu         sync.RWMutex
	subscribed map[string]bool
	msgChan    chan pubsub.Message
	ctx        context.Context
	cancel     context.CancelFunc
	closed     bool
}

// NewClient creates a new Redis pubsub client
func NewClient(logger *zap.Logger, cfg config.RedisCacheConfig) (*Client, error) {
	// Reuse the existing Redis client creation logic from cache
	client, err := newRedisClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating redis client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		logger:     logger.With(zap.String("component", "redis-pubsub")),
		client:     client,
		cfg:        cfg,
		subscribed: make(map[string]bool),
		msgChan:    make(chan pubsub.Message, 100), // Buffered channel
		ctx:        ctx,
		cancel:     cancel,
	}

	return c, nil
}

// newRedisClient creates a Redis client using the same logic as the cache implementation
func newRedisClient(cfg config.RedisCacheConfig) (goredis.UniversalClient, error) {
	switch cfg.Mode {
	case config.RedisCacheModeSingle:
		return goredis.NewClient(&goredis.Options{
			Addr:            fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
			Username:        cfg.Username,
			Password:        cfg.Password,
			DB:              cfg.DB,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConn,
			ConnMaxIdleTime: cfg.ConnMaxIdleTime,
			DialTimeout:     cfg.NetTimeout,
			ReadTimeout:     cfg.NetTimeout * 2,
			WriteTimeout:    cfg.NetTimeout * 2,
			PoolTimeout:     cfg.NetTimeout * 2,
		}), nil
	case config.RedisCacheModeCluster:
		return goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:           []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
			Username:        cfg.Username,
			Password:        cfg.Password,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConn,
			ConnMaxIdleTime: cfg.ConnMaxIdleTime,
			DialTimeout:     cfg.NetTimeout,
			ReadTimeout:     cfg.NetTimeout * 2,
			WriteTimeout:    cfg.NetTimeout * 2,
			PoolTimeout:     cfg.NetTimeout * 2,
		}), nil
	default:
		return nil, fmt.Errorf("invalid redis mode: %s", cfg.Mode)
	}
}

// Publish implements the Publisher interface
func (c *Client) Publish(ctx context.Context, channel string, message []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return pubsub.ErrNotConnected
	}

	result := c.client.Publish(ctx, channel, message)
	if err := result.Err(); err != nil {
		c.logger.Error("failed to publish message",
			zap.String("channel", channel),
			zap.Error(err))
		return fmt.Errorf("publishing to channel %s: %w", channel, err)
	}

	subscribers := result.Val()
	c.logger.Debug("message published",
		zap.String("channel", channel),
		zap.Int64("subscribers", subscribers),
		zap.Int("message_size", len(message)))

	return nil
}

// Subscribe implements the Subscriber interface
func (c *Client) Subscribe(ctx context.Context, channels ...string) (<-chan pubsub.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, pubsub.ErrNotConnected
	}

	// Initialize pubsub if not already done
	if c.pubsub == nil {
		c.pubsub = c.client.Subscribe(ctx, channels...)
		go c.listenForMessages()
	} else {
		// Subscribe to additional channels
		if err := c.pubsub.Subscribe(ctx, channels...); err != nil {
			return nil, fmt.Errorf("subscribing to channels: %w", err)
		}
	}

	// Mark channels as subscribed
	for _, channel := range channels {
		c.subscribed[channel] = true
		c.logger.Info("subscribed to channel", zap.String("channel", channel))
	}

	return c.msgChan, nil
}

// Unsubscribe implements the Subscriber interface
func (c *Client) Unsubscribe(ctx context.Context, channels ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.pubsub == nil {
		return pubsub.ErrNotConnected
	}

	if err := c.pubsub.Unsubscribe(ctx, channels...); err != nil {
		return fmt.Errorf("unsubscribing from channels: %w", err)
	}

	// Mark channels as unsubscribed
	for _, channel := range channels {
		delete(c.subscribed, channel)
		c.logger.Info("unsubscribed from channel", zap.String("channel", channel))
	}

	return nil
}

// listenForMessages runs in a goroutine to listen for Redis pubsub messages
func (c *Client) listenForMessages() {
	defer close(c.msgChan)

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Debug("stopping message listener")
			return
		default:
			msg, err := c.pubsub.ReceiveMessage(c.ctx)
			if err != nil {
				if c.ctx.Err() != nil {
					// Context cancelled, normal shutdown
					return
				}
				c.logger.Error("error receiving message", zap.Error(err))
				// Add a small delay to prevent tight loop on persistent errors
				time.Sleep(100 * time.Millisecond)
				continue
			}

			pubsubMsg := pubsub.Message{
				Channel: msg.Channel,
				Payload: []byte(msg.Payload),
				Metadata: map[string]interface{}{
					"pattern":     msg.Pattern,
					"received_at": time.Now(),
				},
			}

			select {
			case c.msgChan <- pubsubMsg:
				c.logger.Debug("message received",
					zap.String("channel", msg.Channel),
					zap.Int("payload_size", len(msg.Payload)))
			case <-c.ctx.Done():
				return
			default:
				c.logger.Warn("message channel full, dropping message",
					zap.String("channel", msg.Channel))
			}
		}
	}
}

// Close implements both Publisher and Subscriber interfaces
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.cancel()

	var err error
	if c.pubsub != nil {
		if closeErr := c.pubsub.Close(); closeErr != nil {
			err = closeErr
		}
	}

	if closeErr := c.client.Close(); closeErr != nil {
		if err != nil {
			err = fmt.Errorf("multiple close errors: %v; %v", err, closeErr)
		} else {
			err = closeErr
		}
	}

	c.logger.Info("redis pubsub client closed")
	return err
}

// String implements the fmt.Stringer interface
func (c *Client) String() string {
	return pubsubType
}

// GetSubscribedChannels returns the list of currently subscribed channels
func (c *Client) GetSubscribedChannels() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	channels := make([]string, 0, len(c.subscribed))
	for channel := range c.subscribed {
		channels = append(channels, channel)
	}
	return channels
}

// IsSubscribed checks if the client is subscribed to a specific channel
func (c *Client) IsSubscribed(channel string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.subscribed[channel]
}
