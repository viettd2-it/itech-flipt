package pubsub

import (
	"context"
	"fmt"
)

// Publisher defines the interface for publishing messages to a pubsub system
type Publisher interface {
	// Publish sends a message to the specified channel
	Publish(ctx context.Context, channel string, message []byte) error
	// Close closes the publisher connection
	Close() error
	fmt.Stringer
}

// Subscriber defines the interface for subscribing to messages from a pubsub system
type Subscriber interface {
	// Subscribe subscribes to one or more channels and returns a channel for receiving messages
	Subscribe(ctx context.Context, channels ...string) (<-chan Message, error)
	// Unsubscribe unsubscribes from one or more channels
	Unsubscribe(ctx context.Context, channels ...string) error
	// Close closes the subscriber connection
	Close() error
	fmt.Stringer
}

// PubSub combines both Publisher and Subscriber interfaces
type PubSub interface {
	Publisher
	Subscriber
}

// Message represents a message received from a pubsub channel
type Message struct {
	// Channel is the channel the message was received on
	Channel string
	// Payload is the message content
	Payload []byte
	// Metadata contains additional message information (optional)
	Metadata map[string]interface{}
}

// MessageHandler is a function type for handling received messages
type MessageHandler func(ctx context.Context, msg Message) error

// HandlerRegistry manages message handlers for different channels
type HandlerRegistry struct {
	handlers map[string][]MessageHandler
}

// NewHandlerRegistry creates a new handler registry
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string][]MessageHandler),
	}
}

// Register registers a handler for a specific channel
func (r *HandlerRegistry) Register(channel string, handler MessageHandler) {
	r.handlers[channel] = append(r.handlers[channel], handler)
}

// Handle processes a message by calling all registered handlers for its channel
func (r *HandlerRegistry) Handle(ctx context.Context, msg Message) error {
	handlers, exists := r.handlers[msg.Channel]
	if !exists {
		return fmt.Errorf("no handlers registered for channel: %s", msg.Channel)
	}

	for _, handler := range handlers {
		if err := handler(ctx, msg); err != nil {
			return fmt.Errorf("handler error for channel %s: %w", msg.Channel, err)
		}
	}

	return nil
}

// GetChannels returns all channels that have registered handlers
func (r *HandlerRegistry) GetChannels() []string {
	channels := make([]string, 0, len(r.handlers))
	for channel := range r.handlers {
		channels = append(channels, channel)
	}
	return channels
}

// PubSubConfig represents configuration for pubsub systems
type PubSubConfig struct {
	// Enabled indicates if pubsub is enabled
	Enabled bool
	// Backend specifies the pubsub backend (e.g., "redis")
	Backend string
	// Channels is a list of channels to subscribe to
	Channels []string
}

// Error types for pubsub operations
var (
	ErrNotConnected     = fmt.Errorf("pubsub: not connected")
	ErrAlreadyConnected = fmt.Errorf("pubsub: already connected")
	ErrChannelNotFound  = fmt.Errorf("pubsub: channel not found")
	ErrInvalidMessage   = fmt.Errorf("pubsub: invalid message")
)
