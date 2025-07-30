package config

import (
	"encoding/json"
	"time"

	"github.com/spf13/viper"
)

var (
	_ defaulter = (*PubSubConfig)(nil)
	_ validator = (*PubSubConfig)(nil)
)

// PubSubConfig contains fields for configuring pubsub functionality
type PubSubConfig struct {
	Enabled bool              `json:"enabled,omitempty" mapstructure:"enabled" yaml:"enabled,omitempty"`
	Backend PubSubBackend     `json:"backend,omitempty" mapstructure:"backend" yaml:"backend,omitempty"`
	Redis   RedisPubSubConfig `json:"redis,omitempty" mapstructure:"redis" yaml:"redis,omitempty"`
	Signals SignalsConfig     `json:"signals,omitempty" mapstructure:"signals" yaml:"signals,omitempty"`
}

// PubSubBackend represents the pubsub backend type
type PubSubBackend uint8

const (
	_ PubSubBackend = iota
	PubSubBackendRedis
)

func (p PubSubBackend) String() string {
	return pubsubBackendToString[p]
}

func (p PubSubBackend) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

func (p PubSubBackend) MarshalYAML() (interface{}, error) {
	return p.String(), nil
}

var (
	pubsubBackendToString = map[PubSubBackend]string{
		PubSubBackendRedis: "redis",
	}

	stringToPubSubBackend = map[string]PubSubBackend{
		"redis": PubSubBackendRedis,
	}
)

// RedisPubSubConfig contains Redis-specific pubsub configuration
type RedisPubSubConfig struct {
	Host            string          `json:"host,omitempty" mapstructure:"host" yaml:"host,omitempty"`
	Port            int             `json:"port,omitempty" mapstructure:"port" yaml:"port,omitempty"`
	RequireTLS      bool            `json:"requireTLS,omitempty" mapstructure:"require_tls" yaml:"require_tls,omitempty"`
	DB              int             `json:"db,omitempty" mapstructure:"db" yaml:"db,omitempty"`
	Prefix          string          `json:"prefix,omitempty" mapstructure:"prefix" yaml:"prefix,omitempty"`
	Username        string          `json:"-" mapstructure:"username" yaml:"-"`
	Password        string          `json:"-" mapstructure:"password" yaml:"-"`
	PoolSize        int             `json:"poolSize,omitempty" mapstructure:"pool_size" yaml:"pool_size,omitempty"`
	MinIdleConn     int             `json:"minIdleConn,omitempty" mapstructure:"min_idle_conn" yaml:"min_idle_conn,omitempty"`
	ConnMaxIdleTime time.Duration   `json:"connMaxIdleTime,omitempty" mapstructure:"conn_max_idle_time" yaml:"conn_max_idle_time,omitempty"`
	NetTimeout      time.Duration   `json:"netTimeout,omitempty" mapstructure:"net_timeout" yaml:"net_timeout,omitempty"`
	CaCertPath      string          `json:"-" mapstructure:"ca_cert_path" yaml:"-"`
	CaCertBytes     string          `json:"-" mapstructure:"ca_cert_bytes" yaml:"-"`
	InsecureSkipTLS bool            `json:"insecureSkipTLS,omitempty" mapstructure:"insecure_skip_tls" yaml:"insecure_skip_tls,omitempty"`
	Mode            RedisPubSubMode `json:"mode,omitempty" mapstructure:"mode" yaml:"mode,omitempty"`
}

// RedisPubSubMode represents the Redis pubsub mode
type RedisPubSubMode uint8

const (
	_ RedisPubSubMode = iota
	RedisPubSubModeSingle
	RedisPubSubModeCluster
)

func (r RedisPubSubMode) String() string {
	return redisPubSubModeToString[r]
}

func (r RedisPubSubMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

func (r RedisPubSubMode) MarshalYAML() (interface{}, error) {
	return r.String(), nil
}

var (
	redisPubSubModeToString = map[RedisPubSubMode]string{
		RedisPubSubModeSingle:  "single",
		RedisPubSubModeCluster: "cluster",
	}

	stringToRedisPubSubMode = map[string]RedisPubSubMode{
		"single":  RedisPubSubModeSingle,
		"cluster": RedisPubSubModeCluster,
	}
)

// SignalsConfig contains configuration for signal handling
type SignalsConfig struct {
	Enabled  bool     `json:"enabled,omitempty" mapstructure:"enabled" yaml:"enabled,omitempty"`
	Channels []string `json:"channels,omitempty" mapstructure:"channels" yaml:"channels,omitempty"`
}

// setDefaults sets default values for pubsub configuration
func (c *PubSubConfig) setDefaults(v *viper.Viper) error {
	v.SetDefault("pubsub", map[string]any{
		"enabled": false,
		"backend": "redis",
		"redis": map[string]any{
			"host":     "localhost",
			"port":     6379,
			"password": "",
			"db":       0,
			"mode":     "single",
		},
		"signals": map[string]any{
			"enabled": true,
			"channels": []string{
				"flipt:cache:invalidation",
				"flipt:config:reload",
				"flipt:flags:update",
				"flipt:health:check",
				"flipt:signals:custom",
			},
		},
	})

	return nil
}

// validate validates the pubsub configuration
func (c *PubSubConfig) validate() error {
	if c.Enabled && c.Backend == PubSubBackendRedis {
		return c.Redis.validate()
	}
	return nil
}

// validate validates the Redis pubsub configuration
func (c *RedisPubSubConfig) validate() error {
	// Add any Redis-specific validation here
	return nil
}

// IsZero returns true if the config is empty/zero value
func (c *PubSubConfig) IsZero() bool {
	return !c.Enabled
}

// ToRedisCacheConfig converts RedisPubSubConfig to RedisCacheConfig for compatibility
func (c *RedisPubSubConfig) ToRedisCacheConfig() RedisCacheConfig {
	var mode RedisCacheMode
	switch c.Mode {
	case RedisPubSubModeSingle:
		mode = RedisCacheModeSingle
	case RedisPubSubModeCluster:
		mode = RedisCacheModeCluster
	default:
		mode = RedisCacheModeSingle
	}

	return RedisCacheConfig{
		Host:            c.Host,
		Port:            c.Port,
		RequireTLS:      c.RequireTLS,
		DB:              c.DB,
		Username:        c.Username,
		Password:        c.Password,
		PoolSize:        c.PoolSize,
		MinIdleConn:     c.MinIdleConn,
		ConnMaxIdleTime: c.ConnMaxIdleTime,
		NetTimeout:      c.NetTimeout,
		CaCertPath:      c.CaCertPath,
		CaCertBytes:     c.CaCertBytes,
		InsecureSkipTLS: c.InsecureSkipTLS,
		Mode:            mode,
	}
}

// DefaultChannels returns the default signal channels
func DefaultChannels() []string {
	return []string{
		"flipt:cache:invalidation",
		"flipt:config:reload",
		"flipt:flags:update",
		"flipt:health:check",
		"flipt:signals:custom",
	}
}

// GetChannels returns the configured channels or defaults if none specified
func (c *SignalsConfig) GetChannels() []string {
	if len(c.Channels) == 0 {
		return DefaultChannels()
	}
	return c.Channels
}
