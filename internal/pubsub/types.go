package pubsub

// FeatureFlagUpdateSignal represents a feature flag update event
type FeatureFlagUpdateSignal struct {
	FlagKey   string `json:"flag_key"`
	Namespace string `json:"namespace"`
	Action    string `json:"action"`
}
