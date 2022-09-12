package core

type StringMap = map[string]interface{}

type Configurable interface {
	// Configure reads a StringMap and make corresponding configurations.
	Configure(config StringMap) error
	// GetID returns the ID property.
	GetID() string
}

type LifeCycle interface {
	// Start represents start phase in lifecycle.
	Start() error
	// Stop represents stop phase in lifecycle.
	Stop()
}
