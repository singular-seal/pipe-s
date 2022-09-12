package core

// StateStore stores states like position, statistics of a pipeline.
type StateStore interface {
	Configurable
	Save(key string, value []byte) error
	Load(key string) ([]byte, error)
	GetType() string
	Close()
}

const (
	FileStateStore = "file"
)
