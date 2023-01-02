package file

import (
	"errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"io/ioutil"
	"os"
)

const Perm = 0666

type FileStateStore struct {
	core.StateStore
	path string
}

func NewFileStateStore() *FileStateStore {
	s := &FileStateStore{}
	return s
}

func (s *FileStateStore) Configure(config core.StringMap) (err error) {
	if s.path, err = utils.GetStringFromConfig(config, "$.Path"); err != nil {
		return
	}
	if _, err := os.Stat(s.path); errors.Is(err, os.ErrNotExist) {
		if f, err := os.Create(s.path); err == nil {
			f.Close()
		}
	}
	return
}

func (s *FileStateStore) Save(key string, value []byte) error {
	return ioutil.WriteFile(s.path, value, Perm)
}

func (s *FileStateStore) Load(key string) ([]byte, error) {
	return ioutil.ReadFile(s.path)
}

func (s *FileStateStore) Close() {
}

func (s *FileStateStore) GetType() string {
	return core.FileStateStore
}
