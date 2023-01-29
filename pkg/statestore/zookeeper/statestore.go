package zookeeper

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"time"
)

const DefaultConnectionTimeout = time.Second * 10

type ZKStateStoreConfig struct {
	Addresses []string
	RootPath  string
}

type ZKStateStore struct {
	core.StateStore
	config *ZKStateStoreConfig
	conn   *zk.Conn
}

func NewZKStateStore() *ZKStateStore {
	return &ZKStateStore{}
}

func (s *ZKStateStore) Configure(config core.StringMap) (err error) {
	c := &ZKStateStoreConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	s.config = c
	if s.conn, _, err = zk.Connect(s.config.Addresses, DefaultConnectionTimeout); err != nil {
		return
	}
	return
}

func (s *ZKStateStore) Save(key string, value []byte) (err error) {
	path := fmt.Sprintf("%s/%s", s.config.RootPath, key)
	var ok bool
	var stat *zk.Stat
	if ok, stat, err = s.conn.Exists(path); err != nil {
		return
	}
	if ok {
		_, err = s.conn.Set(path, value, stat.Version+1)
	} else {
		acl := zk.WorldACL(zk.PermAll)
		_, err = s.conn.Create(path, value, 0, acl)
	}
	return
}

func (s *ZKStateStore) Load(key string) ([]byte, error) {
	path := fmt.Sprintf("%s/%s", s.config.RootPath, key)
	data, _, err := s.conn.Get(path)
	return data, err
}

func (s *ZKStateStore) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *ZKStateStore) GetType() string {
	return core.ZooKeeperStateStore
}
