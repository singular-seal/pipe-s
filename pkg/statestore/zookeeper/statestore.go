package zookeeper

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"strings"
	"time"
)

const DefaultConnectionTimeout = time.Second * 30

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

func ensurePath(conn *zk.Conn, path string) error {
	parts := strings.Split(path, "/")[1:]
	current := ""
	for _, part := range parts {
		current = fmt.Sprintf("%s/%s", current, part)
		ok, _, err := conn.Exists(current)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		acl := zk.WorldACL(zk.PermAll)
		if _, err = conn.Create(current, []byte{}, 0, acl); err != nil {
			return err
		}
	}
	return nil
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
		if err = ensurePath(s.conn, path); err != nil {
			return
		}
		_, err = s.conn.Set(path, value, 1)
	}
	return
}

func (s *ZKStateStore) Load(key string) ([]byte, error) {
	path := fmt.Sprintf("%s/%s", s.config.RootPath, key)
	data, _, err := s.conn.Get(path)
	if err == nil {
		return data, nil
	}
	if err != zk.ErrNoNode {
		return nil, err
	}

	return []byte{}, ensurePath(s.conn, path)
}

func (s *ZKStateStore) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *ZKStateStore) GetType() string {
	return core.ZooKeeperStateStore
}
