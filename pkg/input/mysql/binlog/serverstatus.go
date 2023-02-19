package binlog

import (
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"strings"
)

type ServerStatus struct {
	Host            string
	Port            uint16
	ServerID        uint32
	ServerUUID      string
	GtidEnabled     bool
	File            string
	Pos             uint32
	ExecutedGtidSet string
	PurgedGtidSet   string
}

func (s *ServerStatus) String() string {
	return fmt.Sprintf("Host:%s Port:%d SUUID:%s GTIDEnabled:%t File:%s Pos:%d ExecGSet:%s PurgedGSet:%s", s.Host,
		s.Port, s.ServerUUID, s.GtidEnabled, s.File, s.Pos, s.ExecutedGtidSet, s.PurgedGtidSet)
}

// BinlogPosition returns server status's corresponding position.
func (s *ServerStatus) BinlogPosition() (core.MysqlBinlogPosition, error) {
	gtidSet, err := mysql.ParseMysqlGTIDSet(s.ExecutedGtidSet)
	if err != nil {
		return core.MysqlBinlogPosition{}, err
	}

	pos := core.MysqlBinlogPosition{
		BinlogName:    s.File,
		BinlogPos:     s.Pos,
		ServerID:      s.ServerID,
		ServerUUID:    s.ServerUUID,
		TransactionID: 0,
		GTIDSetString: s.ExecutedGtidSet,
		GTIDSet:       gtidSet,
	}
	return pos, nil
}

func LoadFromServer(host string, port uint16, user string, password string) (st *ServerStatus, err error) {
	st = &ServerStatus{
		Host: host,
		Port: port,
	}

	conn, err := utils.CreateMysqlConnection(host, port, user, password)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if st.File, st.Pos, st.ExecutedGtidSet, err = loadMasterStatus(conn); err != nil {
		return
	}
	if st.ServerID, err = loadServerID(conn); err != nil {
		return
	}
	if st.ServerUUID, err = loadServerUUID(conn); err != nil {
		return
	}
	if st.GtidEnabled, err = loadGTIDIsEnabled(conn); err != nil {
		return
	}
	if st.PurgedGtidSet, err = loadPurgedGTIDSet(conn); err != nil {
		return
	}

	return
}

func loadMasterStatus(conn *sql.DB) (file string, pos uint32, executedGtidSet string, err error) {
	rows, err := conn.Query("SHOW MASTER STATUS")
	if err != nil {
		return
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return
	}

	var nullPtr interface{}
	for rows.Next() {
		if len(rowColumns) == 5 {
			err = rows.Scan(&file, &pos, &nullPtr, &nullPtr, &executedGtidSet)
		} else {
			err = rows.Scan(&file, &pos, &nullPtr, &nullPtr)
		}
		if err != nil {
			return
		}
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

func loadServerID(conn *sql.DB) (serverID uint32, err error) {
	rs, err := conn.Query("SHOW VARIABLES WHERE variable_name = 'server_id'")
	if err != nil {
		return
	}
	defer rs.Close()

	var nullPtr interface{}
	for rs.Next() {
		if err = rs.Scan(&nullPtr, &serverID); err != nil {
			return
		}
	}
	if err = rs.Err(); err != nil {
		return
	}
	return
}

func loadServerUUID(conn *sql.DB) (serverUUID string, err error) {
	rs, err := conn.Query("SHOW VARIABLES WHERE variable_name = 'server_uuid'")
	if err != nil {
		return
	}
	defer rs.Close()

	var nullPtr interface{}
	for rs.Next() {
		if err = rs.Scan(&nullPtr, &serverUUID); err != nil {
			return
		}
	}
	if err = rs.Err(); err != nil {
		return
	}
	return
}

func loadGTIDIsEnabled(conn *sql.DB) (enabled bool, err error) {
	rs, err := conn.Query("SELECT @@gtid_mode")
	if err != nil {
		return
	}
	defer rs.Close()

	var mode string
	for rs.Next() {
		if err = rs.Scan(&mode); err != nil {
			return
		}
	}
	if err = rs.Err(); err != nil {
		return
	}
	return "on" == strings.ToLower(mode), nil
}

func loadPurgedGTIDSet(conn *sql.DB) (gtidSet string, err error) {
	rs, err := conn.Query("SELECT @@gtid_purged")
	if err != nil {
		return
	}
	defer rs.Close()

	for rs.Next() {
		if err = rs.Scan(&gtidSet); err != nil {
			return
		}
	}
	if err = rs.Err(); err != nil {
		return
	}
	return
}
