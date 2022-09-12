package utils

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"math/rand"
	"time"
)

const (
	DefaultMysqlConnectionTimeout = 10000 // milliseconds
	DefaultMysqlReadTimeout       = 10000
	DefaultMysqlWriteTimeout      = 10000
)

// CreateMysqlClient creates a new instance of mysql connection
func CreateMysqlClient(host string, port uint16, user string, password string) (db1 *sql.DB, err error) {
	dbDSNTimeouts := "&timeout=%vms&readTimeout=%vms&writeTimeout=%vms"
	dbDSNTimeouts = fmt.Sprintf(dbDSNTimeouts, DefaultMysqlConnectionTimeout, DefaultMysqlReadTimeout, DefaultMysqlWriteTimeout)

	dbDSN := "%s:%s@tcp(%s:%d)/?interpolateParams=true&parseTime=true&multiStatements=true&collation=utf8mb4_general_ci%s"
	dbDSN = fmt.Sprintf(dbDSN, user, password, host, port, dbDSNTimeouts)

	if db1, err = sql.Open("mysql", dbDSN); err != nil {
		return nil, err
	}

	timeoutDuration := time.Duration(DefaultMysqlReadTimeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	if err = db1.PingContext(ctx); err != nil {
		return nil, err
	}

	return db1, nil
}

// CloseMysqlClient closes mysql client connection
func CloseMysqlClient(db *sql.DB) error {
	if db == nil {
		return nil
	}
	return db.Close()
}

func GenerateRandomServerID() uint32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	for {
		id := r.Uint32() % 10000000
		if id > 0 {
			return id
		}
	}
}
