#!/bin/bash

SRC_HOST="localhost"
SRC_PORT=3306
SRC_USER="admin"
SRC_PASSWORD="admin"
TARGET_HOST="localhost"
TARGET_PORT=3307
TARGET_USER="root"
TARGET_PASSWORD="root"
DATABASE="pipes_test_db"
SYS_BENCH_SCRIPT="/usr/share/sysbench/oltp_write_only.lua"

WORK_DIR="~"
BINARY="task"
CONFIG="db_sync.json"
STATE_FILE="state_store.data"

function init_binlog() {
  echo "begin init binlog"
  mysql -h$SRC_HOST -P$SRC_PORT -u$SRC_USER -p$SRC_PASSWORD -Bse "drop database if exists $DATABASE;create database $DATABASE"
  mysql -h$SRC_HOST -P$SRC_PORT -u$SRC_USER -p$SRC_PASSWORD -Bse "reset master"
  sysbench --db-driver=mysql --mysql-user=$SRC_USER --mysql-password=$SRC_PASSWORD --mysql-host=$SRC_HOST --mysql-port=$SRC_PORT --mysql-db=$DATABASE --table_size=100000 --tables=10 --threads=10 --rand-type=uniform $SYS_BENCH_SCRIPT prepare
  sysbench --db-driver=mysql --mysql-user=$SRC_USER --mysql-password=$SRC_PASSWORD --mysql-host=$SRC_HOST --mysql-port=$SRC_PORT --mysql-db=$DATABASE --table_size=100000 --tables=10 --time=600 --threads=64 --rand-type=uniform $SYS_BENCH_SCRIPT run
  echo "init db finished"
  return
}

while getopts 'i' OPT; do
  case $OPT in
  i) init_binlog ;;
  esac
done

echo "test done"