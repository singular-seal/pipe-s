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
# binary and config files need to be put in the same dir
WORK_DIR="."
BINARY="task"
DB_SYNC_CONFIG="db_sync.json"
STATE_FILE="state_store.data"
TABLE_STRUCTURE_FILE="dump.sql"

METRICS_PORT=7778

function init_binlog() {
  echo "begin init binlog"
  mysql -h$SRC_HOST -P$SRC_PORT -u$SRC_USER -p$SRC_PASSWORD -Bse "drop database if exists $DATABASE;create database $DATABASE"
  mysql -h$SRC_HOST -P$SRC_PORT -u$SRC_USER -p$SRC_PASSWORD -Bse "reset master"
  sysbench --db-driver=mysql --mysql-user=$SRC_USER --mysql-password=$SRC_PASSWORD --mysql-host=$SRC_HOST --mysql-port=$SRC_PORT --mysql-db=$DATABASE --table_size=100000 --tables=10 --threads=10 --rand-type=uniform $SYS_BENCH_SCRIPT prepare
  sysbench --db-driver=mysql --mysql-user=$SRC_USER --mysql-password=$SRC_PASSWORD --mysql-host=$SRC_HOST --mysql-port=$SRC_PORT --mysql-db=$DATABASE --table_size=100000 --tables=10 --time=600 --threads=64 --rand-type=uniform $SYS_BENCH_SCRIPT run
  echo "init db finished"
  return
}

function is_syncing() {
  qps=$(curl -s localhost:$METRICS_PORT/metrics | grep 'task_qps{' | awk '{print $NF}')
  if [ "$qps" -gt "0" ]; then
    return 1
  fi
  sleep 1
  if [ "$qps" -gt "0" ]; then
    return 1
  else
    return 0
  fi
  return
}

function init_target_db() {
  echo "init target db"
  mysql -h$TARGET_HOST -P$TARGET_PORT -u$TARGET_USER -p$TARGET_PASSWORD -Bse "drop database if exists $DATABASE;create database $DATABASE"
  mysql -h$TARGET_HOST -P$TARGET_PORT -u$TARGET_USER -p$TARGET_PASSWORD $DATABASE < $TABLE_STRUCTURE_FILE
}

function wait_for_end() {
  sleep 10
  while true; do
    is_syncing
    if [ $? -ne 1 ]; then
      return
    fi
    echo "syncing data ..."
    sleep 10
  done
}

function kill_sync_process() {
  ps -ef | grep "$WORK_DIR/$DB_SYNC_CONFIG" | grep -v grep | awk '{print $2}' | xargs kill -9
}

while getopts 'i' OPT; do
  case $OPT in
  i) init_binlog ;;
  esac
done

init_target_db
cp "${STATE_FILE}.bak" $STATE_FILE
nohup $WORK_DIR/$BINARY --config $WORK_DIR/$DB_SYNC_CONFIG &
wait_for_end
kill_sync_process



echo "test done"
