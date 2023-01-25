#!/bin/bash

SRC_HOST="127.0.0.1"
SRC_PORT=3306
SRC_USER="admin"
SRC_PASSWORD="admin"
TARGET_HOST="127.0.0.1"
TARGET_PORT=3307
TARGET_USER="root"
TARGET_PASSWORD="root"
DATABASE="pipes_test_db"
SYS_BENCH_SCRIPT="/usr/share/sysbench/oltp_write_only.lua"
# binary and config files need to be put in the same dir
WORK_DIR="."
BINARY="task"
DB_SYNC_CONFIG="db_sync.json"
DB_SYNC_STATE_FILE="state_store.data"

DB_CHECK_CONFIG="db_check.json"
DB_CHECK_STATE_FILE="db_check_state_store.data"
DB_CHECK_RESULT_FILE="check_result.txt"

TABLE_STRUCTURE_FILE="dump.sql"

METRICS_PORT=7778

RUNNING_COUNT=1

function init_binlog() {
  echo "begin init binlog"
  mysql -h$SRC_HOST -P$SRC_PORT -u$SRC_USER -p$SRC_PASSWORD -Bse "drop database if exists $DATABASE;create database $DATABASE"
  mysql -h$SRC_HOST -P$SRC_PORT -u$SRC_USER -p$SRC_PASSWORD -Bse "reset master"
  sysbench --db-driver=mysql --mysql-user=$SRC_USER --mysql-password=$SRC_PASSWORD --mysql-host=$SRC_HOST --mysql-port=$SRC_PORT --mysql-db=$DATABASE --table_size=100000 --tables=10 --threads=10 --rand-type=uniform $SYS_BENCH_SCRIPT prepare
  sysbench --db-driver=mysql --mysql-user=$SRC_USER --mysql-password=$SRC_PASSWORD --mysql-host=$SRC_HOST --mysql-port=$SRC_PORT --mysql-db=$DATABASE --table_size=100000 --tables=10 --time=600 --threads=64 --rand-type=uniform $SYS_BENCH_SCRIPT run
  echo "init db finished"
  return
}

# 1=syncing 0=finished 2=not started
function sync_status() {
  qps=$(curl -s localhost:$METRICS_PORT/metrics | grep 'task_qps{' | awk '{print $NF}')
  if [ -z "$qps" ]; then
    return 2
  fi
  if [ "$qps" -gt "0" ]; then
    return 1
  fi
  # ensure there's no dataflow by 2 qps=0 which have 10 seconds interval
  sleep 10
  if [ "$qps" -gt "0" ]; then
    return 1
  else
    return 0
  fi
}

function init_target_db() {
  echo "init target db"
  mysql -h$TARGET_HOST -P$TARGET_PORT -u$TARGET_USER -p$TARGET_PASSWORD -Bse "drop database if exists $DATABASE;create database $DATABASE"
  mysql -h$TARGET_HOST -P$TARGET_PORT -u$TARGET_USER -p$TARGET_PASSWORD $DATABASE <$TABLE_STRUCTURE_FILE
}

function wait_for_sync_finished() {
  sleep 10
  while true; do
    sync_status
    s=$?
    if [ $s -eq 0 ]; then
      return
    elif [ $s -eq 1 ]; then
      echo "syncing data"
    else
      echo "syncing process exited"
    fi

    sleep 10
  done
}

function kill_sync_process() {
  ps -ef | grep "$WORK_DIR/$DB_SYNC_CONFIG" | grep -v grep | awk '{print $2}' | xargs kill -9
}

function wait_for_sync_process_stopped() {
  while true; do
    count=$(lsof -i:$METRICS_PORT | wc -l)
    if [ "$count" -eq "0" ]; then
      break
    fi
    sleep 1
  done

  while true; do
    count=$(ps -ef | grep "$WORK_DIR/$DB_SYNC_CONFIG" | grep -v grep -c)
    if [ "$count" -eq "0" ]; then
      echo "syncing stopped ..."
      break
    fi
    sleep 1
  done
  sleep 10
}

function randomly_restart_sync() {
  delay=$((RANDOM % 40 + 10))
  echo "restart after $delay seconds"
  sleep $((delay))
  kill_sync_process
  wait_for_sync_process_stopped
  nohup $WORK_DIR/$BINARY --config $WORK_DIR/$DB_SYNC_CONFIG &
}

function test_once() {
  init_target_db
  cp "${DB_SYNC_STATE_FILE}.bak" $DB_SYNC_STATE_FILE
  nohup $WORK_DIR/$BINARY --config $WORK_DIR/$DB_SYNC_CONFIG &
  randomly_restart_sync
  wait_for_sync_finished
  kill_sync_process

  echo "begin checking db"
  rm "$DB_CHECK_STATE_FILE"
  $WORK_DIR/$BINARY --config $WORK_DIR/$DB_CHECK_CONFIG
  echo "finish one run"
}

while getopts 'i:c:' OPT; do
  case $OPT in
  i) init_binlog ;;
  c) RUNNING_COUNT="$OPTARG" ;;
  esac
done

rm "$DB_CHECK_RESULT_FILE"

while [ "$RUNNING_COUNT" -gt "0" ]; do
  test_once
  RUNNING_COUNT=$((RUNNING_COUNT - 1))
done
echo "all done"
