#!/bin/bash

SRC_HOST="localhost"
SRC_PORT=3306
SRC_USER="admin"
SRC_PASSWORD="admin"
TARGET_HOST="localhost"
TARGET_PORT=3307
TARGET_USER="root"
TARGET_PASSWORD="root"

WORK_DIR="~"
BINARY="task"
CONFIG="db_sync.json"

function init_db() {
  echo "init invoked"
  return
}

while getopts 'i' OPT; do
  case $OPT in
  i) init_db;;
  esac
done

