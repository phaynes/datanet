#!/bin/bash

DC=$1
CN=$2

CONF_FILE="conf/storage_DC${DC}-${CN}.cfg"

PID="";
while [ -z "$PID" ]; do
  PID=$(ps ax | grep zcentral.js | grep ${CONF_FILE} | while read P rest; do echo $P; done);
  if [ -n "${PID}" ]; then
    echo kill -s SIGWINCH $PID;
    kill -s SIGWINCH $PID;
  else
    echo "CENTRAL NOT RUNNING: (${CONF_FILE}) -> sleep 1"
    sleep 0.5;
  fi
done

