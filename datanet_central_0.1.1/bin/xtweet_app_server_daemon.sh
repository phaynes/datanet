#!/bin/bash

SRCDIR="${ZYNC_SRC_PATH}"

if [ -z "$SRCDIR" ]; then
  echo "BASH ENVIRONMENT VARIABLE ZYNC_SRC_PATH must be defined"
  exit 2;
fi

DC=$1
CN=$2
SLEEP=$3

if [ -z "$DC" -o -z "$CN" ]; then
  echo "Usage: DC CN"
  echo "Example: ${0} 1 2 -> starts xtweet_server.js DC1-2"
  exit 2;
fi

if [ -z "SLEEP" ]; then
  SLEEP = 1;
fi

CONF_FILE="${SRCDIR}/conf/app_server_DC${DC}-${CN}.cfg"

echo "SRCDIR: $SRCDIR"
echo "CONF_FILE: $CONF_FILE"
echo "SLEEP: $SLEEP"

(cd $SRCDIR;
  while true; do
    echo; echo; echo; echo;
    echo "node xtweet_server.js $CONF_FILE"
    node xtweet_server.js $CONF_FILE

    echo sleep $SLEEP
    sleep $SLEEP
  done
)

