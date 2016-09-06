#!/bin/bash

SRCDIR="${ZYNC_SRC_PATH}"

if [ -z "$SRCDIR" ]; then
  echo "BASH ENVIRONMENT VARIABLE ZYNC_SRC_PATH must be defined"
  exit 2;
fi

DC=$1
CN=$2

if [ -z "$DC" -o -z "$CN" ]; then
  echo "Usage: DC CN"
  echo "Example: ${0} 1 2 -> starts Agent1-2"
  exit 2;
fi

CUUID="C${DC}0${CN}"
CONF_FILE="${SRCDIR}/conf/agent_DC${DC}-${CN}.cfg"

echo "SRCDIR: $SRCDIR"
echo "CONF_FILE: $CONF_FILE"

(cd $SRCDIR;
  echo "node zagent.js $CONF_FILE"
  node zagent.js $CONF_FILE & </dev/null
)
  
