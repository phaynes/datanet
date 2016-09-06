#!/bin/bash

SRCDIR="${ZYNC_SRC_PATH}"

if [ -z "$SRCDIR" ]; then
  echo "BASH ENVIRONMENT VARIABLE ZYNC_SRC_PATH must be defined"
  exit 2;
fi

usage() {
  echo "Usage: ROLE DC CN"
  echo "Example: ${0} ROUTER 1 2 -> starts ROUTER_DC1-2 with UUID 102"
  echo "Example: ${0} STORAGE 1 2 -> starts STORAGE_DC1-2 with UUID 1102"
  exit 2;
}

ROLE=$1
DC=$2
CN=$3

if [ "$ROLE" != "ROUTER" -a "$ROLE" != "STORAGE" ]; then
  usage
elif [ -z "$DC" -o -z "$CN" ]; then
  usage
fi

CUUID="${DC}0${CN}"
if [ "$ROLE" == "ROUTER" ]; then
  CONF_FILE="${SRCDIR}/conf/router_DC${DC}-${CN}.cfg"
else
  CONF_FILE="${SRCDIR}/conf/storage_DC${DC}-${CN}.cfg"
fi
CTL_FILE="/tmp/ZYNC_CENTRAL_CTL_${CUUID}"

echo "SRCDIR: $SRCDIR"
echo "CONF_FILE: $CONF_FILE"
echo "CTL_FILE: $CTL_FILE"

(cd $SRCDIR;
  CTL_DATA=$(cat $CTL_FILE 2>/dev/null)
  if [ "$CTL_DATA" == "KILLED" ]; then
    DATE=$(date +%s)
    echo "$ROLE: ${CUUID}: STILL KILLED AT: $DATE"
    exit 2;
  else
    rm -f $CTL_FILE
    echo "node zcentral.js $CONF_FILE"
    node zcentral.js $CONF_FILE & </dev/null
  fi
)
  
