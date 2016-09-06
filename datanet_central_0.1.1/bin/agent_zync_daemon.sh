#!/bin/bash

SRCDIR="${ZYNC_SRC_PATH}"

if [ -z "$SRCDIR" ]; then
  echo "BASH ENVIRONMENT VARIABLE ZYNC_SRC_PATH must be defined"
  exit 2;
fi

DC=$1
CN=$2
SLEEP=$3
TYPE=$4

if [ -z "$DC" -o -z "$CN" ]; then
  echo "Usage: DC CN"
  echo "Example: ${0} 1 2 -> starts Agent DC1-2"
  exit 2;
fi

if [ -z "SLEEP" ]; then
  SLEEP = 1;
fi

if [ "$TYPE" == "MEMCACHE" ]; then
  CONF_FILE="${SRCDIR}/conf/memcache_agent_DC${DC}-${CN}.cfg"
elif [ "$TYPE" == "SINGLE" ]; then
  CONF_FILE="${SRCDIR}/conf/single_agent_DC${DC}-${CN}.cfg"
else
  CONF_FILE="${SRCDIR}/conf/agent_DC${DC}-${CN}.cfg"
fi
CUUID="${DC}0${CN}"
AUFILE="/tmp/AGENT_${CUUID}"

echo "SRCDIR: $SRCDIR"
echo "CONF_FILE: $CONF_FILE"
echo "AUFILE: $AUFILE"
echo "SLEEP: $SLEEP"

(cd $SRCDIR;
  while true; do
    echo; echo; echo; echo;
    echo "node zagent.js $CONF_FILE"
    node zagent.js $CONF_FILE

    AUUID=$(cat ${AUFILE})
    LOG_FILE="/tmp/LOG_ZYNC_AGENT_${AUUID}"

    DATE=$(date +%s)
    BAK_LOG_FILE="${LOG_FILE}_${DATE}"
    echo cp ${LOG_FILE} ${BAK_LOG_FILE}
    cp ${LOG_FILE} ${BAK_LOG_FILE}

    echo sleep $SLEEP
    sleep $SLEEP
  done
)
  
