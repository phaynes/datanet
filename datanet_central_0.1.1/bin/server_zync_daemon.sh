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
  echo "Example: ${0} BOTH 2 3 -> starts BOTH_DC2-3 with UUID 2903"
  exit 2;
}

ROLE=$1

DC=$2
CN=$3
SLEEP=$4

if [ "$ROLE" != "ROUTER" -a "$ROLE" != "STORAGE" -a "$ROLE" != "BOTH" ]; then
  usage
elif [ -z "$DC" -o -z "$CN" ]; then
  usage
fi

if [ -z "SLEEP" ]; then
  SLEEP = 1;
fi

if [ "$ROLE" == "BOTH" ]; then
  CUUID="${DC}90${CN}"
  CONF_FILE="${SRCDIR}/conf/both_DC${DC}-${CN}.cfg"
elif [ "$ROLE" == "ROUTER" ]; then
  CUUID="${DC}0${CN}"
  CONF_FILE="${SRCDIR}/conf/router_DC${DC}-${CN}.cfg"
else
  CUUID="${DC}10${CN}"
  CONF_FILE="${SRCDIR}/conf/storage_DC${DC}-${CN}.cfg"
fi
CTL_FILE="/tmp/ZYNC_${ROLE}_CTL_${CUUID}"
LOG_FILE="/tmp/LOG_ZYNC_${ROLE}_${CUUID}"

echo "SRCDIR: $SRCDIR"
echo "CONF_FILE: $CONF_FILE"
echo "CTL_FILE: $CTL_FILE"
echo "SLEEP: $SLEEP"

(cd $SRCDIR;
  while true; do
    CTL_DATA=$(cat $CTL_FILE 2>/dev/null)
    if [ "$CTL_DATA" == "KILLED" ]; then
      DATE=$(date +%s)
      echo "$ROLE: ${CUUID}: STILL KILLED AT: $DATE"
      sleep 1;
    else
      rm -f $CTL_FILE
      echo; echo; echo; echo;
      echo "node zcentral.js $CONF_FILE"
      node zcentral.js $CONF_FILE
      DATE=$(date +%s)
      BAK_LOG_FILE="${LOG_FILE}_${DATE}"
      echo cp ${LOG_FILE} ${BAK_LOG_FILE}
      cp ${LOG_FILE} ${BAK_LOG_FILE}
  
      # Exit may be KillClusterNode() or normal SIGINT
      CTL_DATA=$(cat $CTL_FILE 2>/dev/null)
      if [ "$CTL_DATA" != "KILLED" ]; then
        echo sleep $SLEEP
        sleep $SLEEP
      fi
    fi
  done
)
  
