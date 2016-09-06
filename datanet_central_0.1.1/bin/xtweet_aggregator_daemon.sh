#!/bin/bash

SRCDIR="${ZYNC_SRC_PATH}"

if [ -z "$SRCDIR" ]; then
  echo "BASH ENVIRONMENT VARIABLE ZYNC_SRC_PATH must be defined"
  exit 2;
fi

DC=$1
SLEEP=$2

if [ -z "$DC" ]; then
  echo "Usage: DC"
  echo "Example: ${0} 1 -> starts xtweet_aggregator.js DC1"
  exit 2;
fi

if [ -z "SLEEP" ]; then
  SLEEP = 1;
fi

CONF_FILE="${SRCDIR}/conf/tweet_aggregator_DC${DC}.cfg"

echo "SRCDIR: $SRCDIR"
echo "CONF_FILE: $CONF_FILE"
echo "SLEEP: $SLEEP"

(cd $SRCDIR;
  while true; do
    echo; echo; echo; echo;
    echo "node xtweet_aggregator.js $CONF_FILE"
    node xtweet_aggregator.js $CONF_FILE

    echo sleep $SLEEP
    sleep $SLEEP
  done
)

