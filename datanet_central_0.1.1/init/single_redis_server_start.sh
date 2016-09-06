#!/bin/sh

P="$1"
if [ -z "$P" ]; then
  echo "USAGE: $0 PORT"
  exit 2
fi

echo "START: $0"

SPOT=$(which redis-server)

mkdir ${P} >/dev/null 2>&1
echo "cp ${SPOT} ${P}"
cp ${SPOT} ${P} >/dev/null 2>&1
(cd ${P};
  echo ./redis-server --port ${P}
  ./redis-server --port ${P} & </dev/null
)

ps ax | grep redis-server

echo "END: $0"
echo

