#!/bin/bash

P="$1"
if [ -z "$P" ]; then
  echo "USAGE: $0 PORT"
  exit 2
fi

echo "redis-cli -p $P FLUSHALL"
redis-cli -p $P FLUSHALL

DC=D1
CN=101
CLUSTER="ROUTER"
node add_user.js $P $DC $CN admin     apassword ADMIN ${CLUSTER}
node add_user.js $P $DC $CN hbuser    password  USER  ${CLUSTER}
node add_user.js $P $DC $CN customer1 some_pass USER  ${CLUSTER}
node add_user.js $P $DC $CN tester    password  USER  ${CLUSTER}

echo 'ADD NextDeviceUUID TO REDIS';
redis-cli -p $P hset INFO_D_RGLOBAL_DatanetGlobal_NextDeviceUUID value 100000

echo "FINISH: $0"

