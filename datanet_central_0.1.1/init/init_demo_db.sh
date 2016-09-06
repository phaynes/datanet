#!/bin/bash

ROUTER_REDIS_PORT=6000

for rp in 6000; do
  echo "redis-cli -p $rp FLUSHALL"
  redis-cli -p $rp FLUSHALL
done

DC=D1
CN=101
for port in ${ROUTER_REDIS_PORT}; do
  CLUSTER="ROUTER"
  node add_user.js $port $DC $CN admin     apassword ADMIN ${CLUSTER}
  node add_user.js $port $DC $CN hbuser    password  USER  ${CLUSTER}
  node add_user.js $port $DC $CN customer1 some_pass USER  ${CLUSTER}
  node add_user.js $port $DC $CN tester    password  USER  ${CLUSTER}
  node add_user.js $port $DC $CN default   password  USER  ${CLUSTER}
  node grant_user.js $port $DC $CN default 0 WRITE ${CLUSTER}
done

echo 'ADD NextDeviceUUID TO REDIS';
redis-cli -p 6000 hset INFO_D_RGLOBAL_DatanetGlobal_NextDeviceUUID value 100000

echo "FINISH: $0"

