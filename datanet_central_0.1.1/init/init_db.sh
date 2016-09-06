#!/bin/bash

ROUTER_REDIS_PORT="$1"
STORAGE_REDIS_PORT="$2"

rm -f ~/node_modules/zync/data/memory_dbs/*
rm -f /tmp/AGENT_*

for DC in D1 D2 D3; do
  echo "db.dropDatabase()" | mongo ${DC}_ZYNC
  echo "db.dropDatabase()" | mongo ${DC}_production
done

for rp in 6000 6001 6002 7000 7001 7002 8000 8001 8002 9000 9001 9002 9500 9501 9502 7500; do
  echo "redis-cli -p $rp FLUSHALL"
  redis-cli -p $rp FLUSHALL
done

DC=D1
CN=101
for port in ${STORAGE_REDIS_PORT} ${ROUTER_REDIS_PORT}; do
  CLUSTER="ROUTER"
  if [ $port -eq ${STORAGE_REDIS_PORT} ]; then
    CLUSTER="STORAGE"
  fi
  node add_user.js $port $DC $CN admin     apassword ADMIN ${CLUSTER}
  node add_user.js $port $DC $CN hbuser    password  USER  ${CLUSTER}
  node add_user.js $port $DC $CN customer1 some_pass USER  ${CLUSTER}
  node add_user.js $port $DC $CN tester    password  USER  ${CLUSTER}
  node add_user.js $port $DC $CN default   password  USER  ${CLUSTER}
done

echo 'db.D_GLOBAL_DatanetGlobal.insert({ "_id" : "NextDeviceUUID", "value" : 100000 })' | mongo D1_ZYNC
echo 'db.D_GLOBAL_DatanetGlobal.insert({ "_id" : "NextDeviceUUID", "value" : 200000 })' | mongo D2_ZYNC
echo 'db.D_GLOBAL_DatanetGlobal.insert({ "_id" : "NextDeviceUUID", "value" : 300000 })' | mongo D3_ZYNC

echo 'ADD NextDeviceUUID TO REDIS';
redis-cli -p 6000 hset INFO_D_RGLOBAL_DatanetGlobal_NextDeviceUUID value 100000
redis-cli -p 7000 hset INFO_D_SGLOBAL_DatanetGlobal_NextDeviceUUID value 100000
redis-cli -p 7500 hset INFO_D_SGLOBAL_DatanetGlobal_NextDeviceUUID value 100000
redis-cli -p 6001 hset INFO_D_RGLOBAL_DatanetGlobal_NextDeviceUUID value 200000
redis-cli -p 7001 hset INFO_D_SGLOBAL_DatanetGlobal_NextDeviceUUID value 200000
redis-cli -p 6002 hset INFO_D_RGLOBAL_DatanetGlobal_NextDeviceUUID value 300000
redis-cli -p 7002 hset INFO_D_SGLOBAL_DatanetGlobal_NextDeviceUUID value 300000

echo "FINISH: $0"

