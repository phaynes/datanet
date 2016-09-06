#!/bin/sh

ps ax | grep memcached | grep -v grep | grep -v restart_memcached_cluster.sh | cut -f 1 -d \? | while read PID; do
  echo kill $PID;
  kill $PID;
done

echo sleep 1;
sleep 1;

for P in 11211 11212 11311 11312 11411 11412; do
  echo memcached -p ${P} -m 1 -d
  memcached -p ${P} -m 1 -d
done

