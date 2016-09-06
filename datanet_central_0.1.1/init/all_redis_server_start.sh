#!/bin/sh

echo "START: $0"

SPOT=$(which redis-server)

mkdir ../redis;
(cd ../redis/;
  for P in 6000 6001 6002 \
           7000 7001 7002 \
           8000 8001 8002 \
           9000 9001 9002 \
           9500 9501 9502 \
           7500; do
    mkdir ${P} >/dev/null 2>&1
    echo "cp ${SPOT} ${P}"
    cp ${SPOT} ${P} >/dev/null 2>&1
    (cd ${P};
      echo ./redis-server --port ${P}
      ./redis-server --port ${P} & </dev/null
    )
  done
)

#for P in 11211 11212 \
         #11311 11312 \
         #11411 11412; do
  #memcached -p $P -m 1 -d
#done

ps ax | grep redis-server | grep -v grep | grep redis-server
#ps ax | grep memcached | grep -v grep | grep memcached

echo "END: $0"
echo

