#!/bin/bash

rm -f /tmp/ZYNC_ROUTER_CTL_*; # restart downed central's
sleep 2;                      # wait for them to restart

#kill_datanet_agents
ps ax |grep zagent.js | grep conf/agent_ | while read PID rest; do echo kill $PID; kill $PID; done
#kill_datanet_nginx_agents
ps ax |grep "nginx: master process" | grep -v grep | while read PID rest; do echo kill $PID; kill $PID; done

# NODEJS AGENT CLEANUP
#kill_datanet_both="
ps ax |grep zcentral.js | grep conf/both_ | while read PID rest; do echo kill $PID; kill $PID; done
#kill_datanet_router="
ps ax |grep zcentral.js | grep conf/router_ | while read PID rest; do echo kill $PID; kill $PID; done
#kill_datanet_storager="
ps ax |grep zcentral.js | grep conf/storage_ | while read PID rest; do echo kill $PID; kill $PID; done

sleep 2;                      # wait for log flushing/archiving

# NODEJS AGENT CLEANUP
rm ../data/memory_dbs/*
rm -f /tmp/AGENT_*;

# NGINX AGENT CLEANUP
(cd ../nginx;
  rm LDB/*
  rm logs/error.log
)
rm /tmp/sticky_nginx_socket_*
rm /tmp/NGINX_PID_*

# ALL CLEANUP
rm /tmp/LOG_ZYNC_*;

./init_db.sh 6000 7000

if [ "$1" == "BOTH" ]; then
  sleep 2
  ./both_is_sync.sh    1 1
  sleep 4
  ./both_is_sync.sh    1 1
else
  sleep 2
  ./storage_is_sync.sh 1 1
  sleep 4
  ./storage_is_sync.sh 1 1
fi

if [ "$1" == "APP-SERVER" ]; then
  echo "SLEEP 30 for APP-SERVER INITIALIZATION"
  sleep 30
  echo "./kill_app_server_cluster.sh"
  ./kill_app_server_cluster.sh
  (cd ..
    ./bin/restart_memcached_cluster.sh
  )
  ./init_memcache_agents.sh
fi
