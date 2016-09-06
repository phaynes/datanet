#!/bin/bash

SRCDIR="${ZYNC_SRC_PATH}"

(cd $SRCDIR;
  ./bin/server_zync_daemon.sh STORAGE 1 1 5 & </dev/null
  ./bin/server_zync_daemon.sh ROUTER  1 1 5 & </dev/null
  ./init/router_is_sync.sh 1 1 & </dev/null
  ./bin/server_zync_daemon.sh STORAGE 1 2 5 & </dev/null
  ./bin/server_zync_daemon.sh ROUTER  1 2 5 & </dev/null
  ./bin/server_zync_daemon.sh STORAGE 1 3 5 & </dev/null
  ./bin/server_zync_daemon.sh ROUTER  1 3 5 & </dev/null

  sleep 5;

  ./bin/server_zync_daemon.sh STORAGE 2 1 7 & </dev/null
  ./bin/server_zync_daemon.sh ROUTER  2 1 7 & </dev/null
  ./bin/server_zync_daemon.sh STORAGE 2 2 7 & </dev/null
  ./bin/server_zync_daemon.sh ROUTER  2 2 7 & </dev/null
  ./bin/server_zync_daemon.sh STORAGE 2 3 7 & </dev/null
  ./bin/server_zync_daemon.sh ROUTER  2 3 7 & </dev/null

  ./bin/server_zync_daemon.sh STORAGE 3 1 7 & </dev/null
  ./bin/server_zync_daemon.sh ROUTER  3 1 7 & </dev/null
  ./bin/server_zync_daemon.sh STORAGE 3 2 7 & </dev/null
  ./bin/server_zync_daemon.sh ROUTER  3 2 7 & </dev/null
  ./bin/server_zync_daemon.sh STORAGE 3 3 7 & </dev/null
  ./bin/server_zync_daemon.sh ROUTER  3 3 7 & </dev/null

  sleep 5;

  ./bin/agent_zync_daemon.sh 1 1 7  & </dev/null
  ./bin/agent_zync_daemon.sh 2 1 10 & </dev/null
  ./bin/agent_zync_daemon.sh 3 1 10 & </dev/null

  sleep 2;

  ./bin/agent_zync_daemon.sh 1 2 9  & </dev/null
  ./bin/agent_zync_daemon.sh 3 2 12 & </dev/null
  ./bin/agent_zync_daemon.sh 2 2 12 & </dev/null
)

