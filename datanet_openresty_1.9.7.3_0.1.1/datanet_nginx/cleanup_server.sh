#!/bin/bash

NAME=$1
DIR=$2

function cleanup_unused_unix_sockets() {
  ls /tmp/sticky_nginx_socket_* | while read file; do
    echo $file | cut -f 4 -d _;
  done | while read pid; do
    NAME=$(ps -p $pid -o comm=);
    if [ -z "$NAME" ]; then
      echo rm /tmp/sticky_nginx_socket_${pid};
      rm /tmp/sticky_nginx_socket_${pid};
    fi
  done
}

echo "$0: NAME: ${NAME} DIR: ${DIR}"

rm /tmp/LOG_ZYNC_NGINX_AGENT_${NAME};
cleanup_unused_unix_sockets
rm /tmp/NGINX_PID_*

./create_server_directory.sh $DIR

echo "CLEANUP: DIR: $DIR"
(cd ${DIR}
  rm LDB/*
  rm logs/error.log
)

