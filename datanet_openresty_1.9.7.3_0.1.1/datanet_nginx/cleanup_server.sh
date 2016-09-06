#!/bin/bash

NAME=$1
DIR=$2

echo "$0: NAME: ${NAME} DIR: ${DIR}"

rm /tmp/LOG_ZYNC_NGINX_AGENT_${NAME};
rm /tmp/sticky_nginx_socket_*
rm /tmp/NGINX_PID_*

./create_server_directory.sh $DIR

echo "CLEANUP: DIR: $DIR"
(cd ${DIR}
  rm LDB/*
  rm logs/error.log
)

