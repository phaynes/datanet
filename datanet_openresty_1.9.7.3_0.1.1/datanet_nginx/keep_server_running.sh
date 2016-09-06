#!/bin/bash

WHO="$1"

echo WHO: $WHO

. ./helpers.sh
get_dir_name_conf $WHO
RET=$?
if [ $RET != 0 ]; then
  exit $RET;
fi

function go() {
  D=$(date +%s)
  echo; echo; echo; echo;
  echo "${D}: ./cleanup_server.sh -> ./start_server.sh"
  ./cleanup_server.sh ${NAME} ${DIR}
  ./start_server.sh ${WHO} ${DIR} ${CONF}
  echo "${D}: SERVER STARTED"
  echo; echo;
}

echo kill_nginx
kill_nginx

echo sleep 2;
sleep 2;

go

while true; do
  RES=$(ps ax |grep "nginx: master process nginx -c ${CONF}"| grep -v grep)
  if [ -z "${RES}" ]; then
    echo; echo; echo; echo;
    D=$(date +%s)
    echo "${D}: SERVER EXITED";
    echo "${D}: SLEEP 18";
    sleep 18;
    go
  fi
  sleep 2
done 

