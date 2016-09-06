#!/bin/bash

function get_dir_name_conf() {
  WHO="$1"
  if [ "$WHO" == "ONE" ]; then
    DIR="./"
    NAME="1"
    CONF="conf/nginx.conf"
    return 0;
  elif [ "$WHO" == "TWO" ]; then
    DIR="./TWO/"
    NAME="2"
    CONF="conf/two_nginx.conf"
    return 0;
  elif [ "$WHO" == "THREE" ]; then
    DIR="./THREE/"
    NAME="3"
    CONF="conf/three_nginx.conf"
    return 0;
  else
    echo "USAGE: $0 [ONE,TWO,THREE]"
    return -2;
  fi
}

function kill_nginx() {
  PID=$(ps ax |grep "nginx: master process nginx -c ${CONF}" | grep -v grep | while read P rest; do echo $P; done)
  echo "kill $PID"
  kill $PID
}

