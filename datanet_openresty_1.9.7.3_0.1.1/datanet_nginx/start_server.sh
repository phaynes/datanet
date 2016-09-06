#!/bin/bash

WHO="$1"
DIR="$2"
CONF="$3"

if [ "$WHO" != "ONE" -a "$WHO" != "TWO" -a "$WHO" != "THREE" ]; then
  echo "USAGE: $0 [ONE,TWO,THREE]"
  exit -2;
fi

echo "nginx -c ${CONF} -p $PWD/${DIR}"
nginx -c ${CONF} -p $PWD/${DIR}


#nginx -c conf/nginx_lb.conf -p $PWD
#valgrind --trace-children=yes --log-file=memcheck.log --tool=memcheck --leak-check=full nginx -c conf/nginx.conf -p $PWD
#nginx -c conf/minimal_nginx.conf -p $PWD

