#!/bin/bash

DIR=$1

function create_server_pwd() {
  DIR="$1"
  echo "create_server_pwd: ${DIR}"
  mkdir ${DIR}/
  mkdir ${DIR}/logs/
  mkdir ${DIR}/LDB/
  (cd ${DIR};
    ln -s ../conf      conf;
    ln -s ../locations locations;
  )
}

echo "$0: DIR: ${DIR}"

if [ ! -d $DIR ]; then
  create_server_pwd $DIR
else
  echo "Directory already exists -> NO-OP"
fi

