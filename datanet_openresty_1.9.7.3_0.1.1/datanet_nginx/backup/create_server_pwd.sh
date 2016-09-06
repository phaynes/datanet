#!/bin/bash

DIR="$1"

if [ -z "$DIR" ]; then
  echo "USAGE: $0 DIRECTORY"
  exit 2;
fi

mkdir $DIR
RET=$?

if [ $RET -ne 0 ]; then
  echo "MKDIR $DIR -> FAILED -> EXITING"
  exit 2;
fi

(cd $DIR
  ln -s ../../c_client/ c_client
  mkdir nginx
  (cd nginx
    cp ../../easylogging.conf .
    ln -s ../../lua lua
    ln -s ../../locations locations
    mkdir conf
    cp ../../conf/* conf/
    mkdir logs
    mkdir data
  )
)

