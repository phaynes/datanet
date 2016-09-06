#!/bin/bash

if [ ! -f setup_ubuntu.sh -o \
     ! -f setup_datanet.sh -o  \
     ! -f setup_openresty.sh ]; then
  echo "USAGE: RUN FROM ./init/ directory"
  exit 2;
fi

echo "./setup_ubuntu.sh"
./setup_ubuntu.sh

echo "./setup_datanet.sh"
./setup_datanet.sh

echo "./setup_openresty.sh"
./setup_openresty.sh

