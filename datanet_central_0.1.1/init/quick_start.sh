#!/bin/bash

./all_redis_server_start.sh
./launch_haproxies.sh ALL

RES=$(grep USA /etc/hosts)
if [ -z "${RES}" ]; then
  echo "ADDING 127.0.0.1 USA to /etc/hosts"
  sudo echo -ne "127.0.0.1\tUSA\n" >> /etc/hosts
fi
RES=$(grep IRE /etc/hosts)
if [ -z "${RES}" ]; then
  echo "ADDING 127.0.0.1 IRE to /etc/hosts"
  sudo echo -ne "127.0.0.1\tIRE\n" >> /etc/hosts
fi
RES=$(grep JAP /etc/hosts)
if [ -z "${RES}" ]; then
  echo "ADDING 127.0.0.1 JAP to /etc/hosts"
  sudo echo -ne "127.0.0.1\tJAP\n" >> /etc/hosts
fi

