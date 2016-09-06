#!/bin/bash

DC=$1

if [ -z "$DC" ]; then
  echo "Usage: $0 DC-NAME"
  exit 2;
fi

if [ "$DC" == "DEMO" ]; then
  echo "haproxy -f ../conf/haproxy_DISCOVERY.cfg"
  haproxy -f ../conf/haproxy_DISCOVERY.cfg & </dev/null
  exit
fi

if [ "$DC" == "DC1" ]; then
  echo "haproxy -f ../conf/haproxy_DISCOVERY.cfg"
  haproxy -f ../conf/haproxy_DISCOVERY.cfg & </dev/null
fi

if [ "$DC" == "ALL" ]; then
  DC="DC1 DC2 DC3"
  echo "haproxy -f ../conf/haproxy_DISCOVERY.cfg"
  haproxy -f ../conf/haproxy_DISCOVERY.cfg & </dev/null
fi

for D in ${DC}; do
  echo "haproxy -f ../conf/haproxy_${D}-R-FE.cfg"
  haproxy -f ../conf/haproxy_${D}-R-FE.cfg & </dev/null

  echo "haproxy -f ../conf/haproxy_${D}-R-XE.cfg"
  haproxy -f ../conf/haproxy_${D}-R-XE.cfg & </dev/null

  echo "haproxy -f ../conf/haproxy_${D}-R-BE.cfg"
  haproxy -f ../conf/haproxy_${D}-R-BE.cfg & </dev/null
done

ps ax | grep haproxy | grep -v grep | grep haproxy

