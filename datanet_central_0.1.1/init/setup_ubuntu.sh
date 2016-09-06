#!/bin/bash

echo "INSTALL HELPERS"
echo "apt-get install build-essential make haproxy git ntp cmake zip unzip wget openssl"
apt-get -y install build-essential make haproxy git ntp cmake zip unzip wget openssl

echo "INSTALL NODEJS"
curl -sL https://deb.nodesource.com/setup | bash -
apt-get -y install nodejs

echo "INSTALL DATABASES"
apt-get -y install redis-server

echo "INSTALL OPEN-RESTY PREREQUISITES"
echo "apt-get -y install libreadline-dev libncurses5-dev libpcre3-dev libssl-dev perl make build-essential"
apt-get -y install libreadline-dev libncurses5-dev libpcre3-dev libssl-dev perl make build-essential

echo "INSTALL msgpack-c PREREQUISITES"
echo "apt-get install libtool automake"
apt-get -y install libtool automake

