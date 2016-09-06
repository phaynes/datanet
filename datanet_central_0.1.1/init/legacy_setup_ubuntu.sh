#!/bin/bash

echo "SETUP NODE REPO"
curl -sL https://deb.nodesource.com/setup | sudo bash -

echo "SETUP MONGO REPO"
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list

echo "sudo apt-get update"
sudo apt-get update

echo "INSTALL HELPERS"
echo "sudo apt-get -y install build-essential haproxy git ntp cmake"
sudo apt-get -y install build-essential haproxy git ntp cmake

echo "INSTALL NODEJS"
echo "sudo apt-get -y install nodejs"
sudo apt-get -y install nodejs

echo "INSTALL DATABASES"
echo "sudo apt-get -y install mongodb-org redis-server memcached"
sudo apt-get -y install mongodb-org redis-server memcached

echo "INSTALL OPEN-RESTY PREREQUISITES"
echo "sudo apt-get -y install libreadline-dev libncurses5-dev libpcre3-dev libssl-dev perl make build-essential"
sudo apt-get -y install libreadline-dev libncurses5-dev libpcre3-dev libssl-dev perl make build-essential

echo "INSTALL msgpakc-c PREREQUISITES"
echo "sudo apt-get -y install libtool automake"
sudo apt-get -y install libtool automake

