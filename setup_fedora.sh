#!/bin/bash

echo "INSTALL HELPERS"
echo "yum install haproxy git ntp cmake zip unzip wget openssl"
yum -y install haproxy git ntp cmake zip unzip wget openssl

echo "INSTALL NODEJS"
curl --silent --location https://rpm.nodesource.com/setup_6.x | bash -
yum -y install nodejs
yum -y install gcc-c++ make

echo "INSTALL DATABASES"
yum -y install redis

echo "INSTALL OPEN-RESTY PREREQUISITES"
echo "yum install readline-devel pcre-devel openssl-devel gcc"
yum -y install readline-devel pcre-devel openssl-devel gcc

echo "INSTALL msgpack-c PREREQUISITES"
echo "yum install libtool automake"
yum -y install libtool automake

