#!/bin/bash

echo "RUNNING: $0"
chmod a+w .

echo "mkdir logs LDB";
mkdir logs LDB;

echo "chmod a+w logs LDB";
chmod a+w logs LDB;

(cd conf;
  mkdir ssl;
  chmod a+r ssql;
  (cd ssl;
    echo "sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout nginx.key -out nginx.crt"
    sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout nginx.key -out nginx.crt
    chmod a+r nginx.key nginx.crt
  )
)

