#!/bin/bash

# FW TCP INBOUND/OUTBOUND (Discovery-port, 2*Central-ports, 1*Agent-port)

sudo mkdir -p /data/db
sudo chmod -R 777 /data/


# NOTE: next line has probably already happened (setup.sh is in ZYNC REPO)
#(cd node_modules; git clone git@github.com:JakSprats/zync.git)

(cd ..;
  git submodule update --init --recursive
)

(cd ../../../;
  echo "NPM INSTALLS"
  npm install keypress@0.2.1
  npm install ws@1.0.1
  npm install mongodb@2.1.7
  npm install bcrypt@0.8.5
  npm install redis@2.4.2
  npm install memcached@2.2.1
  npm install msgpack-js@0.3.0
  npm install lz4@0.5.1
)

(cd ..;
  mkdir -p ./data/memory_dbs/
  (cd static/;
    echo "./create_zync_all_js.sh"
    ./create_zync_all_js.sh
  )
  (cd init;
    ./quick_start.sh
    ./manual_start_mongod.sh
  )
)

node --version
echo "db.version()" | mongo

# TODO: ntp: JAP use JAP ntp servers
