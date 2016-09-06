#!/bin/bash

# VERSIONS
. ./versions.sh

./install_npm_packages.sh

tar xvfz ${DATANET_CENTRAL_TARBALL}

(cd ${DATANET_CENTRAL_TEMP_DIR};
  echo "INITIALIZE AND START CENTRAL REDIS SERVER"
  # INITIALIZE CENTRAL DATABASE (REDIS)
  ./init/single_redis_server_start.sh ${DATANET_CENTRAL_REDIS_PORT}
  (cd init;
    ./init_single_redis_db.sh ${DATANET_CENTRAL_REDIS_PORT}
  )

  # CREATE AGENT DATABASE DIRECTORY
  mkdir -p ${AGENT_DATABASE_DIRECTORY}
  chmod a+w ${AGENT_DATABASE_DIRECTORY}
)

