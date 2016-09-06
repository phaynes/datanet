#!/bin/bash

. ./versions.sh

npm install keypress@0.2.1
npm install ws@1.0.1
npm install mongodb@2.1.7
npm install redis@2.4.2
npm install memcached@2.2.1
npm install msgpack-js@0.3.0
npm install lz4@0.5.1
npm install xxhash@0.2.3

# FAKE NPM INSTALL OF DATANET (AKA: zync)
(cd node_modules/
  ln -s ../${DATANET_CENTRAL_TEMP_DIR} zync
)

