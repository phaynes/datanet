#!/bin/bash

B=$(basename $PWD)

if [ "$B" != "zync" ]; then
  echo "ERROR: run from repository's root directory (e.g. ../../zync"
  exit 1;
fi

D=$(date +%s)
echo "RUNNING: $0 at $D"

echo "node ztester.js test/conf/initialize_node_unit_tests.cfg"
node ztester.js test/conf/initialize_node_unit_tests.cfg

echo "node ztester.js test/conf/Test_Merge.cmd"
node ztester.js test/conf/Test_Merge.cmd

D=$(date +%s)
echo "EXITING: $0 at $D"

