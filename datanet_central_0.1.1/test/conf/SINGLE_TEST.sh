#!/bin/bash

B=$(basename $PWD | cut -f -2 -d _)

if [ "$B" != "zync" -a "$B" != "datanet_central" ]; then
  echo "ERROR: run from repository's root directory (e.g. ../../zync"
  exit 1;
fi

D=$(date +%s)
echo "RUNNING: $0 at $D"

echo "node ztester.js test/conf/initialize_unit_tests.cfg"
node ztester.js test/conf/initialize_unit_tests.cfg

echo "node ztester.js test/conf/initialize_test_documents.cfg"
node ztester.js test/conf/initialize_test_documents.cfg

echo "node ztester.js test/conf/run_unit_tests.cfg "
node ztester.js test/conf/run_unit_tests.cfg 

D=$(date +%s)
echo "EXITING: $0 at $D"

