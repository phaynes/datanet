#!/bin/bash

rm -f /tmp/ZYNC_CENTRAL_CTL_*
(cd init/; ./init_db.sh)

(cd conf/
 ./launch_haproxies.sh DEMO
)

node zcentral.js conf/single_central.cfg & < /dev/null
sleep 5;
node zagent.js conf/agent_DC1-1.cfg & < /dev/null
sleep 5;
node zync-cli.js 127.0.0.1 30101 production hbuser password demos/heartbeat/scripts/SETUP_TODO.html 


