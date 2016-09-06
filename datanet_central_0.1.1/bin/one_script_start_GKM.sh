#!/bin/bash

rm -f /tmp/ZYNC_CENTRAL_CTL_*
(cd init/; ./init_db.sh)

(cd conf/
 ./launch_haproxies.sh DC1
 ./launch_haproxies.sh DC2
 ./launch_haproxies.sh DC3
)

./bin/launch_watchdog.sh

./bin/demo_start.sh

sleep 30;
./bin/init_six_agents_for_GKM.sh 

