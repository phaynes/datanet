#!/bin/bash

node zync-cli.js 127.0.0.1 30101 production hbuser password demos/heartbeat/scripts/FIRST_SETUP_HBUSER.zync;
node zync-cli.js 127.0.0.1 30102 production hbuser password demos/heartbeat/scripts/STATION_HBUSER.zync; 
node zync-cli.js 127.0.0.1 30201 production hbuser password demos/heartbeat/scripts/STATION_HBUSER.zync; 
node zync-cli.js 127.0.0.1 30202 production hbuser password demos/heartbeat/scripts/STATION_HBUSER.zync; 
node zync-cli.js 127.0.0.1 30301 production hbuser password demos/heartbeat/scripts/STATION_HBUSER.zync; 
node zync-cli.js 127.0.0.1 30302 production hbuser password demos/heartbeat/scripts/STATION_HBUSER.zync;
