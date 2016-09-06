#!/bin/sh

zmemcache_cli_DC1_1="node zync-cli.js 127.0.0.1 35101 production hbuser password "
zmemcache_cli_DC1_2="node zync-cli.js 127.0.0.1 35102 production hbuser password "
zmemcache_cli_DC2_1="node zync-cli.js 127.0.0.1 35201 production hbuser password "
zmemcache_cli_DC2_2="node zync-cli.js 127.0.0.1 35202 production hbuser password "
zmemcache_cli_DC3_1="node zync-cli.js 127.0.0.1 35301 production hbuser password "
zmemcache_cli_DC3_2="node zync-cli.js 127.0.0.1 35302 production hbuser password "

(cd ..
  $zmemcache_cli_DC1_1 /var/zync/GRANT_TESTER_WRITE_7.zync

  $zmemcache_cli_DC1_1 ./demos/heartbeat/scripts/STATION_TESTER.zync
  $zmemcache_cli_DC1_2 ./demos/heartbeat/scripts/STATION_TESTER.zync
  $zmemcache_cli_DC2_1 ./demos/heartbeat/scripts/STATION_TESTER.zync
  $zmemcache_cli_DC2_2 ./demos/heartbeat/scripts/STATION_TESTER.zync
  $zmemcache_cli_DC3_1 ./demos/heartbeat/scripts/STATION_TESTER.zync
  $zmemcache_cli_DC3_2 ./demos/heartbeat/scripts/STATION_TESTER.zync
)
