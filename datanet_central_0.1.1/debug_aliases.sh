#!/bin/bash

alias zcli_NGINX="node zync-cli.js 127.0.0.1 4000 production hbuser password "

alias zagent_SINGLE_DC1-1="node zagent.js conf/single_agent_DC1-1.cfg"
alias zagent_SINGLE_DC1-3="node zagent.js conf/single_agent_DC1-3.cfg"

alias zagent_DC1-1="node zagent.js conf/agent_DC1-1.cfg"
alias zagent_DC1-2="node zagent.js conf/agent_DC1-2.cfg"
alias zagent_DC1-3="node zagent.js conf/agent_DC1-3.cfg"
alias zagent_DC1-4="node zagent.js conf/agent_DC1-4.cfg"
alias zcli_DC1-1="node zync-cli.js 127.0.0.1 30101 production hbuser password "
alias zcli_DC1-2="node zync-cli.js 127.0.0.1 30102 production hbuser password "
alias zcli_DC1-3="node zync-cli.js 127.0.0.1 30103 production hbuser password "
alias zcli_DC1-4="node zync-cli.js 127.0.0.1 30104 production hbuser password "

alias zmemcache_agent_DC1-1="node zagent.js conf/memcache_agent_DC1-1.cfg"
alias zmemcache_agent_DC1-2="node zagent.js conf/memcache_agent_DC1-2.cfg"
alias zmemcache_cli_DC1-1="node zync-cli.js 127.0.0.1 35101 production hbuser password "
alias zmemcache_cli_DC1-2="node zync-cli.js 127.0.0.1 35102 production hbuser password "
alias zmemcache_cli_DC2-1="node zync-cli.js 127.0.0.1 35201 production hbuser password "
alias zmemcache_cli_DC2-2="node zync-cli.js 127.0.0.1 35202 production hbuser password "
alias zmemcache_cli_DC3-1="node zync-cli.js 127.0.0.1 35301 production hbuser password "
alias zmemcache_cli_DC3-2="node zync-cli.js 127.0.0.1 35302 production hbuser password "


alias zagent_DC2-1="node zagent.js conf/agent_DC2-1.cfg"
alias zagent_DC2-2="node zagent.js conf/agent_DC2-2.cfg"
alias zagent_DC2-3="node zagent.js conf/agent_DC2-3.cfg"
alias zcli_DC2-1="node zync-cli.js 127.0.0.1 30201 production hbuser password "
alias zcli_DC2-2="node zync-cli.js 127.0.0.1 30202 production hbuser password "
alias zcli_DC2-3="node zync-cli.js 127.0.0.1 30203 production hbuser password "


alias zagent_DC3-1="node zagent.js conf/agent_DC3-1.cfg"
alias zagent_DC3-2="node zagent.js conf/agent_DC3-2.cfg"
alias zagent_DC3-3="node zagent.js conf/agent_DC3-3.cfg"
alias zcli_DC3-1="node zync-cli.js 127.0.0.1 30301 production hbuser password "
alias zcli_DC3-2="node zync-cli.js 127.0.0.1 30302 production hbuser password "
alias zcli_DC3-3="node zync-cli.js 127.0.0.1 30303 production hbuser password "

alias DEMO_DC1-1-BOTH="./bin/server_zync_daemon.sh  BOTH    1 1 10"
alias DEMO_DC2-1-BOTH="./bin/server_zync_daemon.sh  BOTH    2 1 12"
alias DEMO_DC3-1-BOTH="./bin/server_zync_daemon.sh  BOTH    3 1 12"

alias DEMO_DC1-1-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 1 10"
alias DEMO_DC1-1-STORAGE="./bin/server_zync_daemon.sh STORAGE 1 1 10"
alias DEMO_DC1-2-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 2 10"
alias DEMO_DC1-2-STORAGE="./bin/server_zync_daemon.sh STORAGE 1 2 10"
alias DEMO_DC1-3-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 3 10"
alias DEMO_DC1-3-STORAGE="./bin/server_zync_daemon.sh STORAGE 1 3 10"
alias DEMO_DC1-4-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 4 10"
alias DEMO_DC1-5-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 5 10"
alias DEMO_DC1-6-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 6 10"
alias DEMO_DC1-7-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 7 10"
alias DEMO_DC1-8-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 8 10"
alias DEMO_DC1-9-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 9 10"
alias DEMO_DC1-10-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 10 10"
alias DEMO_DC1-11-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 11 10"
alias DEMO_DC1-12-ROUTER="./bin/server_zync_daemon.sh  ROUTER  1 12 10"

alias DEMO_DC2-1-ROUTER="./bin/server_zync_daemon.sh  ROUTER  2 1 12"
alias DEMO_DC2-1-STORAGE="./bin/server_zync_daemon.sh STORAGE 2 1 12"
alias DEMO_DC2-2-ROUTER="./bin/server_zync_daemon.sh  ROUTER  2 2 12"
alias DEMO_DC2-2-STORAGE="./bin/server_zync_daemon.sh STORAGE 2 2 12"
alias DEMO_DC2-3-ROUTER="./bin/server_zync_daemon.sh  ROUTER  2 3 12"
alias DEMO_DC2-3-STORAGE="./bin/server_zync_daemon.sh STORAGE 2 3 12"
alias DEMO_DC3-1-ROUTER="./bin/server_zync_daemon.sh  ROUTER  3 1 12"
alias DEMO_DC3-1-STORAGE="./bin/server_zync_daemon.sh STORAGE 3 1 12"
alias DEMO_DC3-2-ROUTER="./bin/server_zync_daemon.sh  ROUTER  3 2 12"
alias DEMO_DC3-2-STORAGE="./bin/server_zync_daemon.sh STORAGE 3 2 12"
alias DEMO_DC3-3-ROUTER="./bin/server_zync_daemon.sh  ROUTER  3 3 12"
alias DEMO_DC3-3-STORAGE="./bin/server_zync_daemon.sh STORAGE 3 3 12"

alias DEMO_DC1-1-SINGLE-AGENT="./bin/agent_zync_daemon.sh 1 1 12 SINGLE"
alias DEMO_DC1-3-SINGLE-AGENT="./bin/agent_zync_daemon.sh 1 3 16 SINGLE"
alias DEMO_DC1-1-AGENT="./bin/agent_zync_daemon.sh 1 1 12"
alias DEMO_DC1-2-AGENT="./bin/agent_zync_daemon.sh 1 2 15"
alias DEMO_DC1-3-AGENT="./bin/agent_zync_daemon.sh 1 3 16"
alias DEMO_DC1-4-AGENT="./bin/agent_zync_daemon.sh 1 4 17"
alias DEMO_DC2-1-AGENT="./bin/agent_zync_daemon.sh 2 1 15"
alias DEMO_DC2-2-AGENT="./bin/agent_zync_daemon.sh 2 2 20"
alias DEMO_DC2-3-AGENT="./bin/agent_zync_daemon.sh 2 3 20"
alias DEMO_DC3-1-AGENT="./bin/agent_zync_daemon.sh 3 1 15"
alias DEMO_DC3-2-AGENT="./bin/agent_zync_daemon.sh 3 2 20"

alias DEMO_DC1-1-MEMCACHE-AGENT="./bin/agent_zync_daemon.sh 1 1 17 MEMCACHE"
alias DEMO_DC1-2-MEMCACHE-AGENT="./bin/agent_zync_daemon.sh 1 2 19 MEMCACHE"
alias DEMO_DC2-1-MEMCACHE-AGENT="./bin/agent_zync_daemon.sh 2 1 22 MEMCACHE"
alias DEMO_DC2-2-MEMCACHE-AGENT="./bin/agent_zync_daemon.sh 2 2 24 MEMCACHE"
alias DEMO_DC3-1-MEMCACHE-AGENT="./bin/agent_zync_daemon.sh 3 1 22 MEMCACHE"
alias DEMO_DC3-2-MEMCACHE-AGENT="./bin/agent_zync_daemon.sh 3 2 24 MEMCACHE"

alias DEMO_DC1-1-APP-SERVER="./bin/xtweet_app_server_daemon.sh 1 1 1"
alias DEMO_DC1-2-APP-SERVER="./bin/xtweet_app_server_daemon.sh 1 2 1"
alias DEMO_DC1-3-APP-SERVER="./bin/xtweet_app_server_daemon.sh 1 3 1"
alias DEMO_DC2-1-APP-SERVER="./bin/xtweet_app_server_daemon.sh 2 1 1"
alias DEMO_DC2-2-APP-SERVER="./bin/xtweet_app_server_daemon.sh 2 2 1"
alias DEMO_DC3-1-APP-SERVER="./bin/xtweet_app_server_daemon.sh 3 1 1"
alias DEMO_DC3-2-APP-SERVER="./bin/xtweet_app_server_daemon.sh 3 2 1"

alias kill_datanet_routers="ps ax |grep zcentral.js | grep conf/router_ | while read PID rest; do echo kill \$PID; kill \$PID; done;"
alias kill_datanet_boths="ps ax |grep zcentral.js | grep conf/both_ | while read PID rest; do echo kill \$PID; kill \$PID; done;"
alias kill_datanet_storages="ps ax |grep zcentral.js | grep conf/storage_ | while read PID rest; do echo kill \$PID; kill \$PID; done;"
alias kill_datanet_agents="ps ax |grep zagent.js | grep conf/agent_ | while read PID rest; do echo kill \$PID; kill \$PID; done;"
alias kill_datanet_memcache_agents="ps ax |grep zagent.js | grep conf/memcache_agent_ | while read PID rest; do echo kill \$PID; kill \$PID; done;"

alias kill_datanet_serverd="ps ax | grep server_zync_daemon.sh | grep -v grep | while read PID rest; do echo kill \$PID; kill \$PID; done; kill_datanet_routers kill_datanet_storages"
alias kill_datanet_agentd="ps ax | grep agent_zync_daemon.sh | grep -v grep | while read PID rest; do echo kill \$PID; kill \$PID; done; kill_datanet_agents"

alias kill_datanet_watchdog="ps ax | grep zwatchdog.js | grep -v grep | while read PID rest; do echo kill \$PID; kill \$PID; done;"

alias kill_haproxy="ps ax | grep haproxy | grep -v grep | while read PID rest; do echo kill \$PID; kill \$PID; done;"

alias kill_entire_datanet="kill_datanet_agentd kill_datanet_serverd kill_datanet_watchdog kill_haproxy"

alias kill_sudoku_datanet="kill \$((ps ax|grep node; ps ax|grep hapr) | grep -v grep | cut -f 1 -d s)"

alias kill_all_redis="ps ax |grep ./redis-server | while read PID rest; do kill \${PID}; done"

alias kill_datanet_app_server="ps ax |grep xtweet_server.js | grep conf/app_server_ | while read PID rest; do echo kill \$PID; kill \$PID; done;"

alias local_storage_sqlite="sqlite /home/sammy/.config/google-chrome/Default/Local\ Storage/https_usa_10100.localstorage"

alias validate_nginx_conf_against_db_schema="grep DATANET_ nginx/conf/nginx.conf  | cut -f 4- -d _ | cut -f 1 -d \  | while read T; do grep \"\${T}\" c_client/db_schema.sql >/dev/null; RET=\$?; if [ \$RET -ne 0 ]; then echo \"\${T}->\${RET}\"; fi; done"

