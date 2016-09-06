#!/bin/bash

. ./versions.sh

echo "1.) CONFIG FILES:"
echo "  CENTRAL CONFIG README: ${DATANET_CENTRAL_TEMP_DIR}/Config.readme"
echo "  CENTRAL CONFIG FILE: ${DATANET_CENTRAL_TEMP_DIR}/conf/both_DC1-1.cfg"
echo
echo "  AGENT CONFIG README: ${DATANET_INSTALL_DIR}/Config.readme"
echo "  AGENT NGINX CONFIG FILE: ${DATANET_INSTALL_DIR}/conf/minimal_nginx.conf"
echo "  AGENT DATANET CONFIG FILE: ${DATANET_INSTALL_DIR}/conf/minimal_datanet_config.lua"
echo
echo

echo "2.) START CENTRAL:"
cat << EOF
  (cd ${DATANET_CENTRAL_TEMP_DIR}
    export ZYNC_SRC_PATH=\${PWD}
    . ./debug_aliases.sh
    (cd init; ./init_demo_db.sh)
    DEMO_DC1-1-BOTH
  )
EOF
echo;
echo;

echo "3.) START NODE.JS AGENT:"
cat << EOF
  (cd ${DATANET_CENTRAL_TEMP_DIR}
    export ZYNC_SRC_PATH=\${PWD}
    . ./debug_aliases.sh
    DEMO_DC1-1-SINGLE-AGENT
  )
EOF
echo;
echo;

echo "4.) RUN DATANET_OPENRESTY_AGENT:"
cat << EOF
  export PATH="\$PATH:${OPENRESTY_BASE_DIR}/nginx/sbin/"
  (cd ${DATANET_INSTALL_DIR};
    nginx -c ${DATANET_INSTALL_DIR}/conf/minimal_nginx.conf -p ${DATANET_INSTALL_DIR}
  )
EOF
echo;
echo;

echo "5.) EXERCISE DATANET_OPENRESTY_AGENT UNIT TESTS:"
echo "  wget -o - -O - \"http://localhost:8080/tester\""
echo;
echo;

echo "6.) EXERCISE THREE AGENT UNIT TESTS"
echo "  STEP A: START NODE.JS AGENT (STEP #3)"
echo "  STEP B: START DATANET_OPENRESTY_AGENT (STEP #4: minimal_nginx.conf)"
echo "  STEP C: START DATANET_OPENRESTY_AGENT THREE"
cat << EOF
    export PATH="\$PATH:${OPENRESTY_BASE_DIR}/nginx/sbin/"
    (cd ${DATANET_INSTALL_DIR};
      ./create_server_directory.sh THREE
      nginx -c ${DATANET_INSTALL_DIR}/conf/three_nginx.conf -p ${DATANET_INSTALL_DIR}/THREE/
    )
EOF
echo "  STEP D: RUN ZTESTER"
cat << EOF
    (cd ${DATANET_CENTRAL_TEMP_DIR}
      ./test/conf/SINGLE_TEST.sh
    )
EOF
echo;
echo;
