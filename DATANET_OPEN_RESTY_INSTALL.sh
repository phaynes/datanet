#!/bin/sh

. ./versions.sh

if [ ! -d ${DATANET_INSTALL_DIR} ]; then
  mkdir -p ${DATANET_INSTALL_DIR}
  mkdir ${DATANET_INSTALL_DIR}/conf
  mkdir ${DATANET_INSTALL_DIR}/locations
  mkdir ${DATANET_INSTALL_DIR}/test
fi

# COMMENTED OUT (DATANET REPO USES EXPLODED TARBALLS)
#tar xvfz ${DATANET_OPEN_RESTY_TARBALL}

(cd ${OPEN_RESTY_TEMP_DIR}
  echo "DATANET LOCATION DIRECTORY INITIALIZER"
  cp ./bin/initialize_datanet_install.sh ${DATANET_INSTALL_DIR}

  echo "DATANET C++ ENGINE LOGGING CONFIGURATION"
  cp ./datanet_nginx/easylogging.conf ${DATANET_INSTALL_DIR}

  echo "OPEN_RESTY CODE TARBALL OVERRIDE"
  (cd EXTRA/;
    unzip lua-nginx-module.zip
  )

  echo "BUILD OPEN_RESTY"
  tar xvf openresty-${OPEN_RESTY_VERSION}.tar.gz
  (cd openresty-${OPEN_RESTY_VERSION};
    (cd bundle/;
      mv ngx_lua-0.10.0/ BACKUP_ngx_lua-0.10.0/
      mkdir ngx_lua-0.10.0/
      cp -r ../../EXTRA/lua-nginx-module-master/* ngx_lua-0.10.0/
    )
    ./configure --with-pcre-jit --with-ipv6 --with-debug
    make
    make install
  )

  echo "BUILD DATANET ENGINE"
  mkdir -p ${OPENRESTY_INSTALL_DIR}/datanet
  (cd datanet_engine;
    echo "DATANET ENGINE INITIALIZATION"
    ./initialize_submodules.sh

    echo "DATANET ENGINE MAKE"
    make initial install
  )

  echo "COPY DATANET LUA CODE, LOCATIONS, & CONFIGURATION"
  (cd datanet_nginx;
    cp lua/*.lua   ${OPENRESTY_INSTALL_DIR}/datanet/
    cp *.sh        ${DATANET_INSTALL_DIR}/
    cp conf/*      ${DATANET_INSTALL_DIR}/conf/
    cp locations/* ${DATANET_INSTALL_DIR}/locations/
    cp test/*      ${DATANET_INSTALL_DIR}/test/
  )
)

echo "INITIALIZE DATANET LOCATION DIRECTORY"
(cd ${DATANET_INSTALL_DIR};
  ./initialize_datanet_install.sh
)

echo "RUN SERVER"
cat << EOF
export PATH="\$PATH:/usr/local/openresty/nginx/sbin/"
(cd /usr/local/datanet/;
  nginx -c /usr/local/datanet/conf/nginx.conf -p /usr/local/datanet/
)
EOF

echo "INITIALIZE AGENT (USER)"
cat << EOF
(cd /tmp;
  wget -O - "http://localhost:8080/station"
  wget -O - "http://localhost:8080/subscribe"
)
EOF

