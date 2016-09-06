#!/bin/bash

OPEN_RESTY_VERSION="1.9.7.3"
LUA_VERSION="5.1.5"
NGINX_BIN="/usr/local/openresty/nginx/sbin/nginx"

(cd /tmp;
  wget https://openresty.org/download/openresty-${OPEN_RESTY_VERSION}.tar.gz
  tar xvf openresty-${OPEN_RESTY_VERSION}.tar.gz
  (cd openresty-${OPEN_RESTY_VERSION}
    mkdir EXTRA
    (cd EXTRA;
      git clone git@github.com:JakSprats/lua-nginx-module.git
    )
    (cd bundle;
      mv ngx_lua-0.10.0/ BAK_ngx_lua-0.10.0/
      mkdir ngx_lua-0.10.0/
      cp -r ../EXTRA/lua-nginx-module/* ngx_lua-0.10.0/
    )
    ./configure --with-pcre-jit --with-ipv6 --with-debug
    make
    sudo make install
  )
)

(cd ../;
  echo "LINK NGINX TO DATANET REPO"
  sudo chmod a+x ${NGINX_BIN}
  ln -s ${NGINX_BIN} ./nginx/nginx
  ln -s ${NGINX_BIN} ./nginx/TWO/nginx/nginx

  echo "COPY NGINX LUA REPO TO DATANET REPO"
  cp -r /tmp/openresty-${OPEN_RESTY_VERSION}/build/lua-${LUA_VERSION} ./c_client/
  (cd ./c_client/;
     ln -s lua-${LUA_VERSION} lua
  )
)

(cd ../;
  (cd nginx;
    mkdir logs LDB;
    (cd conf;
      mkdir ssl;
      (cd ssl;
        echo "sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout nginx.key -out nginx.crt"
        sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout nginx.key -out nginx.crt
      )
    )
    (cd lua;
      sudo ln -s $PWD /usr/local/openresty/lualib/datanet
    )
  )
)
