#!/bin/bash

#(cd ..; git submodule update --init --recursive)

# MAKE JSONCPP (need argument "-fPIC" to be used as a shared-object: *.so)
(cd jsoncpp;
  mkdir -p build/debug
  (cd build/debug
    cmake -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_BUILD_TYPE=debug -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF -G "Unix Makefiles" ../..
  )
)

# COPY JSONCPP HEADERS TO A CONVENIENT DIRECTORY
mkdir json 2>/dev/null
cp ./jsoncpp/include/json/* json

# DOWNLOAD AND INSTALL LUA
LUA_VERSION="5.1.5"
LUA_TARBALL="https://www.lua.org/ftp/lua-${LUA_VERSION}.tar.gz"
echo "wget ${LUA_TARBALL}"
wget "${LUA_TARBALL}"
tar xvfz lua-${LUA_VERSION}.tar.gz
ln -s lua-${LUA_VERSION} lua

cp override/LUA_MAKEFILE lua/src/Makefile

cp override/LMDB_MAKEFILE lmdb/libraries/liblmdb/Makefile

# MAKE msgpack-c (need argument "-fPIC" to be used as a shared-object: *.so)
(cd msgpack-c;
  ./bootstrap
  ./configure CFLAGS="-fPIC"
)
  
cp override/LZ4_MAKEFILE lua-lz4/Makefile

