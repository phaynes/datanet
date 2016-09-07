#!/bin/bash

if [ ! -f versions.sh ]; then
  echo "USAGE: $0 missing files [versions.sh]"
  exit 2;
fi

. ./versions.sh

if [ ! -f setup_ubuntu.sh               -o \
     ! -f install_npm_packages.sh       -o \
     ! -f DATANET_CENTRAL_INSTALL.sh    -o \
     ! -f DATANET_OPEN_RESTY_INSTALL.sh -o \
     ! -f print_example_usage.sh ]; then
  echo "USAGE: $0 missing files [versions.sh setup_ubuntu.sh, install_npm_packages.sh, DATANET_CENTRAL_INSTALL.sh, DATANET_OPEN_RESTY_INSTALL.sh, print_example_usage.sh]"
  exit 2;
fi

echo "ADDING ENTRY: 'USA' to /etc/hosts";
echo -ne "127.0.0.1\tUSA\n" | sudo tee -a /etc/hosts

sudo ./setup_ubuntu.sh

sudo ./DATANET_CENTRAL_INSTALL.sh

sudo ./DATANET_OPEN_RESTY_INSTALL.sh

./print_example_usage.sh

