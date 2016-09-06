#!/bin/bash

DEST="$1"

if [ "$DEST" != "MAX" -a "$DEST" != "MIN" ]; then
  echo "USAGE: $0 MAX|MIN"
  exit 2;
fi

rm -f easylogging.conf
if [ "$DEST" == "MAX" ]; then
  ln -s maximal_easylogging.conf easylogging.conf
else
  ln -s minimal_easylogging.conf easylogging.conf
fi

ls -la easylogging.conf
