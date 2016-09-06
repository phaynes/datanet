#!/bin/bash

PORT=$1
if [ -z "$PORT" ]; then
  PORT=8080
fi

function get_document() {
  wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=GET";
  cat FOO
}

echo "START: $0"

MACHINE="m44"

(cd /tmp;
  echo "INITIALIZE USER"
  wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=INITIALIZE";
  cat FOO

  echo "INITIALIZE DOCUMENT"
  get_document
  sleep 2;

  echo "GET FIELD: TS"
  wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=GET&field=ts";
  cat FOO
  sleep 2;

  NOW=$(date +%s);
  echo "SET FIELD: TS TO: ${NOW}"
  wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=SET&field=ts&value=${NOW}";
  cat FOO
  get_document
  sleep 2;

  I=2;
  while [ $I -gt 0 ]; do
    echo "INCREMENT FIELD: num BYVALUE: 11"
    wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=INCREMENT&field=num&byvalue=11";
    cat FOO
    get_document
    sleep 2;
    I=$[${I}-1];
  done

  I=4;
  while [ $I -gt 0 ]; do
    NOW=$(date +%s);
    EVENT="BIG OLE EVENT at ${NOW}"
    echo "INSERT: FIELD: events VALUE: $EVENT"
wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=INSERT&field=events&value=\"${EVENT}\"&position=0";
    cat FOO
    get_document
    sleep 2;
    I=$[${I}-1];
  done


  echo "DELETE: FIELD: num"
  wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=DELETE&field=num";
  cat FOO
  get_document
  sleep 2;

  echo "SET: FIELD: num VALUE: 0"
  wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=SET&field=num&value=0";
  cat FOO
  echo "INCREMENT FIELD: num BYVALUE: 99"
  wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=INCREMENT&field=num&byvalue=99";
  cat FOO
  echo "INCREMENT FIELD: num BYVALUE: 99"
  wget -o /dev/null -O FOO "http://localhost:${PORT}/client?id=${MACHINE}&cmd=INCREMENT&field=num&byvalue=99";
  cat FOO
  get_document

)

echo "EXIT: $0"
