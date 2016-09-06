#!/bin/sh

MD=$(which mongod)
${MD} >/dev/null 2>&1 </dev/null &

ps ax | grep mongo | grep -v grep | grep mongo
