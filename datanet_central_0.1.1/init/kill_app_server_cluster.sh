#!/bin/bash

echo kill_datanet_app_server
ps ax |grep xtweet_server.js | grep conf/app_server_ | while read PID rest; do
  echo kill $PID
  kill $PID
done

ps ax |grep xtweet_aggregator.js | grep conf/tweet_aggregator.cfg | while read PID rest; do
  echo kill $PID
  kill $PID
done

