#!/bin/sh

echo 'db.D_1000000002_DatanetGlobal.remove({"_id" : "SubscriberLatencyHistogram"});' |mongo D1_ZYNC
echo 'db.D_2000000002_DatanetGlobal.remove({"_id" : "SubscriberLatencyHistogram"});' |mongo D2_ZYNC
echo 'db.D_3000000002_DatanetGlobal.remove({"_id" : "SubscriberLatencyHistogram"});' |mongo D3_ZYNC

