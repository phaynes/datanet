"use strict";

var fs          = require('fs');

var ZMCG        = require('../zmemcache_get');
var ZH          = require('../zhelper');
ZH.LogToConsole = true;
ZH.ZyncRole     = 'MEMCACHE_GETTER';
ZH.MyUUID       = 'MEMCACHE_GETTER';

var mpcfg  = process.argv[2];
var mcname = process.argv[3];
var ns     = process.argv[4];
var cn     = process.argv[5];
var key    = process.argv[6];
var root   = process.argv[7];

var res   = root ? root.split(".") : 0;
if (res.length > 1) {
  console.error('Usage: ROOT argument can not have dot-notation' + 
                ' (e.g. "a", not "a.b.c.")');
  process.exit(-1);
}

var dnodes;
try {
  var data = fs.readFileSync(mpcfg, 'utf8');
  dnodes   = JSON.parse(data);
} catch (e) {
  console.error('ERROR PARSING MEMCACHE-POOL CONFIG FILE: ' + e);
  process.exit(-1);
}

ZMCG.SetClientMemcacheClusterState(mcname, dnodes);

ZMCG.MemcacheGet(ns, cn, key, root, function(gerr, gres) {
  if (gerr) ZH.e(gerr);
  else {
    ZMCG.End();
    if (!gres) ZH.l('NO DATA');
    else {
      ZH.l('RESULT:');
      ZH.p(gres);
    }
    process.exit(0); //TODO HACK, not closing cleanly
  }
});

