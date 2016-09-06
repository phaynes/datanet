"use strict";

var ZBrowserClient, ZCmdClient, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZBrowserClient = require('./zbrowser_client');
  ZCmdClient     = require('./zcmd_client');
  ZS             = require('./zshared');
  ZH             = require('./zhelper');
} 

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMIT --------------------------------------------------------------------

exports.Commit = function(cli, ns, cn, key, crdt, oplog, next) {
  var ks = ZH.CompositeQueueKey(ns, cn, key);
  ZH.l('ZDocOps.Commit: K: ' + ks.kqk);
  if (ZH.AmBrowser) { // Browser Version handles in-thread
    ZBrowserClient.ZyncCommit(ns, cn, key, crdt, oplog, next);
  } else {            // Client version sends to Agent
    cli.Commit(ns, cn, key, crdt, oplog, next);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PULL ----------------------------------------------------------------------

exports.Pull = function(cli, ns, cn, key, crdt, oplog, next) {
  var ks = ZH.CompositeQueueKey(ns, cn, key);
  ZH.l('ZDocOps.Pull: K: ' + ks.kqk);
  if (ZH.AmBrowser) {
    ZBrowserClient.ZyncPull(ns, cn, key, crdt, oplog, next);
  } else {
    cli.Pull(ns, cn, key, crdt, oplog, next);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MEMCACHE-COMMIT -----------------------------------------------------------

exports.MemcacheCommit = function(cli, ns, cn, key, oplog, rchans,
                                  sep, ex, next) {
  var ks = ZH.CompositeQueueKey(ns, cn, key);
  ZH.l('ZDocOps.MemcacheCommit: K: ' + ks.kqk + ' SEP: ' + sep + ' X: ' + ex);
  if (ZH.AmBrowser) {
    throw(new Error("MEMCACHE-COMMIT NOT SUPPORTED IN BROWSER"));
  } else {
    cli.MemcacheCommit(ns, cn, key, oplog, rchans, sep, ex, next);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STATELESS-COMMIT ----------------------------------------------------------

exports.StatelessCommit = function(cli, ns, cn, key, oplog, rchans, next) {
  var ks = ZH.CompositeQueueKey(ns, cn, key);
  ZH.l('ZDocOps.StatelessCommit: K: ' + ks.kqk);
  if (ZH.AmBrowser) {
    ZBrowserClient.ZyncStatelessCommit(ns, cn, key, oplog, rchans, next);
  } else {
    cli.StatelessCommit(ns, cn, key, oplog, rchans, next);
  }
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZDocOps']={} : exports);

