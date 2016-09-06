"use strict";

var Memcached      = require('memcached');
var msgpack        = require('msgpack-js');
var lz4            = require('lz4')

var ZGenericPlugin = require('zync/plugins/generic');
var ZMemoryPlugin  = require('zync/plugins/memory');
var ZMerge         = require('../zmerge');
var ZCMP           = require('../zdcompress');
var ZH             = require('../zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// OPEN ----------------------------------------------------------------------

var MC = null;

function mc_open(db_ip, db_port, ns, xname, next) {
  exports.Plugin.url = 'localhost:' + db_port;
  ZH.e('OPENING MEMCACHE: URL: ' + exports.Plugin.url);
  MC = new Memcached(exports.Plugin.url);
  next(null, null);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATABASE I/O & COMPRESSION ------------------------------------------------

function __store_crdt_root(fkey, member, locex, next) {
  var zval       = ZCMP.CompressCrdtMember(member);
  var encoded    = msgpack.encode(zval);
  var compressed = lz4.encode(encoded);
  MC.set(fkey, compressed, locex, next);
}

function __get_crdt_root(fkey, next) {
  MC.get(fkey, function(gerr, zval) {
    if (gerr) next(gerr, null);
    else {
      if (ZH.IsUndefined(zval)) next(null, null);
      else {
        var uncompressed = lz4.decode(zval);
        var decoded      = msgpack.decode(uncompressed);
        var member       = ZCMP.DecompressCrdtMember(decoded);
        next(null, member);
      }
    }
  });
}

function __store_crdt_separate_head(mkey, head, locex, next) {
  var zhead      = ZCMP.CompressCrdt(head);
  var encoded    = msgpack.encode(zhead);
  var compressed = lz4.encode(encoded);
  MC.set(mkey, compressed, locex, next);
}

function __store_crdt(mkey, crdt, locex, next) {
  var zcrdt      = ZCMP.CompressCrdt(crdt);
  var encoded    = msgpack.encode(zcrdt);
  var compressed = lz4.encode(encoded);
  MC.set(mkey, compressed, locex, next);
}

function __get_crdt(mkey, next) {
  MC.get(mkey, function(gerr, zval) {
    if (gerr) next(gerr, null);
    else {
      if (ZH.IsUndefined(zval)) next(null, null);
      else {
        var uncompressed = lz4.decode(zval);
        var decoded      = msgpack.decode(uncompressed);
        var crdt         = ZCMP.DecompressCrdt(decoded);
        next(null, crdt);
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOGIC ---------------------------------------------------------------------

function mc_get_directory_members(coll, kqk, next) {
  var mkey = kqk;
  var dkey = ZH.GetMemcacheDirectoryMembers(mkey);
  MC.get(dkey, next);
}

function mc_get_crdt_root(coll, kqk, root, next) {
  var mkey = kqk;
  var fkey = mkey + "|" + root;
  ZH.l('mc_get_crdt_root: FK: ' + fkey);
  __get_crdt_root(fkey, next);
}

function mc_get_crdt(coll, kqk, next) {
  if (ZH.DatabaseDisabled) return ZGenericPlugin.DatabaseDisabled(next);
  var mkey = kqk;
  ZH.l('mc_get_crdt: MK: ' + mkey);
  __get_crdt(mkey, function(gerr, data) {
    if (gerr) next(gerr, null);
    else {
      if (!data) next(null, []);
      else       next(null, [data]);
    }
  });
}

function mc_store_summary(mkey, summary, locex, next) {
  if (summary.length === 0) next(null, null);
  else {
    var e     = summary.shift();
    var root  = e.root;
    var value = e.value;
    var fkey  = mkey + "|" + root;
    ZH.e('mc_store_summary: FK: ' + fkey + ' value: ' + value);
    if (value === null) {
      MC.del(fkey, function(rerr, rres) {
        if (rerr) next(rerr, null);
        else      setImmediate(mc_store_summary, mkey, summary, locex, next);
      });
    } else {
      __store_crdt_root(fkey, value, locex, function(serr, sres) {
        if (serr) next(serr, null);
        else      setImmediate(mc_store_summary, mkey, summary, locex, next);
      });
    }
  }
}

function adjust_directory_members(mbrs, summary) {
  var mdict = {};
  for (var i = 0; i < mbrs.length; i++) mdict[mbrs[i]] = i;
  var tor   = [];
  for (var i = 0; i < summary.length; i++) {
    var root  = summary[i].root;
    var value = summary[i].value;
    var hit   = mdict[root];
    if (!value) {
      // HIT & NO-VALUE -> REMOVE MEMBER
      if (ZH.IsDefined(hit)) tor.push(hit);
    } else {
      // NO-HIT & VALUE -> ADD MEMBER
      if (ZH.IsUndefined(hit)) mbrs.push(summary[i].root);
    }
  }
  for (var i = tor.length - 1; i >= 0; i--) {
    mbrs.splice(tor[i], 1);
  }
  return mbrs;
}

function mc_store_crdt_separate(coll, kqk, crdt, locex, delta, next) {
  var mkey    = kqk;
  var summary = ZMerge.SummarizeRootLevelChanges(crdt, delta);
  var head    = ZH.clone(crdt);
  delete(head._data);
  __store_crdt_separate_head(mkey, head, locex, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var dkey = ZH.GetMemcacheDirectoryMembers(mkey);
      MC.get(dkey, function(gerr, gres) {
        if (gerr) next(gerr, null);
        else {
          var mbrs = gres ? gres : [];
          adjust_directory_members(mbrs, summary);
          MC.set(dkey, mbrs, locex, function(uerr, ures) {
            if (uerr) next(uerr, null);
            else {
              mc_store_summary(mkey, summary, locex, function(aerr, ares) {
                next(aerr, crdt);
              });
            }
          });
        }
      });
    }
  });
}

function mc_store_crdt(coll, kqk, crdt, sep, locex, delta, next) {
  if (ZH.DatabaseDisabled) return ZGenericPlugin.DatabaseDisabled(next);
  if (!locex) locex = 0;
  var mkey = kqk;
  ZH.l('mc_store_crdt: MK: ' + mkey + ' SEP: ' + sep + ' DELTA: ' + delta);
  if (sep) {
    if (delta) {
      mc_store_crdt_separate(coll, kqk, crdt, locex, delta, next)
    } else { // SUBSCRIBER-MERGE on SEPARATE
      var dkey = ZH.GetMemcacheDirectoryMembers(mkey);
      MC.get(dkey, function(gerr, mbrs) {
        if (gerr) next(gerr, null);
        else { // Use DIRECTORY to simulate DELTA
          var delta = ZMerge.CreateSeparateDeltaFromDirectory(crdt, mbrs);
          mc_store_crdt_separate(coll, kqk, crdt, locex, delta, next)
        }
      });
    }
  } else {
    __store_crdt(mkey, crdt, locex, function(serr, sres) {
      next(serr, crdt);
    });
  }
}

function remove_directory_members(coll, mkey, mbrs, next) {
  if (mbrs.length === 0) next(null, null);
  else {
    var root = mbrs.shift();
    var fkey = mkey + "|" + root;
    ZH.l('remove_directory_members: MBR: ' + fkey);
    MC.del(fkey, function(rerr, rres) {
      if (rerr) next(rerr, null);
      else      setImmediate(remove_directory_members, coll, mkey, mbrs, next);
    });
  }
}

function mc_remove_crdt(coll, kqk, sep, next) {
  if (ZH.DatabaseDisabled) return ZGenericPlugin.DatabaseDisabled(next);
  var mkey = kqk;
  ZH.l('mc_remove_crdt: MK: ' + mkey + ' SEP: ' + sep);
  if (sep) {
    MC.del(mkey, function(rerr, rres) {
      if (rerr) next(rerr, null);
      else {
        var dkey = ZH.GetMemcacheDirectoryMembers(mkey);
        MC.get(dkey, function(gerr, mbrs) {
          if (gerr) next(gerr, null);
          else {
            if (!mbrs) next(null, null);
            else {
              remove_directory_members(coll, mkey, mbrs, function(serr, sres) {
                next(serr, null);
              });
            }
          }
        });
      }
    });
  } else {
    MC.del(mkey, function(rerr, rres) {
      next(rerr, null);
    });
  }
}

function mc_store_json(coll, key, val, sep, locex, delta, next) { // NO-OP
  if (ZH.DatabaseDisabled) return ZGenericPlugin.DatabaseDisabled(next);
  next(null, null);
}
function mc_remove_json(coll, key, sep, next) { // NO-OP
  if (ZH.DatabaseDisabled) return ZGenericPlugin.DatabaseDisabled(next);
  next(null, null);
}

function mc_find(coll, query, next) {
  if (ZH.DatabaseDisabled) return ZGenericPlugin.DatabaseDisabled(next);
  next(new Error("MEMCACHE DOES NOT SUPPORT FIND"), null);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NON DATA I/O CALLS ---------------------------------------------------------

function populate_data(n, duuid) { // NO-OP -> DATA IS IN MEMCACHE
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXPORTED PLUGIN -----------------------------------------------------------

ZGenericPlugin.Storage            = ZMemoryPlugin.ConstructMemoryStorage();
exports.Plugin                    = ZH.copy_members(ZGenericPlugin.Plugin);
exports.Plugin.url                = 'MEMCACHE';
exports.Plugin.do_open            = mc_open;
exports.Plugin.name               = 'MEMCACHE';

exports.Plugin.PopulateData       = populate_data;        // OVERRIDE

exports.Plugin.SaveDataOnShutdown = true;

exports.Plugin.do_get_crdt              = mc_get_crdt;
exports.Plugin.do_get_crdt_root         = mc_get_crdt_root;
exports.Plugin.do_get_directory_members = mc_get_directory_members;
exports.Plugin.do_store_crdt            = mc_store_crdt;
exports.Plugin.do_remove_crdt           = mc_remove_crdt;

exports.Plugin.do_store_json      = mc_store_json;
exports.Plugin.do_remove_json     = mc_remove_json;
exports.Plugin.do_find_json       = mc_find;

exports.Plugin.do_find            = mc_find;

