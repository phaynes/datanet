"use strict";

var ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZS       = require('./zshared');
  ZH       = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

var MemoryDataCache = {};

exports.DebugDumpCache = function() {
  ZH.e('ZMDC.DebugDumpCache');
  ZH.e(MemoryDataCache);
}

var CacheMisses           = 0;
var CacheMissAlertModulus = 10;

function populate_cache_misses(cval) {
  if (ZH.IsUndefined(cval)) {
    CacheMisses += 1;
    if ((CacheMisses % CacheMissAlertModulus) === 0) {
      ZH.e('CacheMisses: ' + CacheMisses);
    }
  }
}

function cache_get(dkey, dfield) {
  var cval;
  var tval = MemoryDataCache[dkey];
  if (tval) {
    cval = tval[dfield];
  }
  populate_cache_misses(cval);
  return cval;
}

function cache_set(dkey, dfield, cval) {
  var tval = MemoryDataCache[dkey];
  if (!tval) {
    MemoryDataCache[dkey] = {};
    tval                  = MemoryDataCache[dkey];
  }
  tval[dfield] = cval;
}

function cache_delete(dkey, dfield) {
  var tval = MemoryDataCache[dkey];
  if (tval) {
    delete(tval[dfield]);
  }
}

function cache_remove(dkey) {
  delete(MemoryDataCache[dkey]);
}

function do_db_get_all(plugin, collections, dkey, next) {
  plugin.do_get(collections.key_coll, dkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length !== 0) { // CACHE results
        var got = gres[0];
        delete(got._id);
        for (var dfield in got) {
          var cval = got[dfield];
          cache_set(dkey, dfield, cval);
        }
      }
      next(null, gres);
    }
  });
}

function do_cache_invalidate(dkey) {
  delete(MemoryDataCache[dkey]);
}

function do_cache_get(plugin, collections, dkey, dfield, next) {
  //ZH.l('do_cache_get: DK: ' + dkey + ' DF: ' + dfield);
  var cval = cache_get(dkey, dfield);
  if (ZH.IsDefined(cval)) next(null, cval);
  else {
    plugin.do_get_field(collections.key_coll, dkey, dfield,
    function(gerr, gres) {
      if (gerr) next(gerr, null);
      else {
        if (gres !== null) { // MACHETE DONT PERSIST NULLS
          cache_set(dkey, dfield, gres);
        }
        next(null, gres);
      }
    });
  }
}

function do_cache_set(plugin, collections, dkey, dfield, dval, next) {
  //ZH.l('do_cache_set: DK: ' + dkey + ' DF: ' + dfield + ' DV: ' + dval);
  plugin.do_set_field(collections.key_coll, dkey, dfield, dval,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      cache_set(dkey, dfield, dval);
      next(null, sres);
    }
  });
}

function do_cache_cas(plugin, collections, dkey, dfield, dval, next) {
  var cval = cache_get(dkey, dfield);
  if (cval === dval) next(null, dval);
  else {
    do_cache_set(plugin, collections, dkey, dfield, dval, next);
  }
}

function do_cache_incr(plugin, collections, dkey, dfield, byval, next) {
  //ZH.l('do_cache_incr:  DK: ' + dkey + ' DF: ' + dfield + ' BV: ' + byval);
  plugin.do_increment(collections.key_coll, dkey, dfield, byval,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var dval = Number(sres[dfield]);
      cache_set(dkey, dfield, dval);
      next(null, sres);
    }
  });
}

function do_cache_unset(plugin, collections, dkey, dfield, next) {
  plugin.do_unset_field(collections.key_coll, dkey, dfield,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      cache_delete(dkey, dfield);
      next(null, sres);
    }
  });
}

function do_cache_remove(plugin, collections, dkey, next) {
  plugin.do_remove(collections.key_coll, dkey, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      cache_remove(dkey);
      next(null, rres);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DROP CACHE ----------------------------------------------------------------

exports.DropCache = function() {
  ZH.e('ZMDC.DropCache');
  MemoryDataCache = {};
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GETALL -------------------------------------------------------------------

exports.GetAllCentralKeysToSync = function(plugin, collections, next) {
  var dkey = ZS.CentralKeysToSync;
  do_db_get_all(plugin, collections, dkey, next);
}

exports.GetAllChannelToDevice = function(plugin, collections, schanid, next) {
  var dkey = ZS.GetChannelToDevice(schanid);
  do_db_get_all(plugin, collections, dkey, next);
}

exports.GetAllKeyToDevices = function(plugin, collections, ks, next) {
  var dkey = ZS.GetKeyToDevices(ks.kqk);
  do_db_get_all(plugin, collections, dkey, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GETTERS -------------------------------------------------------------------

exports.GetCentralKeysToSync = function(plugin, collections, ks, next) {
  var dkey   = ZS.CentralKeysToSync;
  var dfield = ks.kqk;
  do_cache_get(plugin, collections, dkey, dfield, next);
}

exports.GetKeyRepChans = function(plugin, collections, ks, next) {
  var dkey   = ZS.GetKeyRepChans(ks);
  var dfield = "value";
  do_cache_get(plugin, collections, dkey, dfield, next);
}

exports.GetKeyToDevices = function(plugin, collections, ks, duuid, next) {
  var dkey   = ZS.GetKeyToDevices(ks.kqk);
  var dfield = duuid;
  do_cache_get(plugin, collections, dkey, dfield, next);
}

exports.GetAgentWatchKeys = function(plugin, collections, ks, duuid, next) {
  var dkey   = ZS.GetAgentWatchKeys(ks, duuid);
  var dfield = "value"
  do_cache_get(plugin, collections, dkey, dfield, next);
}

exports.GetGCVersion = function(plugin, collections, ks, next) {
  var dkey   = ZS.GetKeyGCVersion(ks.kqk);
  var dfield = "gc_version";
  do_cache_get(plugin, collections, dkey, dfield, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var gcv = (gres === null) ? 0 : gres;
      next(null, gcv);
    }
  });
}

exports.GetDeviceToCentralAgentVersion = function(plugin, collections,
                                                  duuid, ks, next) {
  var dkey   = ZS.GetDeviceToCentralAgentVersion(duuid, ks.kqk);
  var dfield = "value";
  do_cache_get(plugin, collections, dkey, dfield, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTERS -------------------------------------------------------------------

exports.SetCentralKeysToSync = function(plugin, collections, ks, next) {
  ZH.l('FREEZE: K: ' + ks.kqk); // UNFREEZE KEY
  var dkey   = ZS.CentralKeysToSync;
  var dfield = ks.kqk;
  var dval   = true;
  do_cache_set(plugin, collections, dkey, dfield, dval, next);
}

// TODO: ROUTER data, OK to do a LAZY (async) DB WRITE
exports.SetDeviceToCentralAgentVersion = function(plugin, collections,
                                                  duuid, ks, dval, next) {
  var dkey   = ZS.GetDeviceToCentralAgentVersion(duuid, ks.kqk);
  var dfield = "value";
  do_cache_set(plugin, collections, dkey, dfield, dval, next);
}

exports.SetKeyToDevices = function(plugin, collections, ks, duuid, next) {
  var dkey   = ZS.GetKeyToDevices(ks.kqk);
  var dfield = duuid;
  var dval   = true;
  do_cache_set(plugin, collections, dkey, dfield, dval, next);
}

exports.SetAgentWatchKeys = function(plugin, collections, ks, duuid, next) {
  var dkey   = ZS.GetAgentWatchKeys(ks, duuid);
  var dfield = "value"
  var dval   = true;
  do_cache_set(plugin, collections, dkey, dfield, dval, next);
}

exports.SetGCVersion = function(plugin, collections, ks, dval, next) {
  var dkey   = ZS.GetKeyGCVersion(ks.kqk);
  var dfield = "gc_version";
  do_cache_cas(plugin, collections, dkey, dfield, dval, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CAS -----------------------------------------------------------------------

exports.CasKeyRepChans = function(plugin, collections, ks, dval, next) {
  var dkey   = ZS.GetKeyRepChans(ks);
  var dfield = "value";
  do_cache_cas(plugin, collections, dkey, dfield, dval, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INCREMENTERS & UNSETTERS --------------------------------------------------

exports.IncrementGCVersion = function(plugin, collections, ks, byval, next) {
  var dkey   = ZS.GetKeyGCVersion(ks.kqk);
  var dfield = "gc_version";
  do_cache_incr(plugin, collections, dkey, dfield, byval, next);
}

exports.IncrementChannelToDevice = function(plugin, collections,
                                            schanid, sub, byval, next) {
  var dkey   = ZS.GetChannelToDevice(schanid);
  var dfield = sub.UUID;
  do_cache_incr(plugin, collections, dkey, dfield, byval, next);
}

exports.UnsetCentralKeysToSync = function(plugin, collections, ks, next) {
  ZH.l('UNFREEZE: K: ' + ks.kqk); // UNFREEZE KEY
  var dkey   = ZS.CentralKeysToSync;
  var dfield = ks.kqk;
  do_cache_unset(plugin, collections, dkey, dfield, next);
}

exports.UnsetKeyToDevices = function(plugin, collections, ks, duuid, next) {
  var dkey   = ZS.GetKeyToDevices(ks.kqk);
  var dfield = duuid;
  do_cache_unset(plugin, collections, dkey, dfield, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REMOVERS ------------------------------------------------------------------

exports.RemoveAllCentralKeysToSync = function(plugin, collections, next) {
  var dkey = ZS.CentralKeysToSync;
  do_cache_remove(plugin, collections, dkey, next);
}

exports.RemoveKeyToDevices = function(plugin, collections, ks, next) {
  var dkey = ZS.GetKeyToDevices(ks.kqk);
  do_cache_remove(plugin, collections, dkey, next);
}

exports.RemoveAgentWatchKeys = function(plugin, collections, ks, duuid, next) {
  var dkey = ZS.GetAgentWatchKeys(ks, duuid);
  do_cache_remove(plugin, collections, dkey, next);
}

exports.RemoveGCVersion = function(plugin, collections, ks, next) {
  var dkey = ZS.GetKeyGCVersion(ks.kqk);
  do_cache_remove(plugin, collections, dkey, next);
}

exports.RemoveKeyRepChans = function(plugin, collections, ks, next) {
  var dkey = ZS.GetKeyRepChans(ks);
  do_cache_remove(plugin, collections, dkey, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INVALIDATORS --------------------------------------------------------------

exports.InvalidateChannelToDevice = function(schanid) {
  var dkey = ZS.GetChannelToDevice(schanid);
  do_cache_invalidate(dkey);
}

exports.InvalidateKeyToDevices = function(ks) {
  var dkey = ZS.GetKeyToDevices(ks.kqk);
  do_cache_invalidate(dkey);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZMDC']={} : exports);

