"use strict";

var ZCache, ZMDC, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZCache = require('./zcache');
  ZMDC   = require('./zmemory_data_cache');
  ZAF    = require('./zaflow');
  ZQ     = require('./zqueue');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CACHE REAPER --------------------------------------------------------------

function cmp_when_desc(k1, k2) {
  var w1 = k1.when;
  var w2 = k2.when;
  return (w1 === w2) ? 0 : (w1 > w2) ? -1 : 1; // NOTE DESC
}

function do_evict_key(net, ks, next) {
  var local_evict = ZH.Agent.CacheConfig.local_evict;
  if (local_evict) {
    ZCache.AgentLocalEvict(net, ks, true, {}, next);
  } else {
    ZCache.AgentEvict(net, ks, true, {}, next);
  }
}

exports.FlowDoReapEvict = function(plugin, collections, qe, next) {
  var net     = qe.net;
  var qnext   = qe.next;
  var ks      = qe.ks;
  do_evict_key(net, ks, function(serr, sres) {
    qnext(serr, null);
    next(null, null);
  });
}

function evict_key(net, ks, next) {
  var data = {};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'CACHE_REAP',
                       net, data, null, null, next);
  var flow = {k : ks.kqk,         q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function do_reap_evict(net, toe, next) {
  if (toe.length === 0) next(null, null);
  else {
    var kqk = toe.shift();
    var ks  = ZH.ParseKQK(kqk);
    ZH.e('REAP-EVICT: K: ' + ks.kqk);
    evict_key(net, ks, function(cerr, cres) {
      if (cerr) next(cerr, null);
      else      setImmediate(do_reap_evict, net, toe, next);
    });
  }
}

// ALGORITHM: array divided in half (OLD & NEW)
//            in NEW part, 20% is reserved for local_modification KEYS
//            in NEW part, 20% is reserved for local_read         KEYS
//            All other keys are kept following LRU
function analyze_cache_for_reap(net, cnbytes, want_bytes, klrus, toe) {
  var clrus = [];
  for (var i = 0; i < klrus.length; i++) {
    var klru   = klrus[i];
    var kqk    = klru._id;
    var cached = klru.cached;
    var pin    = klru.pin;
    var watch  = klru.watch;
    if (cached && !pin && !watch) clrus.push(klru);
  }

  if (clrus.length === 0) return 0;

  clrus.sort(cmp_when_desc); // DESC (i.e. START is youngest/biggest)
  var mid   = Math.floor(clrus.length / 2);
  var ot    = clrus[mid].when;
  var rsize = Math.floor(want_bytes * 0.2); // RESERVED SIZE for:
  var msize = rsize;                        //   1.) Local Modifications
  var asize = rsize;                        //   2.) Local Reads
  var olds  = [];
  for (var i = 0; i < mid; i++) {
    var clru   = clrus[i];
    var kqk    = clru._id;
    var when   = clru.when;
    var locr   = clru.local_read;
    var lmod   = clru.local_modification;
    var nbytes = clru.num_bytes;
    var is_lm  = lmod && (lmod > ot);
    var is_lr  = locr && (locr > ot);
    ZH.l('CLRU: K: ' + kqk + ' is_lm: ' + is_lm + ' is_lr: ' + is_lr);
    if (is_lm) {
      msize -= nbytes;
      if (msize <= 0) olds.push(clru);
      else            ZH.l('RESERVE: LMOD: K: ' + kqk);
    } else if (is_lr) {
      asize -= nbytes;
      if (asize <= 0) olds.push(clru); 
      else            ZH.l('RESERVE: LOCR: K: ' + kqk);
    } else {
      olds.push(clru);
    }
  }
  for (var i = mid; i < clrus.length; i++) {
    var clru = clrus[i];
    olds.push(clru);
  }

  //ZH.e('olds'); ZH.e(olds);
  for (var i = (olds.length - 1); i >= 0; i--) { // START with OLDEST
    var old     = olds[i];
    var kqk     = old._id;
    var nbytes  = old.num_bytes; // NEW
    cnbytes    -= nbytes;
    toe.push(kqk);
    ZH.e('EVICT: K: ' + kqk + ' #B: ' + nbytes + ' C(#B): ' + cnbytes);
    if (cnbytes <= want_bytes) break;
  }
  return cnbytes;
}

function do_agent_cache_reap(net, cnbytes, want_bytes, klrus, next) {
  var ocnbytes = cnbytes;
  var toe      = [];
  cnbytes      = analyze_cache_for_reap(net, cnbytes, want_bytes, klrus, toe);
  //ZH.l('toe'); ZH.p(toe);
  do_reap_evict(net, toe, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var ckey = ZS.AgentCacheNumBytes;
      var diff = (cnbytes - ocnbytes);
      ZH.e('POST EVICTION: C(#B): ' + cnbytes + ' D: ' + diff);
      net.plugin.do_increment(net.collections.global_coll, 
                              ckey, "value", diff, next);
    }
  });
}

var AgentCacheReaperRunning = false;

function start_agent_cache_reaper(net, cnbytes, next) {
  var max_bytes  = ZH.Agent.CacheConfig.max_bytes;
  var want_bytes = Math.floor(max_bytes * 0.8);
  net.plugin.do_scan(net.collections.lru_coll, function(serr, klrus) {
    if (serr) next(serr, null);
    else      do_agent_cache_reap(net, cnbytes, want_bytes, klrus, next);
  });
}

function signal_agent_cache_reaper(net, cnbytes) {
  ZH.l('signal_agent_cache_reaper');
  if (AgentCacheReaperRunning) return;
  AgentCacheReaperRunning = true;
  start_agent_cache_reaper(net, cnbytes, function(serr, sres) {
    if (serr) ZH.e(serr.message);
    AgentCacheReaperRunning = false;
  });
}

function check_agent_cache_limits(net, cnbytes) {
  if (ZH.Agent.CacheConfig) {
    var max_bytes = ZH.Agent.CacheConfig.max_bytes;
    if (cnbytes > max_bytes) {
      ZH.e('THRESHOLD: C(#B): ' + cnbytes + ' MAX(#B): ' + max_bytes);
      signal_agent_cache_reaper(net, cnbytes);
    } else {
      ZH.l('OK: cache(#b): ' + cnbytes + ' MAX(#B): ' + max_bytes);
    }
  }
}

exports.SetLruLocalRead = function(net, ks, next) {
  net.plugin.do_get(net.collections.lru_coll, ks.kqk, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var klru        = (gres.length === 0) ? {} : gres[0];
      var now         = ZH.GetMsTime();
      klru.when       = now; // NOTE: required, used in ZAS.do_sync_missing_key
      klru.local_read = now;
      net.plugin.do_set(net.collections.lru_coll, ks.kqk, klru, next);
    }
  });
}

function store_modification(net, ks, selfie, cached, nbytes, mod, next) {
  net.plugin.do_get(net.collections.lru_coll, ks.kqk, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var klru       = (gres.length === 0) ? {} : gres[0];
      var now        = ZH.GetMsTime();
      klru.when      = now; // NOTE: required, used in ZAS.do_sync_missing_key
      klru.cached    = cached;
      klru.num_bytes = nbytes;
      if (selfie) klru.local_modification    = mod;
      else        klru.external_modification = mod;
      net.plugin.do_set(net.collections.lru_coll, ks.kqk, klru, next);
    }
  });
}

exports.StoreNumBytes = function(net, pc, o_nbytes, selfie, next) {
  var ks = pc.ks;
  var md = pc.extra_data.md;
  ZMDC.GetKeyToDevices(net.plugin, net.collections, ks, ZH.MyUUID,
  function (kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      var cached   = kres ? true : false;
      var n_nbytes = 0; // REMOVE by default
      var mod      = 0; // REMOVE by default
      if (!pc.ncrdt && !pc.remove) {
        ZH.l('ZCR.StoreNumBytes: NOT REMOVE & NO NCRDT -> SKIP');
        next(null, null);
      } else {
        if (pc.ncrdt) {
          n_nbytes = pc.ncrdt._meta.num_bytes;
          if (selfie) mod = pc.ncrdt._meta["@"];
          else {
            var created = pc.ncrdt._meta.created;
            if (created) mod = created[ZH.MyDataCenter];
            else         mod = pc.ncrdt._meta["@"]; // OCRDT won in MERGE
          }
        }
        ZH.l('ZCR.StoreNumBytes: cached : ' + cached + ' mod: ' + mod +
             ' o_nbytes: ' + o_nbytes + ' n_nbytes: ' + n_nbytes);
        store_modification(net, ks, selfie, cached, n_nbytes, mod,
        function(serr, sres) {
          if (serr) next(serr, null);
          else {
            if      (!cached)               next(null, null);
            else if (o_nbytes === n_nbytes) next(null, null);
            else {
              var ckey = ZS.AgentCacheNumBytes;
              var diff = (n_nbytes - o_nbytes);
              net.plugin.do_increment(net.collections.global_coll,
                                      ckey, "value", diff,
              function(ierr, ires) {
                if (ierr) next(ierr, null);
                else {
                  var cnbytes = Number(ires.value);
                  ZH.l('DIFF: ' + diff + ' C(#B): ' + cnbytes);
                  next(null, null);
                  // NOTE: check_agent_cache_limits() is ASYNC
                  check_agent_cache_limits(net, cnbytes);
                }
              });
            }
          }
        });
      }
    }
  });
}

// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZCR']={} : exports);

