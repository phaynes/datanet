
"use strict";

var ZSD, ZGC, ZPart, ZCLS, ZMDC, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZSD   = require('./zsubscriber_delta');
  ZGC   = require('./zgc');
  ZPart = require('./zpartition');
  ZCLS  = require('./zcluster');
  ZMDC  = require('./zmemory_data_cache');
  ZAF   = require('./zaflow');
  ZQ    = require('./zqueue');
  ZS    = require('./zshared');
  ZH    = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL SETTINGS ----------------------------------------------------------

var CentralMaxNumberGCVPerKey       = 100;
var CentralMaxNumberTombstonePerKey = 500;
var CentralMaxGCVStaleness          = 120000;
var CentralMinGCVStaleness          = 30000;

var MinNextCentralGCPurgeInterval  = 30000;
var MaxNextCentralGCPurgeInterval  = 45000;

if (ZH.CentralReapTest) {
  CentralMaxNumberGCVPerKey     = 1;
  MinNextCentralGCPurgeInterval = 3000;
  MaxNextCentralGCPurgeInterval = 4000;
  CentralMinGCVStaleness        = 5000;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SETTINGS ------------------------------------------------------------

// Agent gets ONE GCVSummary per SubscriberMerge and
// ONE GCVSummary is all we need for Fetch->Modify->Commit RaceCondition
var AgentMaxNumberGCVPerKey       = 2;
var AgentMaxNumberTombstonePerKey = 0;
var AgentMaxGCVStaleness          = 0;
// Agents must [FETCH->MODIFY->COMMIT] within (AgentMinGCVStaleness) 5 SECS
var AgentMinGCVStaleness          = 5000;

var MinNextAgentGCPurgeInterval  = 20000;
var MaxNextAgentGCPurgeInterval  = 25000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

var GCPurgeTimer = null;

// NOTE: Sort to kqk, then gcn
function cmp_gcv(a_gcv, b_gcv) {
  var a_id  = a_gcv._id;
  var a_res = a_id.split('-');
  var a_kqk = a_res[1];
  var a_gcn = Number(a_res[2]);
  var b_id  = b_gcv._id;
  var b_res = b_id.split('-');
  var b_kqk = b_res[1];
  var b_gcn = Number(b_res[2]);
  if (a_kqk !== b_kqk) return (a_kqk > b_kqk) ? 1 : -1;
  else {
    if (a_gcn === b_gcn) throw(new Error('PROGRAM LOGIC(cmp_gcv)'));
    return (a_gcn > b_gcn) ? 1 : -1;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PURGE ENGINE --------------------------------------------------------------

function do_agent_gcv_reap(net, ks, gcv, next) {
  ZMDC.GetGCVersion(net.plugin, net.collections, ks, function(gerr, cgcv) {
    if (gerr) next(gerr, null);
    else {
      if (gcv >= cgcv) {
        ZH.e('FAIL: REAP: PURGE-GCV: ' + gcv + ' >= (C)GCV: ' + cgcv);
        next(null, null);
      } else {
        var md = {};
        ZSD.GetAgentSyncStatus(net, ks, md, function(gerr, gres) {
          if (gerr) next(gerr, null);
          else {
            var ok = (!md.to_sync_key && !md.out_of_sync_key);
            if (!ok) {
              ZH.e('FAIL: REAP ON NOT IN-SYNC-KEY: K: ' + ks.kqk);
              next(null, null);
            } else {
              ZH.l('<-|(I): DoAgentGCVReap: K: ' + ks.kqk + ' GCV: ' + gcv);
              ZGC.RemoveGCVSummary(net, ks, gcv, next);
            }
          }
        });
      }
    }
  });
}

exports.FlowDoAgentGCVReap = function(plugin, collections, qe, next) {
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  var ks    = qe.ks;
  var data  = qe.data;
  var gcv   = data.gcv;
  do_agent_gcv_reap(net, ks, gcv, function(serr, sres) {
    qnext(serr, null);
    next(null, null);
  });
}

exports.FlowDoStorageGCVReap = function(plugin, collections, qe, next) {
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  var ks    = qe.ks;
  var data  = qe.data;
  var gcv   = data.gcv;
  ZH.l('<-|(I): Reap.FlowDoStorageGCVReap: K: ' + ks.kqk + ' GCV: ' + gcv);
  ZGC.SendReapDelta(net, ks, gcv, function(serr, sres) {
    qnext(serr, null);
    next(null, null);
  });
}

//TODO REAP-DELTA can contain multiple GCV
function purge_single_gcv_summ(net, is_agent, ks, gcv, next) {
  ZH.l('purge_single_gcv_summ: K: ' + ks.kqk + ' GCV: ' + gcv);
  if (is_agent) {
    var data = {ks : ks, gcv : gcv};
    ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'AGENT_GCV_REAP',
                         net, data, null, null, next);
    var flow = {k : ks.kqk,
                q : ZQ.AgentKeySerializationQueue,
                m : ZQ.AgentKeySerialization,
                f : ZAF.FlowAgentKey };
    ZQ.StartFlow(net.plugin, net.collections, flow);
  } else {
    var data = {ks : ks, gcv : gcv};
    var mname = 'STORAGE_GCV_REAP';
    ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                                 net, data, ZH.NobodyAuth, {}, next);
    var flow  = {k : ks.kqk,
                 q : ZQ.StorageKeySerializationQueue,
                 m : ZQ.StorageKeySerialization,
                 f : ZH.Central.FlowCentralKeySerialization};
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}

function purge_gcv_summs(net, is_agent, gcvs, next) {
  if (gcvs.length === 0) next(null, null);
  else {
    var gcdata = gcvs.shift(); // TODO: rename gcvs gcdatas
    var gid    = gcdata._id;
    var res    = gid.split('-');
    var kqk    = res[1];
    var gcv    = Number(res[2]);
    var ks     = ZH.ParseKQK(kqk);
    purge_single_gcv_summ(net, is_agent, ks, gcv, function(perr, pres) {
      if (perr) next(perr, null);
      else {
        setImmediate(purge_gcv_summs, net, is_agent, gcvs, next);
      }
    });
  }
}

function purge_gcv_keys(net, is_agent, apurge_ks, next) {
  if (apurge_ks.length === 0) next(null, null);
  else {
    var purge_ks = apurge_ks.shift();
    var gcvs     = purge_ks.gcvs;
    purge_gcv_summs(net, is_agent, gcvs, function(perr, pres) {
      if (perr) next(perr, null);
      else {
        setImmediate(purge_gcv_keys, net, is_agent, apurge_ks, next);
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PURGE SELECTION ALGORITHM -------------------------------------------------

function add_purge_num_tombstones(purge_ks, ntmbs_kqk, gcv_kqk, max_num_tmbs) {
  var ndlts = 0;
  if (!max_num_tmbs) return ndlts;
  for (var kqk in ntmbs_kqk) {
    var ntmbs = ntmbs_kqk[kqk];
    if (ntmbs > max_num_tmbs) {
      var diff = ntmbs - max_num_tmbs;
      var gcvs = gcv_kqk[kqk];
      var cnt  = 0;
      for (var i = 0; i < gcvs.length; i++) {
        var gcv   = gcvs[i];
        var ntmb  = gcv.num_tombstones;
        cnt      += ntmb;
        if (cnt > diff) break;
        if (!purge_ks[kqk]) purge_ks[kqk] = [];
        purge_ks[kqk].push(gcv);
        ndlts    += 1;
      }
    }
  }
  return ndlts;
}

function add_purge_num_gcv(purge_ks, gcv_kqk, max_num_gcv) {
  var ndlts = 0;
  for (var kqk in gcv_kqk) {
    if (purge_ks[kqk]) continue; // ntmbs_kqk has precedence
    var gcvs = gcv_kqk[kqk];
    if (gcvs.length > max_num_gcv) {
      var dlen       = gcvs.length - max_num_gcv;
      purge_ks[kqk]  = gcvs.slice(0, dlen);
      ndlts         += purge_ks[kqk].length
    }
  }
  return ndlts;
}

function add_purge_stale(purge_ks, stale_kqk) {
  var ndlts = 0;
  for (var kqk in stale_kqk) {
    if (purge_ks[kqk]) continue; // ntmbs_kqk & gcv_kqk have precedence
    var gcvs       = stale_kqk[kqk];
    purge_ks[kqk]  = gcvs;
    ndlts         += gcvs.length;
  }
  return ndlts;
}

function remove_purge_min_stale(purge_ks, now, min_stale) {
  var ndlts = 0;
  for (var kqk in purge_ks) {
    var gcvs = purge_ks[kqk];
    var tor  = [];
    for (var i = 0; i < gcvs.length; i++) {
      var gcv   = gcvs[i];
      var tdiff = now - gcv.when;
      if (tdiff < min_stale) {
        ZH.l('remove_purge_min_stale: GID: ' + gcv._id + ' tdiff: ' + tdiff);
        tor.push(i);
      }
    }
    for (var i = tor.length - 1; i >= 0; i--) {
      gcvs.splice(tor[i], 1);
      ndlts += 1;
    }
    if (gcvs.length === 0) delete(purge_ks[kqk]);
  }
  return ndlts;
}

function do_gc_purge_reaper(net, is_agent, max_num_gcv, max_num_tmbs,
                            max_stale, min_stale, next) {
  net.plugin.do_scan(net.collections.gcdata_coll, function(gerr, gcvs) {
    if (gerr) next(gerr, null);
    else {
      if (gcvs.length === 0) {
        ZH.e('GC_PURGE_REAPER: ZERO GCV(KEYS)');
        next(null, null);
      } else {
        gcvs.sort(cmp_gcv);
        var gcv_kqk   = {};
        var ntmbs_kqk = {};
        var stale_kqk = {};
        var now       = ZH.GetMsTime();
        for (var i = 0; i < gcvs.length; i++) {
          var gcv = gcvs[i];
          var gid = gcv._id;
          var res = gid.split('-');
          var kqk = res[1];
          var ks  = ZH.ParseKQK(kqk);
          var ok  = true;
          if (ZH.AmCentral) {
            var cnode = ZPart.GetKeyNode(ks);
            if (!cnode || cnode.device_uuid !== ZH.MyUUID) ok = false;
          }
          if (ok) {
            if (!gcv_kqk[kqk]) gcv_kqk[kqk] = [];
            gcv_kqk[kqk].push(gcv);
            if (!ntmbs_kqk[kqk]) ntmbs_kqk[kqk]  = gcv.num_tombstones;
            else                 ntmbs_kqk[kqk] += gcv.num_tombstones;
            if (max_stale) {
              var tdiff = now - gcv.when;
              if (tdiff > max_stale) {
                if (!stale_kqk[kqk]) stale_kqk[kqk] = [];
                stale_kqk[kqk].push(gcv);
              }
            }
          }
        }
        var purge_ks = {};
        var ndlts    = 0;
        ndlts += add_purge_num_tombstones(purge_ks, ntmbs_kqk, gcv_kqk,
                                          max_num_tmbs);
        ndlts += add_purge_num_gcv(purge_ks, gcv_kqk, max_num_gcv);
        ndlts += add_purge_stale(purge_ks, stale_kqk);
        ndlts -= remove_purge_min_stale(purge_ks, now, min_stale);
        var apurge_ks = [];
        for (var kqk in purge_ks) {
          apurge_ks.push({kqk : kqk, gcvs : purge_ks[kqk]});
        }
        ZH.e('GC_PURGE_REAPER: #K: ' + apurge_ks.length + ' #Ds: ' + ndlts);
        if (apurge_ks.length === 0) next(null, null);
        else {
          purge_gcv_keys(net, is_agent, apurge_ks, next);
        }
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT GARBAGE PURGE REAPER ------------------------------------------------

function do_agent_gc_purge_reaper(next) {
  var offline = ZH.GetAgentOffline();
  if (ZH.Agent.DisableAgentGCPurgeDaemon || offline) {
    next(null, null);
  } else {
    var net          = ZH.CreateNetPerRequest(ZH.Agent);
    var max_num_gcv  = AgentMaxNumberGCVPerKey;
    var max_num_tmbs = AgentMaxNumberTombstonePerKey;
    var max_stale    = AgentMaxGCVStaleness;
    var min_stale    = AgentMinGCVStaleness;
    do_gc_purge_reaper(net, true, max_num_gcv, max_num_tmbs,
                       max_stale, min_stale, next);
  }
}

function agent_gc_purge_reaper() {
  do_agent_gc_purge_reaper(function(nerr, nres) {
    if (nerr) ZH.e('agent_gc_purge_reaper: ERROR: ' + nerr);
    next_run_agent_gc_purge();
  });
}

function next_run_agent_gc_purge() {
  var min = MinNextAgentGCPurgeInterval;
  var max = MaxNextAgentGCPurgeInterval;
  var to  = min + ((max - min) * Math.random());
  GCPurgeTimer = setTimeout(agent_gc_purge_reaper, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL GARBAGE PURGE REAPER ----------------------------------------------

function do_central_gc_purge_reaper(next) {
  if (!ZH.AmStorage) throw(new Error("central_gc_purge_reaper LOGIC ERROR"));
  if (ZH.CentralDisableCentralGCPurgeDaemon ||
      !ZCLS.AmPrimaryDataCenter()           ||
      ZH.ClusterNetworkPartitionMode        ||
      ZH.ClusterVoteInProgress              ||
      ZH.GeoVoteInProgress) {
    next(null, null);
  } else {
    var net          = ZH.CreateNetPerRequest(ZH.Central);
    var max_num_gcv  = CentralMaxNumberGCVPerKey;
    var max_num_tmbs = CentralMaxNumberTombstonePerKey;
    var max_stale    = CentralMaxGCVStaleness;
    var min_stale    = CentralMinGCVStaleness;
    do_gc_purge_reaper(net, false, max_num_gcv, max_num_tmbs,
                       max_stale, min_stale, next);
  }
}

function central_gc_purge_reaper() {
  do_central_gc_purge_reaper(function(nerr, nres) {
    if (nerr) ZH.e('central_gc_purge_reaper: ERROR: ' + nerr);
    next_run_central_gc_purge();
  });
}

function next_run_central_gc_purge() {
  var min = MinNextCentralGCPurgeInterval;
  var max = MaxNextCentralGCPurgeInterval;
  var to  = min + ((max - min) * Math.random());
  GCPurgeTimer = setTimeout(central_gc_purge_reaper, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZCentral.init_complete()
exports.StartCentralGCPurgeDaemon = function() {
  next_run_central_gc_purge();
}

// NOTE: Used in zagent.js & zbrowser.js initialization
exports.StartAgentGCPurgeDaemon = function() {
  next_run_agent_gc_purge(); //NOTE: next_run() -> not immediate @startup
}

// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZGCReap']={} : exports);

