
var T = require('./ztrace'); // TRACE (before strict)
"use strict";

require('./setImmediate');

var ZPub   = require('./zpublisher');
var ZRAD   = require('./zremote_apply_delta');
var ZPart  = require('./zpartition');
var ZADaem = require('./zagent_daemon');
var ZOOR   = require('./zooo_replay');
var ZCLS   = require('./zcluster');
var ZMDC   = require('./zmemory_data_cache');
var ZDQ    = require('./zdata_queue');
var ZQ     = require('./zqueue');
var ZS     = require('./zshared');
var ZH     = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var RepeatGeoDeltaDrainTimeout = 10000;

var ThresholdNumberDeltaFreezeKey = 100;
var NumberDeltasPerBatch          = 5;
var BetweenBatchDrainSleep        = 1000; // 1 second

// (CentralDirtyDeltaStaleness > ZVote.ClusterNodeMaximumAllowedStaleness*2)
//  otherwise DirtyDeltaReaper jumps the gun on remove-DC-Cluster-REORG
var CentralDirtyDeltaStaleness        = 45000;
var MinNextCentralDirtyDeltasInterval = 18000;
var MaxNextCentralDirtyDeltasInterval = 20000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

var GeoDeltaDrainTimestamp  = {}; // GLOBAL [RGUUID,KQK]

function set_geo_delta_drain(rguuid, kqk, now) {
  if (!GeoDeltaDrainTimestamp[rguuid]) GeoDeltaDrainTimestamp[rguuid] = {};
  GeoDeltaDrainTimestamp[rguuid][kqk] = now;
}

function get_geo_delta_drain(rguuid, kqk) {
  var rok = GeoDeltaDrainTimestamp[rguuid];
  if (!rok) return 0;
  return rok[kqk];
}

var GeoDeltaDrainInProgress = {}; // GLOBAL [RGUUID,KQK]

function set_geo_drain_in_progress(rguuid, kqk) {
  if (!GeoDeltaDrainInProgress[rguuid]) GeoDeltaDrainInProgress[rguuid] = {};
  GeoDeltaDrainInProgress[rguuid][kqk] = true;
}

function get_geo_drain_in_progress(rguuid, kqk) {
  var rok = GeoDeltaDrainInProgress[rguuid];
  if (!rok) return false;
  return rok[kqk];
}

function unset_geo_drain_in_progress(rguuid, kqk) {
  var rok = GeoDeltaDrainInProgress[rguuid];
  if (!rok || !rok[kqk]) return;
  delete(GeoDeltaDrainInProgress[rguuid][kqk]);
  var nr  = Object.keys(GeoDeltaDrainInProgress[rguuid]).length;
  if (nr === 0) delete(GeoDeltaDrainInProgress[rguuid]);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DRAIN GEO DELTAS ----------------------------------------------------------

function get_unacked_DCs(net, ks, author, next) {
  var dkey = ZS.GetCentralGeoAckDeltaMap(ks, author);
  net.plugin.do_get(net.collections.key_coll, dkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var gnodes = ZCLS.GetGeoNodes();
      if (gres.length === 0) { // ERROR-CASE -> TRY ALL DCs (AGAIN)
        next(null, gnodes);
      } else {
        var cgnodes = gnodes;
        var gmap    = gres[0];
        delete(gmap._id);
        var tor  = [];
        for (var i = 0; i < cgnodes.length; i++) {
          var rguuid = cgnodes[i].device_uuid;
          if (gmap[rguuid]) tor.push(i);
        }
        for (var i = tor.length - 1; i >= 0; i--) {
          cgnodes.splice(tor[i], 1);
        }
        next(null, cgnodes);
      }
    }
  });
}

function get_unacked_DCs_authors_map(net, ks, authors, dmap, next) {
  if (authors.length === 0) next(null, null);
  else { 
    var author = authors.shift();
    get_unacked_DCs(net, ks, author, function(gerr, cgnodes) {
      if (gerr) next(gerr, null);
      else {
        for (var i = 0; i < cgnodes.length; i++) {
          var rguuid = cgnodes[i].device_uuid;
          if (!dmap[rguuid]) dmap[rguuid] = [];
          dmap[rguuid].push(author);
        }
        setImmediate(get_unacked_DCs_authors_map, net, ks, authors, dmap, next);
      }
    });
  }
}

function batch_drain_geo_dentries(net, ks, dentries, freeze, gnode,
                                  bid, bnum, next) {
  bnum       += 1; // NEXT BATCH-NUMBER
  var rguuid  = gnode.device_uuid;
  set_geo_drain_in_progress(rguuid, ks.kqk);
  if (dentries.length <= NumberDeltasPerBatch) { // FINAL ONE
    ZH.l('batch_drain_geo_dentries: K: ' + ks.kqk + ' BID: ' + bid + 
         ' BN: ' + bnum + ' FINAL BATCH');
    ZDQ.PushRouterGeoDentries(net.plugin, net.collections,
                              ks, dentries, freeze, gnode, next);
    unset_geo_drain_in_progress(rguuid, ks.kqk);
  } else {
    ZH.l('batch_drain_geo_dentries: K: ' + ks.kqk + ' BID: ' + bid +
         ' BN: ' + bnum + ' SEND BATCH');
    var pdentries = [];
    for (var i = 0; i < NumberDeltasPerBatch; i++) {
      pdentries.push(dentries.shift()); // SHIFT FROM DENTRIES[] TO PDENTRIES[]
      if (dentries.length === 0) break;
    }
    ZDQ.PushRouterGeoDentries(net.plugin, net.collections,
                              ks, pdentries, freeze, gnode,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        if (dentries.length == 0) next(null, null);
        else {
          setTimeout(function() {
            batch_drain_geo_dentries(net, ks, dentries, freeze, gnode,
                                     bid, bnum, next);
          }, BetweenBatchDrainSleep);
        }
      }
    });
  }
}



function drain_keys_dentries_to_DC(net, ks, pdentries, freeze, gnode, next) {
  ks.security_token = ZH.GenerateKeySecurityToken(ks);
  var bid           = ZH.GetNextClientRpcID();
  // NOTE: batch_drain_geo_dentries() is ASYNC -> (HAS TIMEOUTS)
  batch_drain_geo_dentries(net, ks, pdentries, freeze, gnode,
                           bid, 0, ZH.OnErrLog);
  next(null, null);
}

function add_avrsns_to_authors(avrsns, authors) {
  for (var i = 0; i < avrsns.length; i++) {
    var avrsn  = avrsns[i];
    var auuid  = ZH.GetAvUUID(avrsn);
    var author = {agent_uuid : auuid, agent_version : avrsn};
    authors.push(author);
  }
}

function do_get_all_authors(net, ks, next) {
  var gkey = ZS.GetCentralKeyAgentVersions(ks.kqk);
  net.plugin.do_get_array_values(net.collections.delta_coll, gkey, "aversions",
  function (gerr, avrsns) {
    if (gerr) next(gerr, null);
    else {
      var authors = [];
      if (avrsns) add_avrsns_to_authors(avrsns, authors);
      var akey = ZS.GetOOOKeyDeltaVersions(ks.kqk);
      net.plugin.do_get_array_values(net.collections.delta_coll,
                                     akey, "aversions",
      function(ferr, oooavrsns) {
        if (ferr) next(ferr, null);
        else {
          if (oooavrsns) add_avrsns_to_authors(oooavrsns, authors);
          next(null, authors);
        }
      });
    }
  });
}

function get_all_authors(net, ks, unacked, rguuid, next) {
  do_get_all_authors(net, ks, function(ferr, authors) {
    if (ferr) next(ferr, null);
    else {
      if (!unacked) next(null, authors); // SINGLE-DC DRAIN
      else {                             // UNACKED -> DIRTY_DELTA_REAPER
        var dmap = {};
        get_unacked_DCs_authors_map(net, ks, authors, dmap,
        function(gerr, gres) {
          if (gerr) next(gerr, null);
          else {
            var rauthors = dmap[rguuid] ? dmap[rguuid] : [];
            next(null, rauthors);
          }
        });
      }
    }
  });
}

function prepare_deltas_for_geo_drain(dentries) {
  var pdentries = ZH.clone(dentries);
  for (var i = 0; i < pdentries.length; i++) {
    var pdentry = pdentries[i];
    pdentry.delta._meta.dirty_central = true;
    ZPub.CleanupGeoDeltaBeforeSend(pdentry);
  }
  return pdentries;
}

function do_geo_drain_keys_dentries_to_DC(net, ks, authors, gnode, next) {
  ZOOR.DependencySortAuthors(net, ks, false, authors, function(gerr, dentries) {
    if (gerr) next(gerr, null);
    else {
      var freeze    = false;
      var pdentries = freeze ? [] : prepare_deltas_for_geo_drain(dentries);
      drain_keys_dentries_to_DC(net, ks, pdentries, freeze, gnode, next);
    }
  });
}

// TODO: Estimate GeoDentries vs AckGeoNeedMerge sizes
function get_should_freeze_key(net, ks, force, authors, next) {
  var sf = (!force && (authors.length >= ThresholdNumberDeltaFreezeKey));
  ZH.l('get_should_freeze_key: K: ' + ks.kqk + ' SF: ' + sf);
  next(null, sf);
}

function do_flow_drain_geo_deltas(net, ks, gnode, force, unacked, next) {
  var rguuid = gnode.device_uuid;
  var now    = ZH.GetMsTime();
  set_geo_delta_drain(rguuid, ks.kqk, now);
  ZH.e('GEO-DELTA-DRAIN: K: ' + ks.kqk + ' RU: ' + rguuid + ' START: ' + now);
  get_all_authors(net, ks, unacked, rguuid, function(ferr, authors) {
    if (ferr) next(ferr, null);
    else {
      if (authors.length === 0) next(null, null);
      else {
        ZH.RemoveRepeatAuthor(authors);
        ZH.l('geo_drain_keys_dentries_to_DC: K: ' + ks.kqk +
             ' RU: ' + rguuid + ' #As: ' + authors.length);
        get_should_freeze_key(net, ks, force, authors, function(serr, sf) {
          if (serr) next(serr, null);
          else {
            if (sf) {
              var freeze = true;
              drain_keys_dentries_to_DC(net, ks, [], freeze, gnode, next);
            } else {
              do_geo_drain_keys_dentries_to_DC(net, ks, authors, gnode, next);
            }
          }
        });
      }
    }
  });
}

exports.FlowSerializedDrainGeoDeltas = function(plugin, collections, qe, next) {
  var net     = qe.net;
  var ks      = qe.ks;
  ZH.l('<-|(G): ZGDD.FlowSerializedDrainGeoDeltas: K: ' + ks.kqk);
  var data    = qe.data;
  var gnode   = data.gnode;
  var force   = data.force;
  var unacked = data.unacked;
  do_flow_drain_geo_deltas(net, ks, gnode, force, unacked,
  function(aerr, ares) {
    if (aerr) ZH.e(aerr); // NOTE: Just LOG, no THROWs in FLOWs
    next(null, null);
  });
}

function serialized_drain_geo_deltas(net, ks, gnode, force, unacked) {
  var rguuid      = gnode.device_uuid;
  var in_progress = get_geo_drain_in_progress(rguuid, ks.kqk);
  if (in_progress) { // TIMER WAS A LITTLE LATE AND ANOTHER DRAIN STARTED
    ZH.e('serialized_drain_geo_deltas: DRAIN IN PROGRESS: K: ' + ks.kqk +
         ' R: ' + rguuid + ' -> NO-OP');
  } else {
    var data  = {gnode : gnode, force : force, unacked : unacked};
    var mname = 'SERIALIZED_DRAIN_GEO_DELTAS';
    ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                                 net, data, ZH.NobodyAuth, {}, ZH.OnErrLog);
    var flow  = {k : ks.kqk,
                 q : ZQ.StorageKeySerializationQueue,
                 m : ZQ.StorageKeySerialization,
                 f : ZH.Central.FlowCentralKeySerialization};
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}

var DrainGeoDeltasTimer = null;

function set_drain_geo_deltas_timer(net, ks, gnode, force, unacked,
                                    loo_drain, now) {
  if (DrainGeoDeltasTimer) return;
  var good = loo_drain + RepeatGeoDeltaDrainTimeout;
  var to   = good      - now;
  ZH.e('set_drain_geo_deltas_timer: TO: ' + to);
  DrainGeoDeltasTimer = setTimeout(function() {
    DrainGeoDeltasTimer = null;
    serialized_drain_geo_deltas(net, ks, gnode, force, unacked);
  }, to);
}

exports.DoFlowDrainGeoDeltas = function(net, ks, gnode, force, unacked, next) {
  ZH.l('ZGDD.DoFlowDrainGeoDeltas: K: ' + ks.kqk);
  var rguuid      = gnode.device_uuid;
  var in_progress = get_geo_drain_in_progress(rguuid, ks.kqk);
  if (in_progress) {
    ZH.e('Drain-DC: DRAIN IN PROGRESS: K: ' + ks.kqk +
         ' R: ' + rguuid + ' -> NO-OP');
    next(null, null);
  } else {
    var loo_drain = get_geo_delta_drain(rguuid, ks.kqk);
    var now       = ZH.GetMsTime();
    if (loo_drain) { // RATE-LIMITING
      var diff = now - loo_drain;
      if (diff < RepeatGeoDeltaDrainTimeout) {
        ZH.e('GEO-DELTA-DRAIN: TOO SOON: diff: ' + diff);
        set_drain_geo_deltas_timer(net, ks, gnode, force, unacked,
                                   loo_drain, now);
        return next(null, null);
      }
    }
    do_flow_drain_geo_deltas(net, ks, gnode, force, unacked, next);
  }
}

exports.FlowHandleDrainGeoDeltas = function(plugin, collections, qe, next) {
  var net     = qe.net;
  var ks      = qe.ks;
  ZH.l('<-|(G): ZGDD.FlowHandleDrainGeoDeltas: K: ' + ks.kqk);
  var data    = qe.data;
  var gnode   = data.gnode;
  var force   = data.force;
  var unacked = data.unacked;
  exports.DoFlowDrainGeoDeltas(net, ks, gnode, force, unacked,
  function(aerr, ares) {
    if (aerr) ZH.e(aerr); // NOTE: Just LOG, no THROWs in FLOWs
    next(null, null);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL DIRTY DELTA REAPER ------------------------------------------------

function do_dirty_delta_ALL_DC(net, ks, gnodes, next) {
  if (gnodes.length === 0) next(null, null);
  else {
    var gnode  = gnodes.shift();
    var rguuid = gnode.device_uuid;
    ZH.l('do_dirty_delta_ALL_DC: K: ' + ks.kqk + ' RU: ' + rguuid);
    var data   = {gnode : gnode, force : true, unacked : true};
    ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks,
                                 'DRAIN_GEO_DELTAS',
                                 net, data, ZH.NobodyAuth, {},
    function(serr, sres) { // NOTE: CLOSURE as 'next' argument
      if (serr) next(serr, null);
      else {
        setImmediate(do_dirty_delta_ALL_DC, net, ks, gnodes, next);
      }
    });
    var flow  = {k : ks.kqk,
                 q : ZQ.StorageKeySerializationQueue,
                 m : ZQ.StorageKeySerialization,
                 f : ZH.Central.FlowCentralKeySerialization};
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}
function do_central_dirty_deltas(net, kss, gnodes, next) {
  if (kss.length === 0) next(null, null);
  else {
    var ks      = kss.shift();
    var cgnodes = ZH.clone(gnodes); // NOTE: gets modified
    //NOTE per-key FLOWs are ASYNC
    do_dirty_delta_ALL_DC(net, ks, cgnodes, ZH.OnErrLog);
    setImmediate(do_central_dirty_deltas, net, kss, gnodes, next);
  }
}

function central_dirty_deltas(net, dmap, next) {
  var fl_old   = ZH.GetMsTime() - CentralDirtyDeltaStaleness;
  var fl_dvals = [];
  for (var dval in dmap) {
    var ts = dmap[dval];
    if (ts <= fl_old) fl_dvals.push(dval);
  }
  if (fl_dvals.length === 0) {
    ZH.e('CENTRAL_DIRTY_DELTAS: ZERO OLD DIRTY_DELTAS');
    next(null, null);
  } else {
    var ukss = {};
    for (var i  = 0; i < fl_dvals.length; i++) {
      var dval  = fl_dvals[i];
      var res   = dval.split('-');
      var kqk   = res[0];
      ukss[kqk] = true;
    }
    var kss = [];
    for (var kqk in ukss) {
      var ks    = ZH.ParseKQK(kqk);
      var cnode = ZPart.GetKeyNode(ks);
      if (cnode && cnode.device_uuid === ZH.MyUUID) kss.push(ks);
    }
    if (kss.length === 0) {
      ZH.e('CENTRAL_DIRTY_DELTAS: ZERO LOCAL DIRTY_DELTAS');
    } else {
      ZH.e('CENTRAL_DIRTY_DELTAS: #Ks: ' + kss.length);
      ZH.p(kss); ZH.p(fl_dvals);
    }
    var gnodes = ZCLS.GetOtherGeoNodes();
    do_central_dirty_deltas(net, kss, gnodes, next);
  }
}

function __central_dirty_deltas_daemon(next) {
  if (!ZH.AmStorage) throw(new Error("central_dirty_deltas LOGIC ERROR"));
  if (ZH.CentralDisableCentralDirtyDeltasDaemon ||
      ZH.ClusterNetworkPartitionMode            ||
      ZH.ClusterVoteInProgress                  ||
      ZH.GeoVoteInProgress)                            {
    return next(null, null);
  }
  var net  = ZH.CreateNetPerRequest(ZH.Central);
  var dkey = ZS.CentralDirtyDeltas; //TODO SCAN
  net.plugin.do_get(net.collections.global_coll, dkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) {
        ZH.e('CENTRAL_DIRTY_DELTAS: ZERO DIRTY_DELTAS');
        next(null, null);
      } else {
        var dmap = gres[0];
        delete(dmap._id);
        var nd = Object.keys(dmap).length;
        if (nd === 0) {
          ZH.e('CENTRAL_DIRTY_DELTAS: ZERO DIRTY_DELTAS');
          next(null, null);
        } else {
          central_dirty_deltas(net, dmap, next);
        }
      }
    }
  });
}

function central_dirty_deltas_daemon() {
  __central_dirty_deltas_daemon(function(nerr, nres) {
    if (nerr) ZH.e('central_dirty_deltas_daemon: ERROR: ' + nerr);
    next_run_central_dirty_deltas();
  });
}

var CentralDirtyDeltasTimer = null;
function next_run_central_dirty_deltas() {
  var min = MinNextCentralDirtyDeltasInterval;
  var max = MaxNextCentralDirtyDeltasInterval;
  var to  = min + ((max - min) * Math.random());
  //ZH.l('next_run_central_dirty_deltas: to: ' + to);
  CentralDirtyDeltasTimer = setTimeout(central_dirty_deltas_daemon, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZCentral.init_complete()
exports.StartCentralDirtyDeltasDaemon = function() {
  next_run_central_dirty_deltas();
}


