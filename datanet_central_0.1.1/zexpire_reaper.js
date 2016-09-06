"use strict";

require('./setImmediate');

var ZPub   = require('./zpublisher');
var ZRAD   = require('./zremote_apply_delta');
var ZXact  = require('./zxaction');
var ZPart  = require('./zpartition');
var ZCLS   = require('./zcluster');
var ZDS    = require('./zdatastore');
var ZQ     = require('./zqueue');
var ZS     = require('./zshared');
var ZH     = require('./zhelper');

var MinNextCentralExpireDaemonInterval = 3000;
var MaxNextCentralExpireDaemonInterval = 5000;

function do_flow_central_expire(net, ks, next) {
  ZH.e('do_flow_central_expire: K: ' + ks.kqk);
  ZDS.ForceRetrieveCrdt(net.plugin, net.collections, ks, function(gerr, crdt) {
    if (gerr) next(gerr, null);
    else {
      if (!crdt) next(null, null); // EXPIRE-REMOVE RACE CONDITION
      else {
        var rchans = crdt._meta.replication_channels;
        var gcv    = crdt._meta.GC_version;
        ZPub.GetSubs(net.plugin, net.collections, ks, rchans,
        function(ferr, subs) {
          if (ferr) next(ferr, null);
          else {
            var md = {ocrdt : crdt, gcv : gcv, subs : subs};
            var new_count = 1; // DO_GC consists of ONE OP
            var cmeta     = crdt._meta;
            ZXact.CreateAutoDentry(net, ks, cmeta, new_count, false,
            function(aerr, dentry) {
              if (aerr) next(aerr, null);
              else {
                var dmeta        = dentry.delta._meta;
                dmeta.remove     = true;
                dmeta.GC_version = gcv;
                ZRAD.ProcessAutoDelta(net, ks, md, dentry, next);  
              }
            });
          }
        });
      }
    }
  });
}

exports.FlowCentralExpire = function(plugin, collections, qe, next) {
  var net    = qe.net;
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var ks     = qe.ks;
  do_flow_central_expire(net, ks, function(serr, sres) {
    qnext(serr, qhres);
    next (null, null);
  });
}

function do_central_expire(net, ks, next) {
  var data = {};
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks,
                               'CENTRAL_EXPIRE',
                               net, data, ZH.NobodyAuth, {}, next);
  var flow = {k : ks.kqk,
              q : ZQ.StorageKeySerializationQueue,
              m : ZQ.StorageKeySerialization,
              f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function do_central_expires(net, kss, next) {
  if (kss.length == 0) next(null, null);
  else {
    var ks = kss.shift();
    do_central_expire(net, ks, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(do_central_expires, net, kss, next);
    });
  }
}

function __central_expire_daemon(next) {
  if (!ZH.AmStorage) throw(new Error("__central_expire_daemon LOGIC ERROR"));
  if (ZH.CentralDisableCentralExpireDaemon ||
      ZH.ClusterNetworkPartitionMode       ||
      ZH.ClusterVoteInProgress             ||
      ZH.GeoVoteInProgress                 ||
      !ZCLS.AmPrimaryDataCenter())           {
    return next(null, null);
  }
  var net  = ZH.CreateNetPerRequest(ZH.Central);
  var lkey = ZS.LRUIndex;
  var nows = ZH.GetMsTime() / 1000;
  net.plugin.do_sorted_range(net.collections.global_coll, lkey, 0, nows,
  function(gerr, kqks) {
    if (gerr) next(gerr, null);
    else {
      var kss = [];
      for (var i = 0; i < kqks.length; i++) {
        var kqk   = kqks[i];
        var ks    = ZH.ParseKQK(kqk);
        var cnode = ZPart.GetKeyNode(ks);
        if (cnode && cnode.device_uuid === ZH.MyUUID) kss.push(ks);
      }
      if (kss.length === 0) {
        ZH.e('CENTRAL_EXPIRE: ZERO LOCAL EXPIRED_KEYS');
      } else {
        ZH.e('CENTRAL_EXPIRE: #Ks: ' + kss.length);
        ZH.p(kss);
      }
      do_central_expires(net, kss, next);
    }
  });
}

function central_expire_daemon() {
  __central_expire_daemon(function(nerr, nres) {
    if (nerr) ZH.e('central_expire_daemon: ERROR: ' + nerr);
    next_run_central_expire_daemon();
  });
}

var CentralExpireDaemonTimer = null;
function next_run_central_expire_daemon() {
  var min = MinNextCentralExpireDaemonInterval;
  var max = MaxNextCentralExpireDaemonInterval;
  var to  = min + ((max - min) * Math.random());
  CentralExpireDaemonTimer = setTimeout(central_expire_daemon, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZCentral.init_complete()
exports.StartCentralExpireDaemon = function() {
  next_run_central_expire_daemon();
}
