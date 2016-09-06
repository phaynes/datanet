"use strict"

require('./setImmediate');

var ZCloud   = require('./zcloud_server');
var ZMCV     = require('./zmethods_cluster_vote');
var ZPio     = require('./zpio');
var ZCache   = require('./zcache');
var ZCLS     = require('./zcluster');
var ZPart    = require('./zpartition');
var ZASC     = require('./zapp_server_cluster');
var ZAio     = require('./zaio');
var ZPLI     = require('./zplugin_init');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT ---------------------------------------------------------------------

ZH.CentralDisableGeoNetworkPartitions     = true;
ZH.CentralDisableCentralNetworkPartitions = true;

var MemcacheClusterNamespaceName  = "MEMCACHE_SERVER_CLUSTER";
var MemcacheClusterDBCname        = "ZyncGlobal";  // NOTE: FIXED NAME
var MemcacheClusterXname          = "global";      // NOTE: FIXED NAME
var MemcacheClusterCname          = "global_coll"; // NOTE: FIXED NAME

var MemcacheCluster             = this;
MemcacheCluster.ready           = true;
ZH.CentralSynced                = true;
MemcacheCluster.net             = {};   // Grab-bag for [plugin, collections]
MemcacheCluster.net.collections = {};   // Grab-bag for [plugin, collections]
MemcacheCluster.net.plugin      = null;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function summarize_new_memcache_cluster(clname, ascname, cnodes, now) {
  var mstate = {cluster_name            : clname,
                app_server_cluster_name : ascname,
                cluster_nodes           : cnodes,
                cluster_born            : now};
  return mstate;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SEND CENTRAL ANNOUNCE NEW MEMCACHE CLUSTER --------------------------

var ResendNewMCCTimer = null;
var ResendNewMCCSleep = 5000;
function announce_new_MCC_processed(net, err, hres) {
  ZH.l('<<<<(C): announce_new_MCC_processed');
  if (ResendNewMCCTimer) { // CANCEL RESEND
    clearTimeout(ResendNewMCCTimer);
    ResendNewMCCTimer = null;
  }
}

function do_agent_announce_new_memcache_cluster(net, clname, ascname,
                                                cnodes, next) {
  if (ResendNewMCCTimer) { // CANCEL RESEND
    clearTimeout(ResendNewMCCTimer);
    ResendNewMCCTimer = null;
  }
  // NOTE: ZAio.SendCentralAnnounceNewMemcacheCluster() is ASYNC
  ZAio.SendCentralAnnounceNewMemcacheCluster(net.plugin, net.collections,
                                             clname, ascname, cnodes,
  function(err, hres) { // NOTE: CLOSURE as 'next' argument
    announce_new_MCC_processed(net, err, hres);
  });
  // NOTE: ResendNewMCCTimer's function is ASYNC
  ResendNewMCCTimer = setTimeout(function() { // If not ACKed -> RESEND
    ZH.e('TIMEOUT -> RESENDING AgentAnnounceNewMemcacheCluster');
    do_agent_announce_new_memcache_cluster(net, clname, ascname,
                                           cnodes, ZH.OnErrLog);
  }, ResendNewMCCSleep);
  next(null, null);
} 

exports.AgentAnnounceNewMemcacheCluster = function(net, next) {

  var conf    = ZH.Agent.MemcacheClusterConfig;
  var clname  = conf.name;
  var ascname = conf.app_server_cluster_name;
  var cnodes  = ZCLS.ClusterNodes;
  var mstate  = summarize_new_memcache_cluster(clname, ascname, cnodes);
  save_new_memcache_cluster(mstate, function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      do_agent_announce_new_memcache_cluster(net, clname, ascname,
                                             cnodes, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT INITIALIZATION ------------------------------------------------------

function save_last_cluster_vote_summary(net, next) {
  var ptbl   = ZPart.PartitionTable;
  var cnodes = ZCLS.ClusterNodes;
  var skey   = ZS.GetLastClusterVoteSummary(ZH.MyUUID);
  var sentry = {cluster_nodes : cnodes, partition_table : ptbl};
  //ZH.e('save_last_cluster_vote_summary'); ZH.e(sentry);
  net.plugin.do_set(net.collections.global_coll, skey, sentry, next);
}

function get_last_cluster_vote_summary(net, next) {
  var skey = ZS.GetLastClusterVoteSummary(ZH.MyUUID);
  net.plugin.do_get(net.collections.global_coll, skey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var cvsumm = gres[0];
        next(null, cvsumm);
      }
    }
  });
}

function calculate_negative_partition_diff(nptbl) {
  var xprts   = [];
  var my_slot = -1;
  for (var i = 0; i < ZCLS.ClusterNodes.length; i++) {
    var cnode = ZCLS.ClusterNodes[i];
    if (cnode.device_uuid === ZH.MyUUID) {
      my_slot = i;
      break;
    }
  }
  if (my_slot === -1) throw(new Error("calculate_old_new_partition_diff"));
  for (var i = 0; i < nptbl.length; i++) {
    if (nptbl[i].master !== my_slot) xprts.push(i);
  }
  return xprts;
}

function get_sticky_cache_keys_to_evict(net, xpmap, keys, ekqks, next) {
  if (keys.length === 0) next(null, null);
  else {
    var key  = keys.shift();
    var kkey = key._id;
    var res  = kkey.split("_");
    res.shift();
    var kqk  = res.join("_");
    var prt  = ZPart.GetPartitionFromKqk(kqk);
    if (!xpmap[prt]) {
      setImmediate(get_sticky_cache_keys_to_evict, net,
                   xpmap, keys, ekqks, next);
    } else {
      var ks   = ZH.ParseKQK(kqk);
      var kkey = ZS.GetKeyInfo(ks);
      net.plugin.do_get_field(net.collections.kinfo_coll, kkey, "sticky",
      function(gerr, sticky) {
        if (gerr) next(gerr, null);
        else {
          ZH.l('get_sticky__to_evict: K: ' + ks.kqk + ' STICKY: ' + sticky);
          if (sticky) ekqks.push(kqk); // MISS & STICKY -> EVICT
          setImmediate(get_sticky_cache_keys_to_evict, net,
                       xpmap, keys, ekqks, next);
        }
      });
    }
  }
}

function evict_keys(net, ekqks, next) {
  if (ekqks.length == 0) next(null, null);
  else {
    var ekqk = ekqks.shift();
    var ks   = ZH.ParseKQK(ekqk);
    ZH.l('CLUSTER-REORG: STICKY-EVICT: K: ' + ks.kqk);
    ZCache.AgentEvict(net, ks, false, {}, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(evict_keys, net, ekqks, next);
    });
  }
}

function do_adjust_local_keys(net, xpmap, next) {
  net.plugin.do_scan(net.collections.kinfo_coll, function(kerr, keys) {
    if (kerr) next(kerr, null);
    else {
      var ekqks = [];
      get_sticky_cache_keys_to_evict(net, xpmap, keys, ekqks,
      function(gerr, gres) {
        if (gerr) next(gerr, null);
        else      evict_keys(net, ekqks, next);
      });
    }
  });
}

function do_adjust_local_caches(net, xprts, next) {
  var xpmap = {};
  for (var i = 0; i < xprts.length; i++) xpmap[xprts[i]] = true;
  do_adjust_local_keys(net, xpmap, next);
}

function do_adjust_local_cache_to_new_partition_table(net, next) {
  get_last_cluster_vote_summary(net, function(gerr, cvsumm) {
    if (gerr) next(gerr, null);
    else {
      if (!cvsumm) next(null, null);
      else {
        var nptbl = ZPart.PartitionTable;
        var xprts = calculate_negative_partition_diff(nptbl);
        //ZH.e('nptbl'); ZH.e(nptbl); ZH.e('xprts'); ZH.e(xprts);
        next(null, null);
        // NOTE: do_adjust_local_caches() is ASYNC
        do_adjust_local_caches(ZH.Agent.net, xprts, ZH.OnErrLog);
      }
    }
  });
}

exports.AdjustLocalCacheToNewPartitionTable = function(net, next) {
  do_adjust_local_cache_to_new_partition_table(net, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else      save_last_cluster_vote_summary(net, next);
  });
}

exports.ReinitializeMemcacheCluster = function(net, next) {
  if (!ZH.Agent.MemcacheClusterConfig) return next(null, null);
  ZH.e('ZMCC.ReinitializeMemcacheCluster');
  exports.AdjustLocalCacheToNewPartitionTable(net, next);
}

function init_memcache_cluster_database_conn(db, next) {
  var me   = this;
  var info = {namespace : MemcacheClusterNamespaceName,
              xname     : MemcacheClusterXname,
              cname     : MemcacheClusterCname,
              dbcname   : MemcacheClusterDBCname};
  ZPLI.InitPluginConnection(db, info, function(ierr, zhndl) {
    if (ierr) next(ierr, null);
    else {
      MemcacheCluster.net.zhndl       = zhndl;
      MemcacheCluster.net.plugin      = zhndl.plugin;
      MemcacheCluster.net.db_handle   = zhndl.db_handle;
      MemcacheCluster.net.collections = zhndl;
      next(null, null);
    }
  });
}

var Inited = false;

function start_memcache_cluster_daemon(me, conf, next) {
  ZH.e('start_memcache_cluster_daemon');
  Inited = true;
  var db = conf.database;
  init_memcache_cluster_database_conn(db, function(ierr, ires) {
    if (ierr) next(ierr, null);
    else {
      // NOTE: ZCLS.Initialize() is ASYNC
      ZCLS.Initialize(MemcacheCluster.net, MyMemcacheClusterNode,
                      ZCloud.HandleClusterMethod, ZH.OnErrLog);
      next(null, null);
    }
  });
}

var MyMemcacheClusterNode = null;

exports.StartAppMemcacheClusterDaemon = function(net, jconf) {
  if (!ZH.Agent.MemcacheClusterConfig) return;
  if (Inited)                          return;
  ZCloud.Initialize(MemcacheCluster);
  ZCloud.InitializeMethods(ZMCV.Methods, ZH.OnErrLog);
  var me                = this;
  var conf              = ZH.Agent.MemcacheClusterConfig;
  MyMemcacheClusterNode = {name         : conf.name,
                           datacenter   : ZH.Agent.datacenter,
                           device_uuid  : ZH.MyUUID,
                           backend      : conf.backend,
                           fip          : jconf.agent.request_ip,
                           fport        : jconf.agent.port,
                           memcached    : {ip   : net.plugin.ip,
                                           port : net.plugin.port}
                          };
  start_memcache_cluster_daemon(me, conf, ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCIBER HANDLE ANNOUNCE NEW MEMCACHE CLUSTER ----------------------------

function save_new_memcache_cluster(mstate, next) {
  var net    = ZH.Agent.net; // NOTE: NOT MemcacheCluster.net
  var clname = mstate.cluster_name;
  var skey   = ZS.GetMemcacheClusterState(clname);
  //ZH.l('save_new_memcache_cluster'); ZH.p(mstate);
  net.plugin.do_set(net.collections.global_coll, skey, mstate, next);
}

exports.HandleSubscriberNewMemcacheCluster = function(net, mstate, hres, next) {
  save_new_memcache_cluster(mstate, function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      ZASC.HandleNewMemcacheCluster(net, mstate, function(aerr, ares) {
        next(aerr, hres);
      });
    }
  });
}

exports.AgentGetMemcacheClusterState = function(net, clname, hres, next) {
  ZH.l('ZMCC.AgentGetMemcacheClusterState: CL: ' + clname);
  var skey = ZS.GetMemcacheClusterState(clname);
  net.plugin.do_get(net.collections.global_coll, skey, function(gerr, gres) {
    if (gerr) next(gerr, hres);
    else {
      if (gres.length !== 0) {
        hres.mstate = gres[0];
        delete(hres.mstate._id);
      }
      next(null, hres);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE ANNOUNCE NEW MEMCACHE CLUSTER ------------------------------

function announce_memcache_cluster_to_app_server_cluster(net, mstate,
                                                         hres, next) {
  // NOTE: ZPio.BroadcastAnnounceNewMemcacheCluster() is ASYNC
  ZPio.BroadcastAnnounceNewMemcacheCluster(net.plugin, net.collections, mstate);
  next(null, hres);
}

exports.CentralHandleAnnounceNewMemcacheCluster = function(net, clname, ascname,
                                                           cnodes, hres, next) {
  var now    = ZH.GetMsTime();
  var skey   = ZS.GetMemcacheClusterState(clname);
  var mstate = summarize_new_memcache_cluster(clname, ascname, cnodes, now);
  ZH.l('ZMCC.CentralHandleAnnounceNewMemcacheCluster'); ZH.p(mstate);
  net.plugin.do_set(net.collections.global_coll, skey, mstate,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      if (!ZH.Central.AppServerClusterConfig) next(null, hres);
      else {
        announce_memcache_cluster_to_app_server_cluster(net, mstate,
                                                        hres, next);
      }
    }
  });
}

// NOTE: Used by ZASC.CentralHandleAnnounceNewAppServerCluster()
exports.ReBroadcastAnnounceNewMemcacheCluster = function(net, mcname,
                                                         hres, next) {
  var skey = ZS.GetMemcacheClusterState(mcname);
  net.plugin.do_get(net.collections.global_coll, skey, function(gerr, gres) {
    if (gerr) next(gerr, hres);
    else {
      if (gres.length === 0) next(null, hres);
      else {
        var mstate = gres[0];
        announce_memcache_cluster_to_app_server_cluster(net, mstate,
                                                        hres, next);
      }
    }
  });
}


