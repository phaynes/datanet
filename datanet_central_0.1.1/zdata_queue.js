"use strict"

require('./setImmediate');

var ZPlugins = require('./plugins/plugins');
var ZPub     = require('./zpublisher');
var ZNM      = require('./zneedmerge');
var ZPio     = require('./zpio');
var ZGack    = require('./zgack');
var ZDConn   = require('./zdconn');
var ZSumm    = require('./zsummary');
var ZCSub    = require('./zcluster_subscriber');
var ZDelt    = require('./zdeltas');
var ZChannel = require('./zchannel');
var ZSU      = require('./zstationuser');
var ZUM      = require('./zuser_management');
var ZCLS     = require('./zcluster');
var ZPart    = require('./zpartition');
var ZQ       = require('./zqueue');
var ZH       = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var Queue = {NamespaceName    : "QUEUE",
             CollectionName   : "MESSAGES",
             StorageKeyName   : "STORAGE",
             StorageFieldName : "VALUES",
             RouterKeyName    : "ROUTER",
             RouterFieldName  : "VALUES",
            };

var NotReadyToBlockSleep = 1000;

exports.RouterSprinklerSleep = 2500;
if (ZH.TestNotReadyAgentRecheck) {
  exports.RouterSprinklerSleep = 20000;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function get_key_topic_name_from_jrpc_body(jrpc_body) {
  var ks = jrpc_body.params.data.ks;
  if (!ks) return null;
  //TODO STORAGE wants to use RouterPartition
  //     ROUTER  wants to use StoragePartition
  return ZPart.GetKeyPartition(ks);
}

function get_subscriber_topic_name_from_jrpc_body(jrpc_body) {
  var suuid = jrpc_body.params.data.subscriber;
  if (!suuid) return null;
  return ZPart.GetClusterPartition(suuid);
}

var RegisteredCallbacks = {};
function register_queue_callback(id, hres, next) {
  RegisteredCallbacks[id] = {hres : hres, next : next};
}

function handle_ack_callback(net, params, id, puuid, rfield, next) {
  //TODO sanity check that ZH.MyUUID sent the original NeedMerge
  var rpcid = params.data.id;
  var cb    = RegisteredCallbacks[rpcid];
  if (!cb) { // NOTE: on cluster-reorg router callbacks get criss-crossed
    ZH.e('Missing Router Callback: RPC-ID: ' + rpcid);
    next(null, null);
  } else {
    cb.hres[rfield] = params.data[rfield];
    cb.next(null, cb.hres);
    next(null, null);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZATION ------------------------------------------------------------

function handle_no_op(net, params, id, puuid, next) {
  ZH.l('handle_no_op: ID: ' + id);
  CherryBlock = true;
  next(null, null);
}

function get_topic_name(cname, kname, qid) {
  return "INFO_" + cname + "_" + kname + "|" + qid;
}

function get_server_queue_name(duuid, is_router) {
  var cname = Queue.CollectionName;
  var kname = is_router ? Queue.RouterKeyName : Queue.StorageKeyName;
  var qid   = is_router ? "ROUTER_" + duuid   : "STORAGE_" + duuid;
  return get_topic_name(cname, kname, qid);
}

function get_leader_topic(is_router) {
  return is_router ? "ROUTER_CLUSTER_LEADER" : "STORAGE_CLUSTER_LEADER";
}

var CherryBlock = true;
var RetryTimer  = null;

function get_data_topics(prts, am_router) {
  var kname  = am_router ? Queue.RouterKeyName : Queue.StorageKeyName;
  var tnames = [];
  // SERVER SELF REFERENTIAL TOPIC - must be first for REDIS
  tnames.push(get_server_queue_name(ZH.MyUUID, am_router));
  if (ZH.AmClusterLeader) tnames.push(get_leader_topic(am_router));
  for (var i = 0; i < prts.length; i++) {
    var tname = get_topic_name(Queue.CollectionName, kname, prts[i]);
    tnames.push(tname);
  }
  return tnames;
}

function process_queue_request(am_router, entry, next) {
  var net    = ZH.CreateNetPerRequest(ZH.Central);
  var tname  = entry.key;
  var data   = entry.value;
  var method = data.method;
  var params = data.params;
  var id     = data.id;
  var puuid  = params.data.device.uuid;
  if (am_router) ZH.l('ROUTER-POP: T: ' + tname + ' P: ' + puuid);
  else           ZH.l('STORAGE-POP: T: ' + tname + ' P: ' + puuid);
  if (method !== "StorageSprinkle") ZH.p(data);
  var m = am_router ? RouterConsumers[method] : StorageConsumers[method];
  if (!m) throw(new Error('-ERROR: Method: ' + method + ' NOT SUPPORTED'));
  m.handler(net, params, id, puuid, next);
}

function block_on_data_topics(me, plugin) {
  if (RetryTimer) {
    clearTimeout(RetryTimer);
    RetryTimer = null;
  }
  ZH.l('block_on_data_topics: START');
  var prts = ZPart.GetMyPartitions();
  if (prts.length === 0) {
    ZH.e('NOT READY TO BLOCK ON QUEUE -> NO CLUSTER PARTITIONS YET');
    RetryTimer = setTimeout(function() {
      block_on_data_topics(me, plugin);
    }, NotReadyToBlockSleep);
    return;
  }
  var tnames = get_data_topics(prts, ZH.AmRouter);
  ZH.l('block_on_data_topics: BLOCKING');
  if (CherryBlock) {
    ZH.l('TOPICS'); ZH.p(tnames);
    CherryBlock = false;
  }
  plugin.do_queue_pop(tnames, 0, function(perr, entry) {
    if (perr) {
      ZH.e(perr);
      block_on_data_topics(me, plugin);
    } else {
      process_queue_request(ZH.AmRouter, entry, function(serr, sres) {
        if (serr) ZH.e(serr);
        block_on_data_topics(me, plugin);
      });
    }
  });
}

function init_data_queue_collections(me, plugin, db_handle, cname, next) {
  var do_coll = plugin.do_create_collection;
  me[cname]   = do_coll(db_handle, Queue.CollectionName, true);
  next(null, null);
}

function init_queue_plugin(me, name, ip, port, xname, cname, block, next) {
  var plugin   = ZPlugins.GetPlugin(name);
  plugin.ip    = ip;
  plugin.port  = port;
  plugin.xname = xname;
  plugin.do_open(plugin.ip, plugin.port, Queue.NamespaceName, plugin.xname,
  function(cerr, db_handle){
    if (cerr) next(cerr, null);
    else {
      ZH.e('CONNECTED TO QUEUE: IP: ' + plugin.ip + ' P: ' + plugin.port);
      init_data_queue_collections(me, plugin, db_handle, cname,
      function(ierr, ires) {
        if (ierr) next(ierr, null);
        else {
          // NOTE: block_on_data_topics() is ASYNC
          if (block) block_on_data_topics(me, plugin);
          next(null, plugin);
        }
      });
    }
  });
}

exports.InitDataQueueConnection = function(dq, next) {
  var me    = this;
  var block = !ZH.AmRouter; // STORAGE BLOCKS on "storage_queue"
  init_queue_plugin(me, dq.name, dq.ip, dq.port,
                    "storage_queue", "storage_queue_coll", block,
  function(ierr, plugin) {
    if (ierr) next(ierr, null);
    else {
      me.storage_plugin = plugin;
      block             = ZH.AmRouter; // ROUTER blocks on "router_queue"
      init_queue_plugin(me, dq.name, dq.ip, dq.port,
                        "router_queue", "router_queue_coll", block,
      function(qerr, rplugin) {
        if (qerr) next(qerr, null);
        else {
          me.router_plugin = rplugin;
          // NOTE: start_router_sprinkler() is ASYNC
          start_router_sprinkler();
          next(null, null);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROUTER SPRINKLER ----------------------------------------------------------

function sprinkle_topics(net, tnames, when, next) {
  if (tnames.length === 0) next(null, null);
  else {
    var tname = tnames.shift();
    exports.PushStorageSprinkler(net.plugin, net.collections, tname, when,
    function(perr, pres) {
      if (perr) next(perr, null);
      else      setImmediate(sprinkle_topics, net, tnames, when, next);
    });
  }
}

function do_router_sprinkler(net, next) {
  if (ZH.CentralDisableRouterSprinkler) return next(null, null);
  var prts = ZPart.GetMyPartitions();
  if (prts.length === 0) {
    next(new Error("ROUTER SPRINKLER - NO CLUSTER PARTITIONS YET"), null);
  } else {
    var tnames = get_data_topics(prts, false);
    var when   = ZH.GetMsTime();
    sprinkle_topics(net, tnames, when, next);
  }
}

function start_router_sprinkler() {
  if (ZH.AmStorage) return;
  var net = ZH.CreateNetPerRequest(ZH.Central);
  do_router_sprinkler(net, function(serr, sres) {
    if (serr) ZH.e(serr.message);
    setTimeout(start_router_sprinkler, exports.RouterSprinklerSleep);
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REINITIALIZE DATA QUEUE ---------------------------------------------------

/* NOTE: BLPOP in redis can not be cancelled, so we use a trick
         The trick is to LEFT PUSH to the FIRST of the BLOCKED KEYS
         which effectively jumps to the head of the topic
         which will run NoOp() then call block_on_data_topics()
         which then blocks on the NEW partitions
*/
exports.ReinitializeQueueConnection = function(plugin, tname, next) {
  var cnode     = ZCLS.MyNode;
  var method    = 'NoOp';
  var auth      = ZH.NobodyAuth;
  var data      = {device : {uuid : ZH.MyUUID}};
  var id        = ZDelt.GetNextRpcID(null, null, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  ZH.l('ZDQ.ReinitializeQueueConnection: tname: ' + tname);
  ZH.p(jrpc_body);
  plugin.do_queue_unshift(tname, jrpc_body, next);
}

exports.ReinitializeDataQueueConnection = function(next) {
  var me      = this;
  var plugin  = ZH.AmRouter ? me.storage_plugin : me.router_plugin;
  var tname   = get_server_queue_name(ZH.MyUUID, ZH.AmRouter);
  CherryBlock = true;
  exports.ReinitializeQueueConnection(plugin, tname, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE QUEUE RESPONSE CONSUMERS ------------------------------------------

function handle_storage_agent_online(net, params, id, puuid, next) {
  var hres    = {id : id};
  var b       = params.data.value;
  var agent   = params.data.agent;
  var perm    = params.data.permissions;
  var schans  = params.data.subscriptions;
  var susers  = params.data.stationedusers;
  var created = params.data.created;
  ZDConn.HandleStorageAgentOnline(net, b, agent, perm, schans,
                                  susers, created, hres,
  function(serr, data) {
    if (serr) next(serr, null);
    else      exports.PushRouterAckAgentOnline(data, puuid, next);
  });
}

function handle_storage_agent_recheck(net, params, id, puuid, next) {
  var hres    = {id : id};
  var agent   = params.data.agent;
  var created = params.data.created;
  ZDConn.HandleStorageAgentRecheck(net.plugin, net.collections,
                                   agent, created, hres,
  function(serr, data) {
    if (serr) next(serr, null);
    else      exports.PushRouterAckAgentRecheck(data, puuid, next);
  });
}

function handle_storage_get_agent_keys(net, params, id, puuid, next) {
  var hres   = {id : id};
  var agent  = params.data.agent;
  var nkeys  = params.data.num_keys;
  var minage = params.data.minimum_age;
  var wonly  = params.data.watch_only;
  ZDConn.HandleStorageGetAgentKeys(net.plugin, net.collections,
                                   agent, nkeys, minage, wonly, hres,
  function(serr, data) {
    if (serr) next(serr, null);
    else      exports.PushRouterAckGetAgentKeys(data, puuid, next);
  });
}

function handle_storage_geo_data_center_online(net, params, id, puuid, next) {
  var hres     = {id : id};
  var rguuid   = params.data.destination;
  var dkeys    = params.data.device_keys;
  ZDConn.HandleStorageGeoDataCenterOnline(net, rguuid, dkeys, hres,
  function(serr, data) {
    if (serr) next(serr, null);
    else      exports.PushRouterAckDataCenterOnline(data, puuid, next);
  });
}

function post_storage_need_merge_processed(serr, data, puuid, is_geo, next) {
  if (serr) next(serr, null);
  else {
    exports.PushRouterAckNeedMerge(data, puuid, is_geo, next);
  }
}

function do_handle_storage_need_merge(net, params, id, puuid, is_geo, next) {
  var hres   = {id : id};
  var auth   = params.authentication;
  var ks     = params.data.ks;
  var gcv    = params.data.gc_version;
  var agent  = params.data.agent;
  var guuid  = params.data.datacenter;
  var rid    = params.data.request_id;
  var device = params.data.device;     // Used to reply to correct ClusterNode
  var data   = {gc_version : gcv,
                agent      : agent,
                datacenter : guuid,
                request_id : rid,
                device     : device};
  var mname  = is_geo ? 'STORAGE_GEO_NEED_MERGE' :
                        'STORAGE_NEED_MERGE';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, auth, hres,
    function(serr, data) { // NOTE: Closure as 'next argument
      post_storage_need_merge_processed(serr, data, puuid, is_geo, next);
    });
  var flow = {k : ks.kqk,
              q : ZQ.StorageKeySerializationQueue,
              m : ZQ.StorageKeySerialization,
              f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function handle_storage_need_merge(net, params, id, puuid, next) {
  do_handle_storage_need_merge(net, params, id, puuid, false, next);
}

function handle_storage_geo_need_merge(net, params, id, puuid, next) {
  if (ZH.ChaosMode === 25) {
    ZH.e('CHAOS-MODE: 25: Sleeping 30 seconds on STORAGE_GEO_NEED_MERGE');
    setTimeout(function() {
      do_handle_storage_need_merge(net, params, id, puuid, true, next);
    }, 30000);
  } else {
    do_handle_storage_need_merge(net, params, id, puuid, true, next);
  }
}

function handle_storage_find(net, params, id, puuid, next) {
  var hres  = {id : id};
  var auth  = params.authentication;
  var ns    = params.data.namespace;
  var cn    = params.data.collection;
  var query = params.data.query;
  ZNM.HandleStorageFind(net, ns, cn, query, auth, hres, function(serr, data) {
    if (serr) next(serr, null);
    else      exports.PushRouterAckFind(data, puuid, next);
  });
}

function post_storage_cluster_cache_processed(serr, data, puuid, next) {
  if (serr) next(serr, null);
  else      exports.PushRouterAckClusterCache(data, puuid, next);
}

function handle_storage_cluster_cache(net, params, id, puuid, next) {
  var hres      = {id : id};
  var auth      = params.authentication;
  var ks        = params.data.ks;
  var watch     = params.data.watch;
  var agent     = params.data.agent;
  var need_body = params.data.need_body;
  var data      = {ks        : ks,
                   watch     : watch,
                   agent     : agent,
                   need_body : need_body};
  var mname     = 'STORAGE_CLUSTER_CACHE';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, auth, hres,
    function(serr, data) { // NOTE: Closure as 'next argument
      post_storage_cluster_cache_processed(serr, data, puuid, next);
    });
  var flow = {k : ks.kqk,
              q : ZQ.StorageKeySerializationQueue,
              m : ZQ.StorageKeySerialization,
              f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function post_storage_status_repeat_delta_processed(serr, data, puuid, next) {
  if (serr) next(serr, null);
  else      exports.PushRouterAckStatusRepeatDelta(data, puuid, next);
}

function post_storage_cluster_client_delta_processed(serr, data, puuid, next) {
  if (serr) next(serr, null);
  else      exports.PushRouterAckClusterClientCall(data, puuid, next);
}

function handle_storage_cluster_client_call(net, params, id, puuid, next) {
  var hres    = {id : id};
  var auth    = params.authentication;
  var ks      = params.data.ks;
  var fetch   = params.data.fetch;
  var store   = params.data.store;
  var commit  = params.data.commit;
  var scommit = params.data.stateless_commit;
  var remove  = params.data.remove;
  var json    = params.data.json;
  var crdt    = params.data.crdt;
  var oplog   = params.data.oplog;
  var rchans  = params.data.replication_channels;
  var data    = {fetch   : fetch,   store  : store,  commit : commit,
                 scommit : scommit, remove : remove, json   : json,
                 crdt    : crdt,    oplog  : oplog,  rchans : rchans};
  var mname   = 'STORAGE_CLUSTER_CLIENT_CALL';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, auth, hres,
    function(serr, data) { // NOTE: Closure as 'next argument
      post_storage_cluster_client_delta_processed(serr, data, puuid, next);
    });
  var flow = {k : ks.kqk,
              q : ZQ.StorageKeySerializationQueue,
              m : ZQ.StorageKeySerialization,
              f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ASYNC STORAGE QUEUE CONSUMERS ---------------------------------------------

function handle_storage_cluster_evict_method(net, mname, params,
                                             id, puuid, next) {
  var hres   = {id : id};
  var auth   = params.authentication;
  var ks     = params.data.ks;
  var agent  = params.data.agent
  var data   = {ks : ks, agent : agent};
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, auth, hres, next);
  var flow = {k : ks.kqk,
              q : ZQ.StorageKeySerializationQueue,
              m : ZQ.StorageKeySerialization,
              f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function handle_storage_cluster_evict(net, params, id, puuid, next) {
  var mname  = 'STORAGE_CLUSTER_EVICT';
  handle_storage_cluster_evict_method(net, mname, params, id, puuid, next);
}

function handle_storage_cluster_local_evict(net, params, id, puuid, next) {
  var mname  = 'STORAGE_CLUSTER_LOCAL_EVICT';
  handle_storage_cluster_evict_method(net, mname, params, id, puuid, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE QUEUE CONSUMERS ---------------------------------------------------

function handle_storage_apply_delta(net, params, id, puuid, next) {
  var ks     = params.data.ks;
  ZH.l('handle_storage_apply_delta: K: ' + ks.kqk);
  var dentry = params.data.dentry;
  var rguuid = params.data.datacenter;
  var data   = {dentry : dentry, rguuid : rguuid};
  var mname  = 'STORAGE_APPLY_DELTA';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, next);
  var flow   = {k : ks.kqk,
                q : ZQ.StorageKeySerializationQueue,
                m : ZQ.StorageKeySerialization,
                f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function handle_storage_apply_dentries(net, params, id, puuid, next) {
  var ks       = params.data.ks;
  ZH.l('handle_storage_apply_dentries: K: ' + ks.kqk);
  var dentries = params.data.dentries;
  var rguuid   = params.data.datacenter;
  var data     = {dentries : dentries, rguuid : rguuid};
  var mname    = 'STORAGE_APPLY_DENTRIES';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, next);
  var flow   = {k : ks.kqk,
                q : ZQ.StorageKeySerializationQueue,
                m : ZQ.StorageKeySerialization,
                f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function handle_storage_freeze_key(net, params, id, puuid, next) {
  var ks       = params.data.ks;
  ZH.l('handle_storage_freeze_key: K: ' + ks.kqk);
  var data     = {};
  var mname    = 'STORAGE_FREEZE_KEY';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, next);
  var flow   = {k : ks.kqk,
                q : ZQ.StorageKeySerializationQueue,
                m : ZQ.StorageKeySerialization,
                f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

// NOTE: ZQ.StorageKeySerialization done in ZGack.HandleStorageAckGeoDelta()
function handle_storage_ack_geo_delta(net, params, id, puuid, next) {
  var ks     = params.data.ks;
  ZH.l('<-|(R): StorageAckGeoDelta: K: ' + ks.kqk);
  var berr  = params.data.berr;
  var dres  = params.data.dres;
  ZGack.HandleStorageAckGeoDelta(net, ks, berr, dres, next);
}

// NOTE: ZQ.StorageKeySerialization done in
//       ZH.Central.HandleStorageClusterGeoCommitDelta()
function handle_storage_geo_commit_delta(net, params, id, puuid, next) {
  var ks       = params.data.ks;
  ZH.l('<-|(R): StorageGeoCommitDelta: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var rguuid = params.data.datacenter;
  ZH.Central.HandleStorageClusterGeoCommitDelta(net, ks, author,
                                                rchans, rguuid, next);
}

// NOTE: ZQ.StorageKeySerialization done in
//       ZH.Central.HandleStorageClusterAckGeoCommitDelta()
function handle_storage_ack_geo_commit_delta(net, params, id, puuid, next) {
  var ks       = params.data.ks;
  ZH.l('<-|(R): StorageAckGeoCommitDelta: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var rguuid = params.data.datacenter;
  ZH.Central.HandleStorageClusterAckGeoCommitDelta(net, ks, author, rchans,
                                                   rguuid, next);
}

// NOTE: ZQ.StorageKeySerialization done in
//       ZH.Central.HandleStorageGeoSubscriberCommitDelta()
function handle_storage_geo_subscriber_commit_delta(net, params, id,
                                                    puuid, next) {
  var ks       = params.data.ks;
  ZH.l('<-|(R): StorageGeoSubscriberCommitDelta: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  ZH.Central.HandleStorageGeoSubscriberCommitDelta(net, ks, author, rchans,
                                                   next);
}

function handle_storage_ack_geo_need_merge(net, params, id, puuid, next) {
  var ks = params.data.ks;
  ZH.l('<-|(R): StorageAckGeoNeedMerge: K: ' + ks.kqk);
  var md = params.data.merge_data;
  ZNM.HandleStorageAckGeoNeedMerge(net, ks, md, next);
}

function handle_storage_set_device_keys(net, params, id, puuid, next) {
  var hres  = {id : id};
  var dkeys = params.data.device_keys;
  ZDConn.HandleStorageDeviceKeys(net, dkeys, next);
}

function handle_storage_data_center_online_response(net, params,
                                                    id, puuid, next) {
  var auth   = params.authentication;
  var brbody = params.data.body;
  ZH.l('<-|(R): StorageDataCenterOnlineResponse');
  ZDConn.HandleStorageDataCenterOnlineResponse(net.plugin, net.collections,
                                               brbody, next);
}

function handle_storage_announce_new_geo_cluster(net, params,
                                                 id, puuid, next) {
  var duuid    = params.data.device.uuid;
  var gterm    = params.data.geo_term_number;
  var gnodes   = params.data.geo_nodes;
  ZH.l('<-|(R): StorageAnnounceNewGeoCluster GT: ' + gterm +
       ' #DC: ' + gnodes.length);
  var gnetpart = params.data.geo_network_partition;
  var gmaj     = params.data.geo_majority;
  var csynced  = params.data.cluster_synced;
  var send_dco = params.data.send_datacenter_online;
  ZNM.StoragePrimaryHandleAnnounceNewGeoCluster(net, next);
  // NOTE: ZPio.BroadcastAnnounceNewGeoCluster() is ASYNC
  ZPio.BroadcastAnnounceNewGeoCluster(net.plugin, net.collections,
                                      gterm, gnodes, gnetpart, gmaj,
                                      csynced, send_dco);
}

function handle_storage_geo_broadcast_station_user(net, params,
                                                   id, puuid, next) {
  var auth  = params.authentication;
  ZH.l('<-|(R): StorageGeoBroadcastStationUser UN: ' + auth.username);
  var auuid = params.data.agent.uuid;
  ZSU.HandleStorageGeoStationUser(net, auuid, auth, next);
}

function handle_storage_geo_broadcast_destation_user(net, params,
                                                   id, puuid, next) {
  var auth  = params.authentication;
  ZH.l('<-|(R): StorageGeoBroadcastDestationUser UN: ' + auth.username);
  var auuid = params.data.agent.uuid;
  ZSU.HandleStorageGeoDestationUser(net, auuid, auth, next);
}

function handle_storage_geo_broadcast_add_user(net, params, id, puuid, next) {
  var username = params.data.username;
  var password = params.data.password;
  var role     = params.data.role;
  ZH.l('<-|(R): StorageGeoBroadcastAddUser: UN: ' + username);
  var internal = params.data.internal;
  ZUM.HandleStorageGeoAddUser(net, username, password, role, internal, next);
}

function handle_storage_geo_broadcast_grant_user(net, params, id, puuid, next) {
  var username = params.data.username;
  var schanid  = params.data.channel.id;
  var priv     = params.data.channel.privilege;
  ZH.l('<-|(R): StorageGeoBroadcastGrantUser: UN: ' + username +
       ' R: ' + schanid + ' P: ' + priv);
  var do_unsub = params.data.do_unsubscribe;
  ZUM.HandleStorageGeoGrantUser(net, username, schanid, priv, do_unsub, next);
}

function handle_storage_geo_broadcast_remove_user(net, params,
                                                  id, puuid, next) {
  var username = params.data.username;
  ZH.l('<-|(R): StorageGeoBroadcastRemoveUser: UN: ' + username);
  ZUM.HandleStorageGeoRemoveUser(net, username, next);
}

function handle_storage_geo_broadcast_subscribe(net, params, id, puuid, next) {
  var auth    = params.authentication;
  var auuid   = params.data.agent.uuid;
  var schanid = params.data.channel.id;
  ZH.l('<-|(R): StorageGeoBroadcastSubscribe: U: ' + auuid + ' R: ' + schanid);
  ZChannel.HandleStorageGeoSubscribe(net, auuid, schanid, auth, next);
}

function handle_storage_geo_broadcast_unsubscribe(net, params,
                                                  id, puuid, next) {
  var auth    = params.authentication;
  var auuid   = params.data.agent.uuid;
  var schanid = params.data.channel.id;
  ZH.l('<-|(R): StorageGeoBroadcastUnsubscribe: U: ' + auuid +
       ' R: ' + schanid);
  ZChannel.HandleStorageGeoUnsubscribe(net, auuid, schanid, auth, next);
}

function handle_storage_geo_broadcast_remove_datacenter(net, params,
                                                        id, puuid, next) {
  var dcuuid = params.data.remove_datacenter;
  ZH.l('<-|(R): StorageGeoBroadcastRemoveDataCenter: DC: ' + dcuuid);
  ZCLS.HandleStorageGeoRemoveDataCenter(net, dcuuid, next);
}

function handle_storage_ack_storage_online(net, params, id, puuid, next) {
  handle_ack_callback(net, params, id, puuid, "router_data", next);
}

// NOTE: ZQ.StorageKeySerialization done in ZGack.HandleStorageAckGeoDentries()
function handle_storage_ack_geo_dentries(net, params, id, puuid, next) {
  var ks   = params.data.ks;
  ZH.l('<-|(S): StorageAckGeoDentries: K: ' + ks.kqk);
  var berr = params.data.berr;
  var dres = params.data.dres;
  ZGack.HandleStorageAckGeoDentries(net, ks, berr, dres, next);
}

function handle_storage_sprinkle(net, params, id, puuid, next) {
  var tname = params.data.topic;
  var when  = params.data.when;
  ZSumm.HandleStorageSprinkle(net, tname, when, next);
}

var StorageConsumers = {
      'StorageAgentOnline'              :
        {handler : handle_storage_agent_online},
      'StorageAgentRecheck'             :
        {handler : handle_storage_agent_recheck},
      'StorageGetAgentKeys'             :
        {handler : handle_storage_get_agent_keys},
      'StorageGeoDataCenterOnline'      :
        {handler : handle_storage_geo_data_center_online},
      'StorageApplyDelta'               :
       {handler : handle_storage_apply_delta},
      'StorageApplyDentries'            :
       {handler : handle_storage_apply_dentries},
      'StorageFreezeKey'                :
       {handler : handle_storage_freeze_key},
      'StorageAckGeoDelta'              :
        {handler : handle_storage_ack_geo_delta},
      'StorageGeoCommitDelta'           :
        {handler : handle_storage_geo_commit_delta},
      'StorageAckGeoCommitDelta'        :
        {handler : handle_storage_ack_geo_commit_delta},
      'StorageGeoSubscriberCommitDelta' :
        {handler : handle_storage_geo_subscriber_commit_delta},
      'StorageClusterClientCall'        :
        {handler : handle_storage_cluster_client_call},
      'StorageNeedMerge'                :
        {handler : handle_storage_need_merge},
      'StorageGeoNeedMerge'             :
        {handler : handle_storage_geo_need_merge},
      'StorageAckGeoNeedMerge'          :
        {handler : handle_storage_ack_geo_need_merge},
      'StorageDeviceKeys'               :
        {handler : handle_storage_set_device_keys},
      'StorageFind'                     :
        {handler : handle_storage_find},
      'StorageClusterCache'             :
        {handler : handle_storage_cluster_cache},
      'StorageClusterEvict'             :
        {handler : handle_storage_cluster_evict},
      'StorageClusterLocalEvict'        :
        {handler : handle_storage_cluster_local_evict},
      'StorageDataCenterOnlineResponse' :
        {handler : handle_storage_data_center_online_response},
      'StorageAnnounceNewGeoCluster'   :
        {handler : handle_storage_announce_new_geo_cluster},
      'StorageGeoBroadcastStationUser'  :
        {handler : handle_storage_geo_broadcast_station_user},
      'StorageGeoBroadcastDestationUser' :
        {handler : handle_storage_geo_broadcast_destation_user},
      'StorageGeoBroadcastAddUser'      :
        {handler : handle_storage_geo_broadcast_add_user},
      'StorageGeoBroadcastGrantUser'    :
        {handler : handle_storage_geo_broadcast_grant_user},
      'StorageGeoBroadcastRemoveUser'   :
        {handler : handle_storage_geo_broadcast_remove_user},
      'StorageGeoBroadcastSubscribe'    :
        {handler : handle_storage_geo_broadcast_subscribe},
      'StorageGeoBroadcastUnsubscribe'  :
        {handler : handle_storage_geo_broadcast_unsubscribe},
      'StorageGeoBroadcastRemoveDataCenter' :
        {handler : handle_storage_geo_broadcast_remove_datacenter},
      'StorageAckStorageOnline'         :
        {handler : handle_storage_ack_storage_online},
      'StorageAckGeoDentries'           :
        {handler : handle_storage_ack_geo_dentries},
      'StorageSprinkle'                 :
        {handler : handle_storage_sprinkle},
      'NoOp'                            :
        {handler : handle_no_op},
     };


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROUTER PRODUCER ACKS ------------------------------------------------------

function push_storage_ack(me, method, rdata, puuid, dfield, next) {
  if (!ZH.AmRouter) throw(new Error("LOGIC ERROR(push_storage_ack)"));
  var cnode     = ZCLS.StorageNode;
  var auth      = ZH.NobodyAuth;
  var data      = {id      : rdata.id,
                   device  : {uuid : ZH.MyUUID}};
  data[dfield]  = rdata[dfield];
  var id        = ZDelt.GetNextRpcID(null, null, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var tname     = get_server_queue_name(puuid, false);
  ZH.l('push_storage_ack: tname: ' + tname); ZH.p(jrpc_body);
  if (ZH.AmBoth) {
    setImmediate(function() {
      var key   = tname;
      var entry = {key : key, value : jrpc_body};
      process_queue_request(false, entry, ZH.OnErrLog);
    });
    next(null, null);
  } else {
    me.storage_plugin.do_queue_push(tname, jrpc_body, next);
  }
}

exports.PushStorageAckStorageOnline = function(rdata, puuid, next) {
  var me     = this;
  var method = 'StorageAckStorageOnline';
  push_storage_ack(me, method, rdata, puuid, "router_data", next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROUTER QUEUE RESPONSE CONSUMERS -------------------------------------------

function handle_router_storage_online(net, params, id, puuid, next) {
  var csynced = params.data.central_synced;
  var hres    = {id : id};
  ZCLS.HandleRouterStorageOnline(net.plugin, net.collections, csynced, hres,
  function(serr, data) {
    if (serr) next(serr, null);
    else      exports.PushStorageAckStorageOnline(data, puuid, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROUTER QUEUE CONSUMERS ----------------------------------------------------

function handle_router_ack_on_user_data(net, params, id, puuid, next) {
  handle_ack_callback(net, params, id, puuid, "user_data", next);
}

function handle_router_ack_data_center_online(net, params, id, puuid, next) {
  handle_ack_callback(net, params, id, puuid, "dc_data", next);
  // NOTE: ZPio.GeoBroadcastDeviceKeys() is async
  var ddata = params.data["dc_data"];
  var dkeys = ddata.device_keys;
  ZPio.GeoBroadcastDeviceKeys(net.plugin, net.collections, dkeys);
}

function handle_router_ack_storage_need_merge(net, params, id, puuid, next) {
  handle_ack_callback(net, params, id, puuid, "merge_data", next);
}

function handle_router_ack_find(net, params, id, puuid, next) {
  handle_ack_callback(net, params, id, puuid, "f_data", next);
}

function handle_router_ack_cluster_cache(net, params, id, puuid, next) {
  handle_ack_callback(net, params, id, puuid, "merge_data", next);
}

function handle_router_ack_status_repeat_delta(net, params, id, puuid, next) {
  handle_ack_callback(net, params, id, puuid, "key_data", next);
}

function handle_router_ack_cluster_client_call(net, params, id, puuid, next) {
  handle_ack_callback(net, params, id, puuid, "key_data", next);
}

// NOTE: QUEUE via ZQ.PubSubQueue[]
function handle_router_cluster_subscriber_merge(net, params, id, puuid, next) {
  var ks      = params.data.ks;
  ZH.l('<-|(S): RouterClusterSubscriberMerge: K : ' + ks.kqk);
  var cavrsns = params.data.central_agent_versions;
  var crdt    = params.data.crdt;
  var gcsumms = params.data.gc_summary;
  var ether   = params.data.ether;
  var remove  = params.data.remove;
  // NOTE: ZCSub.HandleRouterClusterSubscriberMerge() is ASYNC
  ZCSub.HandleRouterClusterSubscriberMerge(net, ks, crdt, cavrsns,
                                           gcsumms, ether, remove);
  next(null, null);
}

function handle_router_geo_need_merge(net, params, id, puuid, next) {
  var ks = params.data.ks;
  ZH.l('<-|(S): RouterGeoNeedMerge: K : ' + ks.kqk);
  ZNM.HandleRouterGeoNeedMerge(net, ks, next);
}

function handle_router_geo_commit_delta(net, params, id, puuid, next) {
  var ks     = params.data.ks;
  ZH.l('<-|(S): RouterGeoCommitDelta: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  // NOTE: ZPio.GeoBroadcastCommitDelta() is ASYNC
  ZPio.GeoBroadcastCommitDelta(net.plugin, net.collections, ks, author, rchans);
  next(null, null);
}

function handle_router_geo_send_ack_geo_delta(net, params, id, puuid, next) {
  var ks            = params.data.ks;
  ZH.l('<-|(S): RouterGeoSendAckGeoDelta: K: ' + ks.kqk);
  var rguuid        = params.data.destination;
  var author        = params.data.author;
  var rchans        = params.data.replication_channels;
  var dirty_central = params.data.dirty_central;
  if (dirty_central) { // AckGeoDeltaError -> GeoDrain -> GeoDentries
    // NOTE: ZPio.GeoBroadcastAckGeoDelta() is ASYNC
    ZPio.GeoBroadcastAckGeoDelta(net.plugin, net.collections,
                                 ks, author, rchans);
  } else {
    var gnode  = ZCLS.GetGeoNodeByUUID(rguuid);
    if (gnode) { // Only if Author's GeoCluster is UP
      // NOTE: ZPio.GeoSendAckGeoDelta() is ASYNC
      ZPio.GeoSendAckGeoDelta(net.plugin, net.collections,
                              gnode, ks, author, rchans);
    }
  }
  next(null, null);
}

function handle_router_geo_send_ack_geo_delta_error(net, params,
                                                    id, puuid, next) {
  var ks     = params.data.ks;
  ZH.l('<-|(S): RouterGeoSendAckGeoDeltaError: K: ' + ks.kqk);
  var rguuid = params.data.destination;
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var error  = params.data.error;
  var gnode  = ZCLS.GetGeoNodeByUUID(rguuid);
  if (gnode) { // Only if Author's GeoCluster is UP
    // NOTE: ZPio.GeoSendAckGeoDelta() is ASYNC
    ZPio.GeoSendAckGeoDeltaError(net.plugin, net.collections,
                                 gnode, ks, author, rchans, error);
  }
  next(null, null);
}

function handle_router_ack_geo_broadcast_subscriber_commit_delta(net, params,
                                                                 id, puuid,
                                                                 next) {
  var ks     = params.data.ks;
  ZH.l('<-|(S): RouterGeoBroadcastSubscriberCommitDelta: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  // NOTE: ZPio.GeoBroadcastSubscriberCommitDelta() is ASYNC
  ZPio.GeoBroadcastSubscriberCommitDelta(net, ks, author, rchans);
  next(null, null);
}

function handle_router_ack_geo_commit_delta(net, params, id, puuid, next) {
  var ks     = params.data.ks;
  ZH.l('<-|(S): RouterAckGeoCommitDelta: K: ' + ks.kqk);
  var rguuid = params.data.destination;
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var gnode  = ZCLS.GetGeoNodeByUUID(rguuid);
  if (gnode) { // Only if Author's GeoCluster is UP
    // NOTE: ZPio.GeoSendAckGeoCommitDelta() is ASYNC
    ZPio.GeoSendAckGeoCommitDelta(net.plugin, net.collections,
                                  gnode, ks, author, rchans);
  }
  next(null, null);
}

function handle_router_geo_send_ack_agent_delta_error(net, params,
                                                      id, puuid, next) {
  var ks     = params.data.ks;
  ZH.l('<-|(S): RouterGeoSendAckAgentDeltaError: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var error  = params.data.error;
  var agent  = params.data.agent;
  // NOTE: ZPio.SendClusterSubscriberAgentDeltaError() is ASYNC
  ZPio.SendClusterSubscriberAgentDeltaError(net.plugin, net.collections,
                                            ks, author, rchans, error, agent);
  next(null, null);
}

function handle_router_geo_dentries(net, params, id, puuid, next) {
  var ks        = params.data.ks;
  var dentries  = params.data.dentries;
  var freeze    = params.data.freeze;
  var rguuid    = params.data.destination;
  var gnode = ZCLS.GetGeoNodeByUUID(rguuid);
  if (gnode) { // NOTE: ZPio.GeoSendDentries() is ASYNC
    ZPio.GeoSendDentries(net.plugin, net.collections, ks, dentries, freeze,
                         gnode, ZGack.CallbackAckGeoDentries);
  }
  next(null, null);
}

function handle_router_subscriber_propogate_subscribe(net, params,
                                                      id, puuid, next) {
  var suuid     = params.data.subscriber;
  var schanid   = params.data.channel.id;
  var perm      = params.data.channel.permissions;
  var pkss      = params.data.pkss;
  var username  = params.data.username;
  // NOTE: ZPio.SendSubscriberPropogateSubscribe() is ASYNC
  ZPio.SendSubscriberPropogateSubscribe(net.plugin, net.collections,
                                        suuid, schanid, username, perm, pkss);
  next(null, null);
}

function handle_router_subscriber_propogate_unsubscribe(net, params,
                                                        id, puuid, next) {
  var suuid     = params.data.subscriber;
  var schanid   = params.data.channel.id;
  var perm      = params.data.channel.permissions;
  var username  = params.data.username;
  // NOTE: ZPio.SendSubscriberPropogateUnsubscribe() is ASYNC
  ZPio.SendSubscriberPropogateUnsubscribe(net.plugin, net.collections,
                                          suuid, schanid, username, perm);
  next(null, null);
}

function handle_router_subscriber_propogate_grant_user(net, params,
                                                       id, puuid, next) {
  var suuid     = params.data.subscriber;
  var schanid   = params.data.channel.id;
  var priv      = params.data.channel.privilege;
  var username  = params.data.username;
  // NOTE: ZPio.SendSubscriberPropogateGrantUser() is ASYNC
  ZPio.SendSubscriberPropogateGrantUser(net.plugin, net.collections,
                                        suuid, username, schanid, priv);
  next(null, null);
}

function handle_router_subscriber_propogate_remove_user(net, params,
                                                        id, puuid, next) {
  var suuid     = params.data.subscriber;
  var username  = params.data.username;
  // NOTE: ZPio.SendSubscriberPropogateRemoveUser() is ASYNC
  ZPio.SendSubscriberPropogateRemoveUser(net.plugin, net.collections,
                                         suuid, username);
  next(null, null);
}

function handle_router_self_ack_geo_delta(net, params, id, puuid, next) {
  var ks     = params.data.ks;
  ZH.l('handle_router_self_ack_geo_delta: K: ' + ks.kqk);
  var dentry = params.data.dentry;
  ZGack.InternalAckGeoDelta(net, ks, dentry, next);
}

function handle_router_broadcast_auto_delta(net, params, id, puuid, next) {
  var ks     = params.data.ks;
  var dentry = params.data.dentry;
  ZPub.HandleRouterBroadcastAutoDelta(net, ks, dentry, next);
}

function handle_router_watch_delta(net, params, id, puuid, next) {
  var ks     = params.data.ks;
  ZH.l('handle_router_watch_delta: K: ' + ks.kqk);
  var dentry = params.data.dentry;
  var pmd    = params.data.post_merge_deltas;
  var rchans = dentry.delta._meta.replication_channels;
  ZPub.GetSubs(net.plugin, net.collections, ks, rchans, function(rerr, subs) {
    if (rerr) next(rerr, null);
    else {
      dentry.post_merge_deltas = pmd;
      ZPub.SendClusterSubscriberDelta(net, ks, dentry, subs, true, next)
    }
  });
}

function handle_router_send_data_center_online(net, params, id, puuid, next) {
  next(null, null);
  // NOTE: ZDConn.SendDataCenterOnline() is ASYNC
  ZDConn.SendDataCenterOnline();
}

var RouterConsumers = {
                  'RouterAckAgentOnline'                 :
                    {handler : handle_router_ack_on_user_data},
                  'RouterAckAgentRecheck'                   :
                    {handler : handle_router_ack_on_user_data},
                  'RouterAckGetAgentKeys'                :
                    {handler : handle_router_ack_on_user_data},
                  'RouterAckDataCenterOnline'            :
                    {handler : handle_router_ack_data_center_online},
                  'RouterAckNeedMerge'                   :
                    {handler : handle_router_ack_storage_need_merge},
                  'RouterAckGeoNeedMerge'                :
                    {handler : handle_router_ack_storage_need_merge},
                  'RouterAckFind'                        :
                    {handler : handle_router_ack_find},
                  'RouterAckClusterCache'                :
                    {handler : handle_router_ack_cluster_cache},
                  'RouterAckStatusRepeatDelta'           :
                    {handler : handle_router_ack_status_repeat_delta},
                  'RouterAckClusterClientCall'           :
                    {handler : handle_router_ack_cluster_client_call},
                  'RouterClusterSubscriberMerge'         :
                    {handler : handle_router_cluster_subscriber_merge},
                  'RouterGeoNeedMerge'                   :
                    {handler : handle_router_geo_need_merge},
                  'RouterGeoCommitDelta'                 :
                    {handler : handle_router_geo_commit_delta},
                  'RouterGeoDentries'                    :
                    {handler : handle_router_geo_dentries},
                  'RouterStorageOnline'                  :
                    {handler : handle_router_storage_online},
                  'RouterGeoSendAckGeoDelta'             :
                    {handler : handle_router_geo_send_ack_geo_delta},
                  'RouterGeoSendAckGeoDeltaError'        :
                    {handler : handle_router_geo_send_ack_geo_delta_error},
                  'RouterGeoBroadcastSubscriberCommitDelta' :
                    {handler :
                       handle_router_ack_geo_broadcast_subscriber_commit_delta},
                  'RouterAckGeoCommitDelta'              :
                    {handler : handle_router_ack_geo_commit_delta},
                  'RouterGeoSendAckAgentDeltaError'      :
                    {handler : handle_router_geo_send_ack_agent_delta_error},
                  'RouterSubscriberPropogateSubscribe'   :
                    {handler : handle_router_subscriber_propogate_subscribe},
                  'RouterSubscriberPropogateUnsubscribe' :
                    {handler : handle_router_subscriber_propogate_unsubscribe},
                  'RouterSubscriberPropogateGrantUser'   :
                    {handler : handle_router_subscriber_propogate_grant_user},
                  'RouterSubscriberPropogateRemoveUser'  :
                    {handler : handle_router_subscriber_propogate_remove_user},
                  'RouterSelfAckGeoDelta'                :
                    {handler : handle_router_self_ack_geo_delta},
                  'RouterBroadcastAutoDelta'             :
                    {handler : handle_router_broadcast_auto_delta},
                  'RouterWatchDelta'                     :
                    {handler : handle_router_watch_delta},
                  'RouterSendDataCenterOnline'           :
                    {handler : handle_router_send_data_center_online},
                  'NoOp'                                 :
                    {handler : handle_no_op},
  };


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE PRODUCER ACKS -----------------------------------------------------

function push_router_ack(me, method, rdata, puuid, dfield, next) {
  if (!ZH.AmStorage) throw(new Error("LOGIC ERROR(push_router_ack)"));
  var cnode     = ZCLS.RouterNode;
  var auth      = ZH.NobodyAuth;
  var data      = {id      : rdata.id,
                   device  : {uuid : ZH.MyUUID}};
  data[dfield]  = rdata[dfield];
  var id        = ZDelt.GetNextRpcID(null, null, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var tname     = get_server_queue_name(puuid, true);
  ZH.l('push_router_ack: tname: ' + tname);
  ZH.p(jrpc_body);
  if (ZH.AmBoth) {
    setImmediate(function() {
      var key   = tname;
      var entry = {key : key, value : jrpc_body};
      process_queue_request(true, entry, ZH.OnErrLog);
    });
    next(null, null);
  } else {
    me.router_plugin.do_queue_push(tname, jrpc_body, next);
  }
}

exports.PushRouterAckAgentOnline = function(rdata, puuid, next) {
  var me     = this;
  var method = 'RouterAckAgentOnline';
  push_router_ack(me, method, rdata, puuid, "user_data", next);
}

exports.PushRouterAckAgentRecheck = function(rdata, puuid, next) {
  var me     = this;
  var method = 'RouterAckAgentRecheck';
  push_router_ack(me, method, rdata, puuid, "user_data", next);
}

exports.PushRouterAckGetAgentKeys = function(rdata, puuid, next) {
  var me     = this;
  var method = 'RouterAckGetAgentKeys';
  push_router_ack(me, method, rdata, puuid, "user_data", next);
}

exports.PushRouterAckDataCenterOnline = function(rdata, puuid, next) {
  var me     = this;
  var method = 'RouterAckDataCenterOnline';
  push_router_ack(me, method, rdata, puuid, "dc_data", next);
}

exports.PushRouterAckNeedMerge = function(rdata, puuid, is_geo, next) {
  var me     = this;
  var method = is_geo ? 'RouterAckGeoNeedMerge' :
                        'RouterAckNeedMerge';
  push_router_ack(me, method, rdata, puuid, "merge_data", next);
}

exports.PushRouterAckFind = function(rdata, puuid, next) {
  var me     = this;
  var method = 'RouterAckFind';
  push_router_ack(me, method, rdata, puuid, "f_data", next);
}

exports.PushRouterAckClusterCache = function(rdata, puuid, next) {
  var me     = this;
  var method = 'RouterAckClusterCache';
  push_router_ack(me, method, rdata, puuid, "merge_data", next);
}

exports.PushRouterAckStatusRepeatDelta = function(rdata, puuid, next) {
  var me     = this;
  var method = 'RouterAckStatusRepeatDelta';
  push_router_ack(me, method, rdata, puuid, "key_data", next);
}

exports.PushRouterAckClusterClientCall = function(rdata, puuid, next) {
  var me     = this;
  var method = 'RouterAckClusterClientCall';
  push_router_ack(me, method, rdata, puuid, "key_data", next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE PRODUCER HELPERS (PUSH TO ROUTER) ---------------------------------

function do_push_to_router_leader(me, jrpc_body, next) {
  var tname = get_leader_topic(true);
  if (ZH.AmBoth) {
    setImmediate(function() {
      var key   = tname;
      var entry = {key : key, value : jrpc_body};
      process_queue_request(true, entry, ZH.OnErrLog);
    });
    next(null, null);
  } else {
    if (ZH.AmRouter) throw(new Error("LOGIC ERROR(do_push_to_router_leader)"));
    ZH.l('do_push_to_router_leader: tname: ' + tname);
    ZH.p(jrpc_body);
    me.router_plugin.do_queue_push(tname, jrpc_body, next);
  }
}

function do_push_to_router_key(me, jrpc_body, next) {
  var kname = Queue.RouterKeyName;
  var tname = get_key_topic_name_from_jrpc_body(jrpc_body);
  if (tname === null) {
    ZH.e(jrpc_body); throw(new Error('PUSH_TO_ROUTER: NO KEY IN BODY'));
  }
  if (ZH.AmBoth) {
    setImmediate(function() {
      var key   = kname + '|' + tname;
      var entry = {key : key, value : jrpc_body};
      process_queue_request(true, entry, ZH.OnErrLog);
    });
    next(null, null);
  } else {
    if (ZH.AmRouter) throw(new Error("LOGIC ERROR(do_push_to_router_key)"));
    ZH.l('do_push_to_router_key: kname: ' + kname + ' tname: ' + tname);
    ZH.p(jrpc_body);
    me.router_plugin.do_push(me.router_queue_coll, kname, tname,
                             jrpc_body, next);
  }
}

function do_push_to_router_subscriber(me, jrpc_body, next) {
  var kname = Queue.RouterKeyName;
  var tname = get_subscriber_topic_name_from_jrpc_body(jrpc_body);
  if (tname === null) {
    ZH.e(jrpc_body);throw(new Error('PUSH_TO_ROUTER: NO SUBSCRIBER IN BODY'));
  }
  if (ZH.AmBoth) {
    setImmediate(function() {
      var key   = kname + '|' + tname;
      var entry = {key : key, value : jrpc_body};
      process_queue_request(true, entry, ZH.OnErrLog);
    });
    next(null, null);
  } else {
    if (ZH.AmRouter) throw(new Error("LOGIC ERROR(push_to_router_subscriber)"));
    ZH.l('do_push_to_router_subscriber: kname: ' + kname + ' tname: ' + tname);
    ZH.p(jrpc_body);
    me.router_plugin.do_push(me.router_queue_coll, kname, tname,
                            jrpc_body, next);
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ASYNC/REQUEST STORAGE PRODUCERS (PUSH TO ROUTER) --------------------------

exports.RouterQueueRequestStorageOnline = function(plugin, collections, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterStorageOnline';
  var auth   = ZH.NobodyAuth;
  var data   = {device         : {uuid : ZH.MyUUID},
                central_synced : ZH.CentralSynced};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, {}, next);
  do_push_to_router_leader(me, jrpc_body, ZH.OnErrLog);
}

exports.PushRouterGeoNeedMerge = function(plugin, collections, ks, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterGeoNeedMerge';
  var auth   = ZH.NobodyAuth;
  var data   = {device : {uuid : ZH.MyUUID},
                ks     : ks};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.AsyncPushRouterSubscriberPropogateSubscribe =
                                    function(plugin, collections,
                                             sub, schanid, auth, perm, pkss) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterSubscriberPropogateSubscribe';
  var auth   = ZH.NobodyAuth;
  var data   = {device     : {uuid : ZH.MyUUID},
                subscriber : sub.UUID,
                channel    : {id          : schanid,
                              permissions : perm},
                pkss       : pkss,
                username   : auth.username};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_subscriber(me, jrpc_body, ZH.OnErrLog);
}

exports.AsyncPushRouterSubscriberPropogateUnsubscribe =
                                    function(plugin, collections,
                                             sub, schanid, auth, perm) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterSubscriberPropogateUnsubscribe';
  var auth   = ZH.NobodyAuth;
  var data   = {device     : {uuid : ZH.MyUUID},
                subscriber : sub.UUID,
                channel    : {id          : schanid,
                              permissions : perm},
                username   : auth.username};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_subscriber(me, jrpc_body, ZH.OnErrLog);
}

exports.AsyncPushRouterSubscriberPropogateGrantUser =
                                    function(plugin, collections,
                                             sub, auth, schanid, priv) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterSubscriberPropogateGrantUser';
  var auth   = ZH.NobodyAuth;
  var data   = {device     : {uuid : ZH.MyUUID},
                subscriber : sub.UUID,
                channel    : {id        : schanid,
                              privilege : priv},
                username   : auth.username};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_subscriber(me, jrpc_body, ZH.OnErrLog);
}

exports.AsyncPushRouterSubscriberPropogateRemoveUser =
                                    function(plugin, collections, sub, auth) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterSubscriberPropogateRemoveUser';
  var auth   = ZH.NobodyAuth;
  var data   = {device     : {uuid : ZH.MyUUID},
                subscriber : sub.UUID,
                username   : auth.username};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_subscriber(me, jrpc_body, ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NORMAL STORAGE PRODUCERS --------------------------------------------------

exports.PushRouterClusterSubscriberMerge = function(plugin, collections,
                                                    ks, xcrdt, cavrsns,
                                                    gcsumms, ether,
                                                    remove, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterClusterSubscriberMerge';
  var auth   = ZH.NobodyAuth;
  var data   = {device                 : {uuid : ZH.MyUUID},
                datacenter             : ZH.MyDataCenter,
                ks                     : ks,
                central_agent_versions : cavrsns,
                crdt                   : xcrdt,
                gc_summary             : gcsumms,
                ether                  : ether,
                remove                 : remove};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterGeoCommitDelta = function(plugin, collections,
                                            ks, author, rchans, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterGeoCommitDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                ks                   : ks,
                author               : author,
                replication_channels : rchans};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterGeoDentries = function(plugin, collections,
                                         ks, dentries, freeze, gnode, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterGeoDentries';
  var auth   = ZH.NobodyAuth;
  var data = {device      : {uuid : ZH.MyUUID},
              destination : gnode.device_uuid,
              ks          : ks,
              dentries    : dentries,
              freeze      : freeze};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterSendAckGeoDelta = function(plugin, collections,
                                             rguuid, ks, author,
                                             rchans, dirty_central, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterGeoSendAckGeoDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                destination          : rguuid,
                ks                   : ks,
                author               : author,
                replication_channels : rchans,
                dirty_central        : dirty_central};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterSendAckGeoDeltaError = function(plugin, collections,
                                                  rguuid, ks, author, rchans,
                                                  message, details, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterGeoSendAckGeoDeltaError';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                destination          : rguuid,
                ks                   : ks,
                author               : author,
                replication_channels : rchans,
                error                : {code       : -32006,
                                        message    : message,
                                        details    : details,
                                        datacenter : ZH.MyDataCenter,
                                        am_primary : ZCLS.AmPrimaryDataCenter()
                }
               };
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterGeoBroadcastSubscriberCommitDelta = function(net, ks, author, 
                                                               rchans, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterGeoBroadcastSubscriberCommitDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                ks                   : ks,
                author               : author,
                replication_channels : rchans};
  var id        = ZDelt.GetNextRpcID(net.plugin, net.collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterAckGeoCommitDelta = function(net, ks, author, rchans,
                                               rguuid, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterAckGeoCommitDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                destination          : rguuid,
                ks                   : ks,
                author               : author,
                replication_channels : rchans};
  var id        = ZDelt.GetNextRpcID(net.plugin, net.collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterSendAckAgentDeltaError = function(plugin, collections,
                                                    auuid, ks, author, rchans,
                                                    message, details, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterGeoSendAckAgentDeltaError';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                agent                : {uuid : auuid},
                ks                   : ks,
                author               : author,
                replication_channels : rchans,
                error                : {code       : -32006,
                                        message    : message,
                                        details    : details,
                                        datacenter : ZH.MyDataCenter}};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterSelfAckGeoDelta = function(plugin, collections,
                                             ks, dentry, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterSelfAckGeoDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device : {uuid : ZH.MyUUID},
                ks     : ks,
                dentry : dentry};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterBroadcastAutoDelta = function(plugin, collections,
                                                ks, dentry, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterBroadcastAutoDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device : {uuid : ZH.MyUUID},
                ks     : ks,
                dentry : dentry};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterWatchDelta = function(plugin, collections,
                                        ks, dentry, pmd, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterWatchDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device            : {uuid : ZH.MyUUID},
                ks                : ks,
                dentry            : dentry,
                post_merge_deltas : pmd};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_router_key(me, jrpc_body, next);
}

exports.PushRouterSendDataCenterOnline = function(plugin, collections, next) {
  var me     = this;
  var cnode  = ZCLS.RouterNode;
  var method = 'RouterSendDataCenterOnline';
  var auth   = ZH.NobodyAuth;
  var data   = {device : {uuid : ZH.MyUUID}};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, {}, next);
  do_push_to_router_leader(me, jrpc_body, ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROUTER PRODUCER HELPERS (PUSH TO STORAGE) ---------------------------------

function do_push_to_storage_leader(me, jrpc_body, next) {
  var tname = get_leader_topic(false);
  if (ZH.AmBoth) {
    setImmediate(function() {
      var key   = tname;
      var entry = {key : key, value : jrpc_body};
      process_queue_request(false, entry, ZH.OnErrLog);
    });
    next(null, null);
  } else {
    if (ZH.AmStorage) {
      throw(new Error("LOGIC ERROR(do_push_to_storage_leader)"));
    }
    ZH.l('do_push_to_storage_leader: tname: ' + tname); ZH.p(jrpc_body);
    me.storage_plugin.do_queue_push(tname, jrpc_body, next);
  }
}

function do_push_to_storage_random(me, jrpc_body, next) {
  var kname = Queue.StorageKeyName;
  var tname = ZPart.GetRandomPartition();
  if (tname === null) tname = 0;
  if (ZH.AmBoth) {
    setImmediate(function() {
      var key   = kname + '|' + tname;
      var entry = {key : key, value : jrpc_body};
      process_queue_request(false, entry, ZH.OnErrLog);
    });
    next(null, null);
  } else {
    if (ZH.AmStorage) {
      throw(new Error("LOGIC ERROR(do_push_to_storage_random)"));
    }
    ZH.l('do_push_to_storage_random: kname: ' + kname + ' tname: ' + tname);
    ZH.p(jrpc_body);
    me.storage_plugin.do_push(me.storage_queue_coll, kname, tname,
                              jrpc_body, next);
  }
}

function do_push_to_storage_key(me, jrpc_body, next) {
  var kname = Queue.StorageKeyName;
  var tname = get_key_topic_name_from_jrpc_body(jrpc_body);
  if (tname === null) {
    ZH.e(jrpc_body); throw(new Error('PUSH_TO_STORAGE: NO KEY IN BODY'));
  }
  if (ZH.AmBoth) {
    setImmediate(function() {
      var key   = kname + '|' + tname;
      var entry = {key : key, value : jrpc_body};
      process_queue_request(false, entry, ZH.OnErrLog);
    });
    next(null, null);
  } else {
    if (ZH.AmStorage) throw(new Error("LOGIC ERROR(do_push_to_storage_key)"));
    ZH.l('do_push_to_storage_key: kname: ' + kname + ' tname: ' + tname);
    ZH.p(jrpc_body);
    me.storage_plugin.do_push(me.storage_queue_coll, kname, tname,
                              jrpc_body, next);
  }
}

function do_push_to_storage_topic(me, jrpc_body, tname, next) {
  if (ZH.AmBoth) {
    setImmediate(function() {
      var key   = tname;
      var entry = {key : key, value : jrpc_body};
      process_queue_request(false, entry, ZH.OnErrLog);
    });
    next(null, null);
  } else {
    if (ZH.AmStorage) throw(new Error("LOGIC ERROR(do_push_to_storage_topic)"));
    if (jrpc_body.method !== "StorageSprinkle") {
      ZH.l('do_push_to_storage_topic: tname: ' + tname); ZH.p(jrpc_body);
    }
    me.storage_plugin.do_queue_push(tname, jrpc_body, next);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ASYNC ROUTER PRODUCERS (PUSH TO STORAGE) ----------------------------------

//TODO DEPRECATE ASYNC-PUSH-STORAGE -< normal PushStorage is OK
function async_push_storage_cluster_evict_method(me, plugin, collections,
                                                 method, ks, agent) {
  var cnode  = ZCLS.StorageNode;
  var auth   = ZH.NobodyAuth;
  var data   = {device : {uuid : ZH.MyUUID},
                ks     : ks,
                agent  : agent};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, ZH.OnErrLog);
}

exports.AsyncPushStorageClusterEvict = function(plugin, collections,
                                                ks, agent) {
  var me     = this;
  var method = 'StorageClusterEvict';
  async_push_storage_cluster_evict_method(me, plugin, collections, method,
                                          ks, agent);
}

exports.AsyncPushStorageClusterLocalEvict = function(plugin, collections,
                                                     ks, agent) {
  var me     = this;
  var method = 'StorageClusterLocalEvict';
  async_push_storage_cluster_evict_method(me, plugin, collections, method,
                                          ks, agent);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REQUEST ROUTER PRODUCERS (PUSH TO STORAGE) --------------------------------

exports.StorageQueueRequestAgentNeedMerge = function(plugin, collections,
                                                     id, ks, gcv, agent,
                                                     auth, hres, next) {
  var me = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageNeedMerge';
  var data   = {device     : {uuid : ZH.MyUUID},
                ks         : ks,
                gc_version : gcv,
                request_id : id,
                agent      : {uuid : agent.uuid},
               };
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, hres, next);
  do_push_to_storage_key(me, jrpc_body, ZH.OnErrLog);
}

exports.StorageQueueRequestGeoNeedMerge = function(plugin, collections,
                                                   ks, guuid,
                                                   auth, hres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoNeedMerge';
  var data   = {device     : {uuid : ZH.MyUUID},
                ks         : ks,
                datacenter : guuid,
               };
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, hres, next);
  do_push_to_storage_key(me, jrpc_body, ZH.OnErrLog);
}

exports.StorageQueueRequestAgentOnline = function(plugin, collections,
                                                  b, device, perm, schans,
                                                  susers, created, hres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageAgentOnline';
  var auth   = ZH.NobodyAuth;
  var data   = {device         : {uuid : ZH.MyUUID},
                value          : b,
                agent          : device,
                permissions    : perm,
                subscriptions  : schans,
                stationedusers : susers,
                created        : created};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, hres, next);
  do_push_to_storage_random(me, jrpc_body, ZH.OnErrLog);
}

exports.StorageQueueRequestAgentRecheck = function(plugin, collections,
                                                   agent, created, hres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageAgentRecheck';
  var auth   = ZH.NobodyAuth;
  var data   = {device  : {uuid : ZH.MyUUID},
                agent   : agent,
                created : created};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, hres, next);
  do_push_to_storage_random(me, jrpc_body, ZH.OnErrLog);
}

exports.StorageQueueRequestGetAgentKeys = function(plugin, collections,
                                                   agent, nkeys, minage, wonly,
                                                   hres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGetAgentKeys';
  var auth   = ZH.NobodyAuth;
  var data   = {device      : {uuid : ZH.MyUUID},
                agent       : agent,
                num_keys    : nkeys,
                minimum_age : minage,
                watch_only  : wonly};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, hres, next);
  do_push_to_storage_random(me, jrpc_body, ZH.OnErrLog);
}

exports.StorageQueueRequestGeoDataCenterOnline = function(plugin, collections, 
                                                          rguuid, dkeys,
                                                          hres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoDataCenterOnline';
  var auth   = ZH.NobodyAuth;
  var data   = {device      : {uuid : ZH.MyUUID},
                destination : rguuid,
                device_keys : dkeys};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, hres, next);
  do_push_to_storage_random(me, jrpc_body, ZH.OnErrLog);
}

exports.StorageQueueRequestFind = function(plugin, collections,
                                           ns, cn, query, auth, hres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageFind';
  var data   = {device     : {uuid : ZH.MyUUID},
                namespace  : ns,
                collection : cn,
                query      : query};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, hres, next);
  do_push_to_storage_random(me, jrpc_body, ZH.OnErrLog);
}

exports.StorageQueueRequestClusterCache = function(plugin, collections,
                                                   ks, watch, agent, need_body,
                                                   auth, hres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageClusterCache';
  var data   = {device    : {uuid : ZH.MyUUID},
                ks        : ks,
                watch     : watch,
                agent     : agent,
                need_body : need_body};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, hres, next);
  do_push_to_storage_key(me, jrpc_body, ZH.OnErrLog);
}

exports.StorageRequestClusterClientCall =
                                function(plugin, collections, 
                                         fetch, store, commit, scommit, remove,
                                         ks, json, crdt, oplog, rchans,
                                         hres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageClusterClientCall';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                ks                   : ks,
                fetch                : fetch,
                store                : store,
                commit               : commit,
                stateless_commit     : scommit,
                remove               : remove,
                json                 : json,
                crdt                 : crdt,
                oplog                : oplog,
                replication_channels : rchans};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  register_queue_callback(id, hres, next);
  do_push_to_storage_key(me, jrpc_body, ZH.OnErrLog);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NORMAL ROUTER PRODUCERS (PUSH TO STORAGE) ---------------------------------

exports.PushStorageSprinkler = function(plugin, collections,
                                        tname, when, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageSprinkle';
  var auth   = ZH.NobodyAuth;
  var data   = {device : {uuid : ZH.MyUUID},
                topic  : tname,
                when   : when};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_topic(me, jrpc_body, tname, next);
}

exports.PushStorageDataCenterOnlineResponse = function(plugin, collections,
                                                       brbody, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = "StorageDataCenterOnlineResponse";
  var auth   = ZH.NobodyAuth;
  var data   = {device : {uuid : ZH.MyUUID},
                body   : brbody};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageAnnounceNewGeoCluster = function(plugin, collections,
                                                    gterm, gnodes, gnetpart,
                                                    gmaj, csynced, send_dco,
                                                    next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageAnnounceNewGeoCluster';
  var auth   = ZH.NobodyAuth;
  var data   = {device                 : {uuid : ZH.MyUUID},
                geo_term_number        : gterm,
                geo_nodes              : gnodes,
                cluster_synced         : csynced,
                geo_network_partition  : gnetpart,
                geo_majority           : gmaj,
                send_datacenter_online : send_dco};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageGeoStationUser = function(plugin, collections,
                                             duuid, auth, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoBroadcastStationUser';
  var data   = {device : {uuid : ZH.MyUUID},
                agent  : {uuid : duuid}};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageGeoDestationUser = function(plugin, collections,
                                               duuid, auth, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoBroadcastDestationUser';
  var data   = {device : {uuid : ZH.MyUUID},
                agent  : {uuid : duuid}};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageGeoAddUser = function(plugin, collections,
                                         username, password, role, internal,
                                         next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoBroadcastAddUser';
  var auth   = ZH.NobodyAuth;
  var data   = {device   : {uuid : ZH.MyUUID},
                username : username,
                password : password,
                role     : role,
                internal : internal};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageGeoGrantUser = function(plugin, collections,
                                           username, schanid, priv,
                                           do_unsub, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoBroadcastGrantUser';
  var auth   = ZH.NobodyAuth;
  var data   = {device         : {uuid        : ZH.MyUUID},
                username       : username,
                channel        : {id          : schanid,
                                  privilege   : priv},
                do_unsubscribe : do_unsub};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageGeoRemoveUser = function(plugin, collections,
                                            username, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoBroadcastRemoveUser';
  var auth   = ZH.NobodyAuth;
  var data   = {device   : {uuid : ZH.MyUUID},
                username : username};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageGeoSubscribe = function(plugin, collections,
                                           duuid, schanid, perm, auth, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoBroadcastSubscribe';
  var data   = {device  : {uuid        : ZH.MyUUID},
                agent   : {uuid        : duuid},
                channel : {id          : schanid,
                           permissions : perm}};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageGeoUnsubscribe = function(plugin, collections,
                                             duuid, schanid, auth, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoBroadcastUnsubscribe';
  var data   = {device  : {uuid        : ZH.MyUUID},
                agent   : {uuid        : duuid},
                channel : {id          : schanid}};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageGeoRemoveDataCenter = function(plugin, collections,
                                                  dcuuid, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoBroadcastRemoveDataCenter';
  var auth   = ZH.NobodyAuth;
  var data   = {device            : {uuid : ZH.MyUUID},
                remove_datacenter : dcuuid};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}

exports.PushStorageAckGeoNeedMerge = function(plugin, collections,
                                              ks, md, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageAckGeoNeedMerge';
  var auth   = ZH.NobodyAuth;
  var data   = {device     : {uuid : ZH.MyUUID},
                ks         : ks,
                merge_data : md};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, next);
}

exports.PushStorageDeviceKeys = function(plugin, collections, dkeys, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageDeviceKeys';
  var auth   = ZH.NobodyAuth;
  var data   = {device      : {uuid : ZH.MyUUID},
                device_keys : dkeys};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_leader(me, jrpc_body, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DELTA (FAST-PATH) ROUTER PRODUCERS (PUSH TO STORAGE) -----------------------

exports.PushStorageGeoClusterCommitDelta = function(plugin, collections,
                                                    ks, author, rchans,
                                                    rguuid, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoCommitDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                ks                   : ks,
                author               : author,
                replication_channels : rchans,
                datacenter           : rguuid};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, next);
}

exports.PushStorageAckGeoCommitDelta = function(net, ks, author, rchans,
                                                rguuid, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageAckGeoCommitDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                ks                   : ks,
                author               : author,
                replication_channels : rchans,
                datacenter           : rguuid};
  var id        = ZDelt.GetNextRpcID(net.plugin, net.collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, next);
}

exports.PushStorageGeoSubscriberCommitDelta = function(net, ks, author, rchans,
                                                       next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageGeoSubscriberCommitDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {device               : {uuid : ZH.MyUUID},
                ks                   : ks,
                author               : author,
                replication_channels : rchans};
  var id        = ZDelt.GetNextRpcID(net.plugin, net.collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, next);
}

exports.PushStorageAckGeoDentries = function(plugin, collections,
                                             ks, berr, dres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageAckGeoDentries';
  var auth   = ZH.NobodyAuth;
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID},
                berr   : berr,
                dres   : dres};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, next);
}

exports.PushStorageAckGeoDelta = function(plugin, collections,
                                          ks, berr, dres, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageAckGeoDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {ks         : ks,
                device     : {uuid : ZH.MyUUID},
                berr       : berr,
                dres       : dres};
  var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, next);
}

function do_push_storage_cluster_apply_delta(me, net, ks, dentry, rguuid,
                                             next) {
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageApplyDelta';
  var auth   = ZH.NobodyAuth;
  var data   = {ks         : ks,
                device     : {uuid : ZH.MyUUID},
                dentry     : dentry,
                datacenter : rguuid};
  var id        = ZDelt.GetNextRpcID(net.plugin, net.collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, next);
}

exports.PushStorageClusterDelta = function(net, ks, dentry, rguuid, next) {
  var me = this;
  if (ZH.ChaosMode === 26) {
    ZH.e('CHAOS-MODE: ' + ZH.ChaosDescriptions[26]);
    setTimeout(function() {
      ZH.e('RUNNING: ZDQ.PushStorageClusterDelta: K: ' + ks.kqk);
      do_push_storage_cluster_apply_delta(me, net, ks, dentry, rguuid, next);
    }, 10000);
  } else {
    do_push_storage_cluster_apply_delta(me, net, ks, dentry, rguuid, next);
  }
}

exports.PushStorageClusterDentries = function(net, ks, dentries, rguuid, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageApplyDentries';
  var auth   = ZH.NobodyAuth;
  var data   = {ks         : ks,
                device     : {uuid : ZH.MyUUID},
                dentries   : dentries,
                datacenter : rguuid};
  var id        = ZDelt.GetNextRpcID(net.plugin, net.collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, next);
}

exports.PushStorageFreezeKey = function(net, ks, next) {
  var me     = this;
  var cnode  = ZCLS.StorageNode;
  var method = 'StorageFreezeKey';
  var auth   = ZH.NobodyAuth;
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID}};
  var id        = ZDelt.GetNextRpcID(net.plugin, net.collections, cnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  do_push_to_storage_key(me, jrpc_body, next);
}

