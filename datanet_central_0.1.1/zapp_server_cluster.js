"use strict"

var https    = require('https');

var ZCloud   = require('./zcloud_server');
var ZMCV     = require('./zmethods_cluster_vote');
var ZMCC     = require('./zmemcache_server_cluster');
var ZPio     = require('./zpio');
var ZCLS     = require('./zcluster');
var ZPart    = require('./zpartition');
var ZAio     = require('./zaio');
var ZPLI     = require('./zplugin_init');
var ZChannel = require('./zchannel');
var ZDQ      = require('./zdata_queue');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT ---------------------------------------------------------------------

var NotReadyToBlockSleep = 1000;

ZH.CentralDisableGeoNetworkPartitions     = true;
ZH.CentralDisableCentralNetworkPartitions = true;

var AppClusterNamespaceName = "APP_SERVER_CLUSTER";
var AppClusterDBCname       = "ZyncGlobal";  // NOTE: FIXED NAME in zcluster.js
var AppClusterXname         = "global";      // NOTE: FIXED NAME in zcluster.js
var AppClusterCname         = "global_coll"; // NOTE: FIXED NAME in zcluster.js

var AppQueueNamespaceName   = "APP_QUEUE";
var AppQueueDBCname         = "ZyncQueue";
var AppQueueXname           = "queue";
var AppQueueCname           = "queue_coll";
var AppQueueDBCname         = "MESSAGES";
var AppQueueKeyName         = "APP_CLUSTER";

var CloudDBCname            = "ZyncCloud";
var CloudQueueKeyName       = "SERVER";
var CloudQueueXname         = "cloud";

var AppCluster              = this;
AppCluster.ready            = true;
ZH.CentralSynced            = true;
AppCluster.net              = {};   // Grab-bag for [plugin, collections]
AppCluster.net.collections  = {};   // Grab-bag for [plugin, collections]
AppCluster.net.plugin       = null;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT CONNECTIONS (DB & QUEUE) --------------------------------------------

function init_app_cluster_database_conn(db, next) {
  var me   = this;
  var info = {namespace : AppClusterNamespaceName,
              xname     : AppClusterXname,
              cname     : AppClusterCname,
              dbcname   : AppClusterDBCname};
  ZPLI.InitPluginConnection(db, info, function(ierr, zhndl) {
    if (ierr) next(ierr, null);
    else {
      AppCluster.net.zhndl       = zhndl;
      AppCluster.net.plugin      = zhndl.plugin;
      AppCluster.net.db_handle   = zhndl.db_handle;
      AppCluster.net.collections = zhndl;
      next(null, null);
    }
  });
}

function init_app_cluster_queue_connection(me, dq, next) {
  var info = {namespace : AppQueueNamespaceName,
              xname     : AppQueueXname,
              cname     : AppQueueCname,
              dbcname   : AppQueueDBCname};
  ZPLI.InitPlugin({}, dq, info, function(ierr, plugin) {
    if (ierr) next(ierr, null);
    else {
      me.queue_plugin = plugin;
      info.xname = CloudQueueXname; // XNAMEs must differ
      ZPLI.InitPlugin({}, dq, info, function(ierr, plugin) {
        if (ierr) next(ierr, null);
        else {
          me.asc_plugin = plugin;
          next(null, null);
        }
      });
    }
  });
}

function get_app_server_cluster_route_key_topic(found) {
  var kname = AppQueueKeyName;
  var prt   = ZPart.GetPartitionFromKqk(found);
  return get_topic_name(AppQueueDBCname, kname, prt);
}

function get_app_server_cluster_random_topic() {
  var kname = AppQueueKeyName;
  var prt   = ZPart.GetRandomPartition();
  return get_topic_name(AppQueueDBCname, kname, prt);
}

function get_topic_name(cname, kname, qid) {
  return "INFO_" + cname + "_" + kname + "|" + qid;
}

function get_server_queue_name(is_agent, duuid) {
  var cname = is_agent ? AppClusterDBCname  : CloudDBCname;
  var kname = is_agent ? AppQueueKeyName    : CloudQueueKeyName;
  var qid   = is_agent ? "MESSAGE_" + duuid : "RESPONSE_" + duuid;
  return get_topic_name(cname, kname, qid);
}

function get_leader_topic() {
  return "APP_SERVER_CLUSTER_LEADER";
}

var CherryBlock = true;
var RetryTimer  = null;

function block_on_message_topics(me, is_agent, plugin) {
  if (RetryTimer) {
    clearTimeout(RetryTimer);
    RetryTimer = null;
  }
  ZH.l('block_on_message_topics: START');
  var tnames = [];
  if (is_agent) {
    var prts  = ZPart.GetMyPartitions();
    if (prts.length === 0) {
      ZH.e('NOT READY TO BLOCK ON QUEUE -> NO CLUSTER PARTITIONS YET');
      RetryTimer = setTimeout(function() {
        block_on_message_topics(me, is_agent, plugin);
      }, NotReadyToBlockSleep);
      return;
    }
    var kname  = AppQueueKeyName;
    // SERVER SELF REFERENTIAL TOPIC - must be first for REDIS
    var my_server_tname = get_server_queue_name(is_agent, ZH.MyUUID);
    tnames.push(my_server_tname);
    if (ZH.AmClusterLeader) tnames.push(get_leader_topic(ZH.AmRouter));
    for (var i = 0; i < prts.length; i++) {
      var tname = get_topic_name(AppQueueDBCname, kname, prts[i]);
      tnames.push(tname);
    }
  } else {
    var my_server_tname = get_server_queue_name(is_agent, ZH.MyUUID);
    tnames.push(my_server_tname);
  }
  ZH.l('block_on_message_topics: BLOCKING');
  if (CherryBlock) {
    ZH.l('TOPICS'); ZH.p(tnames);
    CherryBlock = false;
  }
  plugin.do_queue_pop(tnames, 0, function(perr, entry) {
    if (perr) {
      ZH.e(perr);
      block_on_message_topics(me, is_agent, plugin);
    } else {
      var tname  = entry.key;
      var data   = entry.value;
      if (is_agent) {
        ZH.l('AppCluster-POP: T: ' + tname);
        if (tname === my_server_tname) { // RE-INIT-QUEUE-CONNECTION
          CherryBlock = true;
          block_on_message_topics(me, is_agent, plugin);
        } else {
          handle_app_server_message(data, function(serr, sres) {
            if (serr) ZH.e(serr);
            block_on_message_topics(me, is_agent, plugin);
          });
        }
      } else {
        ZH.l('CloudServer-POP: T: ' + tname);
        handle_app_server_response(data, function(serr, sres) {
          if (serr) ZH.e(serr);
          block_on_message_topics(me, is_agent, plugin);
        });
      }
    }
  });
}

exports.ReinitializeAppServerCluster = function(net, next) {
  if (!ZH.Agent.AppServerClusterConfig) return next(null, null);
  var me = this;
  ZH.e('ZASC.ReinitializeAppServerCluster');
  ZMCC.AdjustLocalCacheToNewPartitionTable(net, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      CherryBlock = true;
      var tname   = get_server_queue_name(true, ZH.MyUUID);
      ZDQ.ReinitializeQueueConnection(me.asc_plugin, tname, next);
    }
  });
}

var Inited = false;

function start_app_server_cluster_daemon(me, conf, next) {
  ZH.e('start_app_server_cluster_daemon');
  Inited = true;
  var db = conf.database;
  init_app_cluster_database_conn(db, function(ierr, ires) {
    if (ierr) next(ierr, null);
    else {
      var dq = conf.dataqueue;
      init_app_cluster_queue_connection(me, dq, function(qerr, qres) {
        if (qerr) next(qerr, null);
        else {
          // NOTE: ZCLS.Initialize() is ASYNC
          ZCLS.Initialize(AppCluster.net, MyAppClusterNode,
                          ZCloud.HandleClusterMethod, ZH.OnErrLog);
          // NOTE: block_on_message_topics() is ASYNC
          block_on_message_topics(me, true, me.queue_plugin);
          next(null, null);
        }
      });
    }
  });
}

function send_app_server_https(port, path, data) {
  var rid   = null;
  var ruuid = null;
  var isreq = (data.type === "request");
  if (isreq) {
    rid     = data.id;
    var res = rid.split('-');
    ruuid   = res[0];
  }
  var post_data;
  try {
    post_data = JSON.stringify(data);
  } catch(e) {
    ZH.e('ERROR: send_app_server_https: JSON.stringify(): ' + e.message);
    return;
  }
  var post_options = {
    host               : '127.0.0.1',
    port               : port,
    path               : path,
    method             : 'POST',
    rejectUnauthorized : false,
    requestCert        : true,
    agent              : false,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': post_data.length
    }
  };
  var post_request = https.request(post_options, function(res) {
    res.setEncoding('utf8');
    var pdata = '';
    res.on('data', function (chunk) { pdata += chunk; });
    res.on('end', function() {
      if (isreq) {
        exports.PushCloudResponse(rid, ruuid, pdata);
      } else {
        ZH.l('APP_SERVER_MESSAGE RESPONSE'); ZH.p(pdata);
      }
    });
  });
  post_request.on('error', function(e) {
    ZH.e('ERROR: send_app_server_https: https.request(): ' + e.message);
  });
  ZH.l('send_app_server_https: WRITE'); ZH.l(post_data);
  post_request.write(post_data);
  post_request.end();
}

function handle_app_server_message(data, next) {
  ZH.l('handle_app_server_message'); ZH.p(data);
  var conf = ZH.Agent.AppServerClusterConfig;
  if (conf.server.protocol === "HTTPS") {
    var port = conf.server.port;
    var path = conf.server.path;
    // NOTE: send_app_server_https() is ASYNC
    send_app_server_https(port, path, data);
    next(null, null);
  } else {
    throw(new Error("APP-SERVER-CLUSTER currently ONLY supports HTTPS"));
  }
}

exports.HandleNewMemcacheCluster = function(net, mstate, next) {
  var data = {body : {
                action        : "internal",
                desc          : "memcache_cluster",
                cluster_state : mstate
              }
             };
  handle_app_server_message(data, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SEND CENTRAL ANNOUNCE NEW APP SERVER CLUSTER ------------------------

var ResendNewASCTimer = null;
var ResendNewASCSleep = 5000;
function announce_new_ASC_processed(net, err, hres) {
  ZH.l('<<<<(C): announce_new_ASC_processed');
  if (ResendNewASCTimer) { // CANCEL RESEND
    clearTimeout(ResendNewASCTimer);
    ResendNewASCTimer = null;
  }
}

exports.AgentAnnounceNewAppServerCluster = function(net, next) {
  if (ResendNewASCTimer) { // CANCEL RESEND
    clearTimeout(ResendNewASCTimer);
    ResendNewASCTimer = null;
  }
  var conf   = ZH.Agent.AppServerClusterConfig;
  var clname = conf.name;
  var mcname = conf.memcache_cluster_name;
  var cnodes = ZCLS.ClusterNodes;
  // NOTE: ZAio.SendCentralAnnounceNewAppServerCluster() is ASYNC
  ZAio.SendCentralAnnounceNewAppServerCluster(net.plugin, net.collections,
                                              clname, mcname, cnodes,
  function(err, hres) { // NOTE: CLOSURE as 'next' argument
    announce_new_ASC_processed(net, err, hres);
  });
  // NOTE: ResendNewASCTimer's function is ASYNC
  ResendNewASCTimer = setTimeout(function() { // If not ACKed -> RESEND
    ZH.e('TIMEOUT -> RESENDING AgentAnnounceNewAppServerCluster');
    exports.AgentAnnounceNewAppServerCluster(net, ZH.OnErrLog);
  }, ResendNewASCSleep);
  next(null, null);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT INITIALIZATION ------------------------------------------------------

var MyAppClusterNode = null;

exports.StartAppServerClusterDaemon = function(net) {
  if (!ZH.Agent.AppServerClusterConfig) return;
  if (Inited)                           return;
  ZCloud.Initialize(AppCluster);
  ZCloud.InitializeMethods(ZMCV.Methods, ZH.OnErrLog);
  var me           = this;
  var conf         = ZH.Agent.AppServerClusterConfig;
  MyAppClusterNode = {name         : conf.name,
                      server       : {ip   : conf.server.ip,
                                      port : conf.server.port},
                      datacenter   : ZH.Agent.datacenter,
                      device_uuid  : ZH.MyUUID,
                      backend      : conf.backend,
                     };
  start_app_server_cluster_daemon(me, conf, ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL INITIALIZATION ----------------------------------------------------

function init_central_app_cluster_queue_connection(me, conf, next) {
  var dq = conf.dataqueue;
  init_app_cluster_queue_connection(me, dq, function(qerr, qres) {
    if (qerr) next(qerr, null);
    else {
      // NOTE: block_on_message_topics() is ASYNC
      block_on_message_topics(me, false, me.asc_plugin);
      next(null, null);
    }
  });
}

exports.InitializeCentralAppServerClusterQueueConnection = function() {
  if (!ZH.Central.AppServerClusterConfig) return;
  ZH.l('ZASC.InitializeCentralAppServerClusterQueueConnection');
  var me   = this;
  var conf = ZH.Central.AppServerClusterConfig;
  init_central_app_cluster_queue_connection(me, conf, ZH.OnErrLog);
}

exports.CentralHandleAnnounceNewAppServerCluster = 
                                                 function(net, clname, mcname,
                                                          cnodes, hres, next) {
  var now   = ZH.GetMsTime();
  var skey  = ZS.GetAppServerClusterState(clname);
  var state = {cluster_name          : clname,
               memcache_cluster_name : mcname,
               cluster_nodes         : cnodes,
               cluster_born          : now};
  ZH.l('ZASC.CentralHandleAnnounceNewAppServerCluster'); ZH.p(state);
  net.plugin.do_set(net.collections.global_coll, skey, state,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      if (!mcname) next(null, hres);
      else {
        ZMCC.ReBroadcastAnnounceNewMemcacheCluster(net, mcname, hres, next);
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE MESSAGE ----------------------------------------------------

function send_cluster_user_message(plugin, collections, username, msg, next) {
  var auth = {username : username};
  ZChannel.GetUserAgents(plugin, collections, auth, function(uerr, suuids) {
    if (uerr) next(uerr, null);
    else {
      if (suuids.length === 0) next(null, null);
      else {
        var cnodes = {};
        for (var i = 0; i < suuids.length; i++) {
          var suuid = suuids[i];
          var cnode = ZPart.GetClusterNode(suuid);
          if (cnode) {
            cnodes[cnode.device_uuid] = cnode;
          }
        }
        for (var cuuid in cnodes) {
          var cnode = cnodes[cuuid];
          ZPio.SendClusterUserMessage(plugin, collections, cnode,
                                      username, msg);
        }
      }
    }
  });
}

exports.ProcessGeoMessage = function(plugin, collections, msg, hres, next) {
  var me   = this;
  var body = msg.body;
  var um   = body.route && body.route.user;
  var dm   = body.route && body.route.device;
  if (!um && !dm) {
    return next(new Error(ZS.Errors.MessageFormat), hres);
  }
  if (um) ZH.l('ZASC.ProcessGeoMessage -> USER-MESSAGE');
  else    ZH.l('ZASC.ProcessGeoMessage -> DEVICE-MESSAGE');
  if (um) {
    var username = body.route.user;
    // NOTE: send_cluster_user_message() is ASYNC
    send_cluster_user_message(plugin, collections, username, msg, ZH.OnErrLog);
    next(null, hres);
  } else { // dm
    var suuid = body.route.device;
    // NOTE: ZPio.SendClusterSubscriberMessage() is ASYNC
    ZPio.SendClusterSubscriberMessage(plugin, collections, suuid, msg);
    next(null, hres);
  }
}

exports.ProcessAgentMessage = function(plugin, collections,
                                       body, device, auth, hres, next) {
  var me = this;
  ZH.l('ZASC.ProcessAgentMessage');
  var um = body.route && body.route.user;
  var dm = body.route && body.route.device;
  if (um && dm) {
    return next(new Error(ZS.Errors.MessageFormat), hres);
  }
  var msg = {type   : "message",
             sender : {username    : auth.username,
                       device_uuid : device.uuid},
             body   : body};
  if (um || dm) {
    // NOTE: ZPio.GeoBroadcastMessage() is ASYNC
    ZPio.GeoBroadcastMessage(plugin, collections, msg);
    next(null, hres);
  } else {
    var found = null;
    if (body.route && body.route.key) {
      var f = body.route.key;
      found = ZH.LookupByDotNotation(body, f);
    }
    var tname = found ? get_app_server_cluster_route_key_topic(found) :
                        get_app_server_cluster_random_topic();
    ZH.l('PushAppClusterMessage: T: ' + tname); ZH.p(msg);
    me.queue_plugin.do_queue_push(tname, msg, function(perr, pres) {
      next(perr, hres);
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE REQUEST ----------------------------------------------------

var NextRpcId = 1;
function get_next_app_server_rpc_id() {
  NextRpcId += 1;
  return ZH.MyUUID + '-APP_SERVER-' + NextRpcId;
}

var AgentRequestCallbacks = {};

function register_agent_request_callback(rid, hres, next) {
  AgentRequestCallbacks[rid] = {hres : hres, next : next};
}

function handle_app_server_response(data, next) {
  ZH.l('handle_app_server_response'); ZH.l(data);
  var rid = data.id;
  var cb  = AgentRequestCallbacks[rid];
  if (!cb) {
    ZH.e('APP_SERVER_RESPONSE: ID: ' + rid + ' NOT FOUND');
    next(null, null);
  } else {
    cb.hres.r_data = data.body;
    // NOTE: cb.next() is ASYNC
    cb.next(null, cb.hres);
    next(null, null);
  }
}

exports.PushCloudResponse = function(rid, ruuid, pdata) {
  var me = this;
  var data;
  var error;
  try {
    data = JSON.parse(pdata);
  } catch(e) {
    error = e.message;
  }
  var response = {id    : rid,
                  body  : data,
                  error : error};
  ZH.l('PushCloudResponse: U: ' + ruuid + ' ID: ' + rid); ZH.p(response);
  var tname    = get_server_queue_name(false, ruuid);
  me.asc_plugin.do_queue_push(tname, response, ZH.OnErrLog);
}

exports.ProcessAgentRequest = function(plugin, collections,
                                       body, device, auth, hres, next) {
  var me  = this;
  ZH.l('ZASC.ProcessAgentRequest');
  var rid = get_next_app_server_rpc_id();
  var msg = {type   : "request",
             id     : rid,
             sender : {username    : auth.username,
                       device_uuid : device.uuid},
             body   : body};
  var found = null;
  if (body.route && body.route.key) {
    var f = body.route.key;
    found = ZH.LookupByDotNotation(body, f);
  }
  var tname = found ? get_app_server_cluster_route_key_topic(found) :
                      get_app_server_cluster_random_topic();
  ZH.l('PushAppClusterRequest: T: ' + tname); ZH.p(msg);
  me.queue_plugin.do_queue_push(tname, msg, function(perr, pres) {
    if (perr) next(perr, hres);
    else      register_agent_request_callback(rid, hres, next);
  });
}
