"use strict";

var WebSocket;
var ZDelt, ZSM, ZSD, ZISL, ZDack, ZAS, ZChannel, ZUM, ZMCC, ZCMP, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  WebSocket = require('ws');
  ZDelt     = require('./zdeltas');
  ZSM       = require('./zsubscriber_merge');
  ZSD       = require('./zsubscriber_delta');
  ZISL      = require('./zisolated');
  ZDack     = require('./zdack');
  ZAS       = require('./zactivesync');
  ZChannel  = require('./zchannel');
  ZUM       = require('./zuser_management');
  ZMCC      = require('./zmemcache_server_cluster');
  ZCMP      = require('./zdcompress');
  ZS        = require('./zshared');
  ZH        = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

function init_agent_settings() {
  ZH.Agent.DisableAgentToSync            = false;
  ZH.Agent.DisableAgentDirtyDeltasDaemon = false;
  ZH.Agent.DisableAgentGCPurgeDaemon     = false;
  ZH.Agent.DisableReconnectAgent         = false;

  ZH.Agent.DisableSubscriberLatencies    = true;

  // NOTE: delta coalescing not supported in C++ -> DISABLE (for now)
  ZH.Agent.DisableDeltaCoalescing        = true;
}

var CentralNotYetReadyTimeout = 1000;

var CatchCentralRequestErrors = false;
var CatchCentralAckErrors     = false;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// METHODS FROM CENTRAL ------------------------------------------------------

//TODO move to zahandler.js

//NOTE: Do NOT do Read/Write/Basic Permissions/Auth for WSS requests
//      They come from Central which is the CENTRAL AUTHORITY

var CentralMethodHandlerMap = {
  'SubscriberDelta'                      :
    handle_subscriber_delta,
  'SubscriberDentries'                   :
    handle_subscriber_dentries,
  'AgentDeltaError'                      :
    handle_agent_delta_error,
  'SubscriberCommitDelta'                :
    handle_subscriber_commit_delta,
  'SubscriberMerge'                      :
    handle_subscriber_merge,
  'SubscriberGeoStateChange'             :
    handle_subscriber_geo_state_change,
  'SubscriberMessage'                    :
    handle_subscriber_message,
  'SubscriberAnnounceNewMemcacheCluster' :
    handle_subscriber_announce_new_memcache_cluster,
  'PropogateSubscribe'                   :
    handle_subscriber_propogate_subscribe,
  'PropogateUnsubscribe'                 :
    handle_subscriber_propogate_unsubscribe,
  'PropogateRemoveUser'                  :
    handle_subscriber_propogate_remove_user,
  'PropogateGrantUser'                   :
    handle_subscriber_propogate_grant_user,
  'AgentBackoff'                         :
    handle_agent_backoff,
};

function handle_subscriber_delta(net, data) {
  var next   = ZH.OnErrLog;
  var params = data.params;
  var ks     = params.data.ks;
  var dentry = params.data.dentry;
  var meta   = dentry.delta._meta;
  var rchans = ZH.GetReplicationChannels(meta);
  var hres   = {params : params, id : data.id, channels : rchans};
  ZH.l('<-|(C): SubscriberDelta: ' + ZH.SummarizeCrdt(ks.kqk, meta));
  ZSD.HandleSubscriberDelta(net, ks, dentry, false, hres, next);
}

function handle_subscriber_dentries(net, data) {
  var next     = ZH.OnErrLog;
  var params   = data.params;
  var ks       = params.data.ks;
  var dentries = params.data.dentries;
  var hres     = {params : params, id : data.id};
  ZSD.HandleSubscriberDentries(net, ks, dentries, hres, next);
}

function handle_agent_delta_error(net, data) {
  var next   = ZH.OnErrLog;
  var params = data.params;
  var ks     = params.data.ks;
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var error  = params.data.error;
  //var hres   = {params : params, id : data.id};
  ZH.l('<-|(C): AgentDeltaError: K : ' + ks.kqk);
  var data   = {error : error};
  ZDack.HandleAckAgentDeltaError(net, data, null); //TODO cleaner API
}

function handle_subscriber_commit_delta(net, data) {
  var next    = ZH.OnErrLog;
  var params  = data.params;
  var ks      = params.data.ks;
  var author  = params.data.author;
  ZH.l('<-|(C): SubscriberCommitDelta: K: ' + ks.kqk);
  ZAS.HandleSubscriberCommitDelta(net, ks, author, next);
}

function subscriber_merge_processed(err, hres) {
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('subscriber_merge_processed: ERROR: ' + etxt);
  } else {
    var ks = hres.ks;
    if (hres.remove) {
      ZH.l('<<<<(C): subscriber_merge_processed: K: ' + ks.kqk + ' -> REMOVED');
    } else {
      ZH.l('<<<<(C): subscriber_merge_processed: ' +
           ZH.SummarizeCrdt(ks.kqk, hres.crdt._meta));
    }
  }
}

function handle_subscriber_merge(net, data) {
  var next   = subscriber_merge_processed;
  var params = data.params;
  var hres   = {params : params, id : data.id};
  var ks     = params.data.ks;
  var prid   = params.data.request_id;
  var nkey   = ZS.GetNeedMergeRequestID(ks);
  net.plugin.do_get_field(net.collections.kinfo_coll, nkey, "id",
  function(gerr, rid) {
    if (gerr) next(gerr, hres);
    else { // NOTE: GeoNeedMerge SEND SubscriberMerge w/o request_id
      ZH.l('SubscriberMerge: PRID: ' + prid + ' RID: ' + rid);
      if (prid && (prid !== rid)) {
        next(new Error(ZS.Errors.OverlappingSubscriberMergeRequests), hres);
      } else {
        var crdt    = ZCMP.DecompressCrdt(params.data.zcrdt);
        var cavrsns = params.data.central_agent_versions;
        var gcsumms = params.data.gc_summary;
        var ether   = params.data.ether;
        var remove  = params.data.remove;
        ZH.l('<-|(C): SubscriberMerge: K ' + ks.kqk);
        ZSM.ProcessSubscriberMerge(net, ks, crdt, cavrsns, gcsumms, ether,
                                   remove, hres, next);
      }
    }
  });
}

function subscriber_message_processed(err, hres) {
  ZH.l('<<<<(C): subscriber_message_processed');
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('subscriber_message_processed: ERROR: ' + etxt);
  }
}

function handle_subscriber_message(net, data) {
  var next   = subscriber_message_processed;
  var params = data.params;
  var msg    = params.data.message;
  var hres   = {params : params, id : data.id};
  ZH.l('<-|(C): SubscriberMessage'); //ZH.p(msg);
  if (typeof(ZS.EngineCallback.SubscriberMessage) !== 'undefined') {
    ZS.EngineCallback.SubscriberMessage(msg);
  }
  next(null, hres);
}

function subscriber_announce_new_memcache_cluster_processed(err, hres) {
  ZH.l('<<<<(C): subscriber_announce_new_memcache_cluster_processed');
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('subscriber_announce_new_memcache_cluster_processed: ERROR: ' + etxt);
  }
}

function handle_subscriber_announce_new_memcache_cluster(net, data) {
  var next   = subscriber_announce_new_memcache_cluster_processed;
  var params = data.params;
  var mstate = params.data.cluster_state;
  var hres   = {params : params, id : data.id};
  ZH.l('<-|(C): SubscriberAnnounceNewMemcacheCluster'); ZH.p(mstate);
  ZMCC.HandleSubscriberNewMemcacheCluster(net, mstate, hres, next);
}

function geo_state_change_processed(err, hres) {
  ZH.l('<<<<(C): geo_state_change_processed');
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('geo_state_change_processed: ERROR: ' + etxt);
  }
  //TODO Engine Callback GeoStateChange
}

function handle_subscriber_geo_state_change(net, data) {
  var params = data.params;
  var gnodes = params.data.geo_nodes;
  ZH.l('<-|(C): SubscriberGeoStateChange');
  ZISL.SetGeoNodes(net, gnodes, geo_state_change_processed);
}

function propogate_subscribe_processed(err, hres) {
  ZH.l('<<<<(C): propogate_subscribe_processed');
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('propogate_subscribe_processed: ERROR: ' + etxt);
  }
  //TODO Engine Callback SubscriptionChange
}

function handle_subscriber_propogate_subscribe(net, data) {
  var params   = data.params;
  var schanid  = params.data.channel.id;
  var perm     = params.data.channel.permissions;
  var username = params.data.username;
  var pkss     = params.data.pkss;
  ZH.l('<-|(C): PropogateSubscribe: UN: ' + username + ' R: ' + schanid);
  var auth     = {username : username};
  var hres     = {params : params, id : data.id};
  ZChannel.HandlePropogateSubscribe(net, schanid, perm, pkss,
                                    auth, hres, propogate_subscribe_processed);
}

function propogate_unsubscribe_processed(err, hres) {
  ZH.l('<<<<(C): propogate_unsubscribe_processed');
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('propogate_unsubscribe_processed: ERROR: ' + etxt);
  }
  //TODO Engine Callback SubscriptionChange
}

function handle_subscriber_propogate_unsubscribe(net, data) {
  var params   = data.params;
  var schanid  = params.data.channel.id;
  var username = params.data.username;
  ZH.l('<-|(C): PropogateUnsubscribe: UN: ' + username + ' R: ' + schanid);
  var auth     = {username : username};
  var hres     = {params : params, id : data.id};
  ZChannel.HandlePropogateUnsubscribe(net, schanid, auth,
                                      hres, propogate_unsubscribe_processed);
}

function propogate_remove_user_processed(err, hres) {
  ZH.l('<<<<(C): propogate_remove_user_processed');
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('propogate_remove_user_processed: ERROR: ' + etxt);
  }
  //TODO Engine Callback UserChange
}

function handle_subscriber_propogate_remove_user(net, data) {
  var params   = data.params;
  var ks       = params.data.ks;
  var username = params.data.username;
  ZH.l('<-|(C): PropogateRemoveUser UN: ' + username);
  var auth     = {username : username};
  var hres     = {params : params, id : data.id};
  ZUM.HandlePropogateRemoveUser(net, auth,
                                hres, propogate_remove_user_processed);
}

function propogate_grant_user_processed(err, hres) {
  ZH.l('<<<<(C): propogate_grant_user_processed');
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('propogate_grant_user_processed: ERROR: ' + etxt);
  }
  //TODO Engine Callback UserChange
}

function handle_subscriber_propogate_grant_user(net, data) {
  var params   = data.params;
  var ks       = params.data.ks;
  var username = params.data.username;
  var schanid  = params.data.channel.id;
  var priv     = params.data.privilege;
  ZH.l('<-|(C): PropogateGrantUser UN: ' + username);
  var auth     = {username : username};
  var hres     = {params : params, id : data.id};
  ZUM.HandlePropogateGrantUser(net, username, schanid, priv,
                               hres, propogate_grant_user_processed);
}

function backoff_processed(err, hres) {
  ZH.l('<<<<(C): backoff_processed');
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('backoff_processed: ERROR: ' + etxt);
  }
}

function handle_agent_backoff(net, data) {
  var params = data.params;
  var bsec   = params.data.seconds;
  ZH.l('<-|(C): AgentBackoff: SEC: ' + bsec);
  var hres   = {params : params, id : data.id};
  ZISL.HandleBackoff(net, bsec, hres, backoff_processed);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MESSAGS (CENTRAL ACKS & CENTRAL REQUESTS) -------------------------------

// NOTE: ALL Central Requests MUST have 'ks' defined
function handle_central_request(hfunc, net, data) {
  hfunc(net, data);
}

function handle_central_ack(net, data, ri) {
  if (ri.next) ri.next(null, data.result);
}

var NotReadyTimer = null;
function central_not_ready_reconnect() {
  NotReadyTimer = null;
  var net = ZH.CreateNetPerRequest(ZH.Agent);
  ZISL.ReconnectToCentral(net);
}

function handle_redirect(ri, dres) {
  var cnode = dres.device ? dres.device.cluster_node : null;
  if (cnode && !ZH.SameMaster(ZH.CentralMaster, cnode)) {
    exports.RedirectWss(cnode, false);
    if (ri.next) ri.next(new Error(ZS.Errors.WssReconnecting), null);
    return true;;
  }
  return false;
}

function handle_error_response_from_agent(me, ri, data) {
  if (!ri) {
    ZH.l('HandleErrorResponseFromAgent'); ZH.p(data);
  } else {
    var method = ri.method;
    if (method === "AgentDelta") {
      var net = ZH.CreateNetPerRequest(me);
      ZDack.HandleAckAgentDeltaError(net, data, ri);
    } else {
      var error = data.error;
      if (ri.next) ri.next(error, ri.hres);
      else         ZH.e(error);
    }
  }
}

function handle_server_push(me, data) {
  if (ZH.FixLogActive) {
    kill_wss(function(werr, wres) {
      if (werr) ZH.e(werr);
      ZH.e(ZS.Errors.FixLogRunning);
      return;
    });
  }
  if (ZH.IsDefined(data.method)) { // METHOD means CENTRAL sent REQUEST
    var method = data.method;
    var hfunc  = CentralMethodHandlerMap[method];
    if (!hfunc) throw(new Error("FATAL ERROR CentralMethodHandlerMap"));
    var net    = ZH.CreateNetPerRequest(me);
    try {
      handle_central_request(hfunc, net, data);
    } catch(e) {
      if (CatchCentralRequestErrors) {
        ZH.e('ERROR: (handle_central_request): ' + e.message);
      } else {
        ZH.CloseLogFile(function() {
          throw(e);
        });
      }
    }
    if (method !== "AgentBackoff" && ZISL.BackoffTimer) {
      clearTimeout(ZISL.BackoffTimer);
      ZISL.BackoffTimer = null;
      ZISL.AgentDeIsolate();
    }
  } else if (ZH.IsDefined(data.result)) { // RESULT means CENTRAL sent ACK
    var ri = ZH.AgentWssRequestMap[data.id]; // ACK'ed what METHOD
    if (ri) {
      if (ri.method !== 'AgentOnline') {
        if (handle_redirect(ri, data.result)) return;
      }
      // CLONE collections  -> parallel events wont reset collections
      var net = ZH.CreateNetPerRequest(me);
      try {
        handle_central_ack(net, data, ri);
      } catch(e) {
        if (CatchCentralAckErrors) {
          ZH.e('ERROR: (handle_central_ack): ' + e.message);
        } else {
          ZH.CloseLogFile(function() {
            throw(e);
          });
        }
      }
    }
  } else { // CENTRAL sent ACK wtih ERROR
    var ri = ZH.AgentWssRequestMap[data.id]; // ACK'ed what METHOD
    if (ri) {
      var method    = ri.method;
      var not_ready = false;
      if (method == 'AgentOnline') {
        var ecode = data.error.code;
        if (ecode === -32100 || ecode === -32101) not_ready = true;
      }
      if (not_ready) {
        var now = Math.floor(ZH.GetMsTime() / 1000);
        var ecode = data.error.code;
        if (ecode === -32100) {
          ZH.e('AgentOnline FAIL Not-Yet-Ready-Central -> RETRY: ' + now);
        } else {
          ZH.e('AgentOnline FAIL Central-Not-Yet-Synced -> RETRY: ' + now);
        }
        if (!NotReadyTimer) {
          NotReadyTimer = setTimeout(central_not_ready_reconnect,
                                     CentralNotYetReadyTimeout);
        }
      } else {
        ZH.l('Error on Method: ' + method); ZH.p(data.error);
        handle_error_response_from_agent(me, ri, data);
      }
    } else {
      ZH.l('Unknown RPC-ID: ' + data.id + ' recieved response'); ZH.p(data);
      handle_error_response_from_agent(me, null, data);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZE WSS ----------------------------------------------------------

exports.InitAgent = function(me) {
  ZH.Agent           = me;
  me.wss             = null;
  me.dconn           = false; // dconn means connected to database
  me.cconn           = false; // cconn means connected to dentral
  me.synced          = false;
  me.net             = {};    // Grab-bag for [plugin, collections]
  me.net.collections = {};    // Grab-bag for [admin_coll, c_delta_coll]
  me.net.plugin      = null;
  me.net.zhndl       = null;
  init_agent_settings();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// OPEN WSS SETTINGS -------------------------------------------------------

var MaxNumReconnectFailsBeforeFailover = 3;
var RetryConfigDCSleep                 = 30000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// OPEN WSS GLOBALS --------------------------------------------------------

var LastGeoNode                        = -1;
exports.CurrentNumberReconnectFails    =  0;
exports.RetryConfigDC                  = true;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// OPEN WSS HELPERS --------------------------------------------------------

function get_agent_isolation(next) {
  var net        = ZH.CreateNetPerRequest(ZH.Agent);
  var admin_coll = net.collections.admin_coll;
  if (!admin_coll) { // Device Not YET initialized
    next(null, false);
  } else {
    var ikey = ZS.AgentIsolation;
    net.plugin.do_get_field(admin_coll, ikey, "value", next);
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// OPEN WSS LOGIC ----------------------------------------------------------

exports.DoWssConnect = function(next) {
  ZH.l('ZWss.DoWssConnect');
  if (ZH.Agent.DisableReconnectAgent) {
    ZH.e('ZH.Agent.DisableReconnectAgent');
    return next(null, null);
  }
  var me         = ZH.Agent;
  me.dc_hostname = ZH.CentralMaster.wss.server.hostname;
  me.dc_port     = ZH.CentralMaster.wss.server.port;
  ZH.l('CONNECT: wss://' + me.dc_hostname + ':' + me.dc_port);
  try {
    me.wss = new WebSocket('wss://' + me.dc_hostname + ':' + me.dc_port);
    if (me.wss) delete(me.wss.URL); // Avoid chrome warning
  } catch(err) {
    ZH.e(err);
    me.cconn  = false;
    me.synced = false;
    ZH.l('<<<<(C): new Websocket() ERROR: ' + JSON.stringify(err));
    if (ZH.FireOnAgentConnectionError) ZH.FireOnAgentConnectionError();
    reconnect_agent();
  }
  me.wss.onopen = function() {
    ZH.l('<<<<(C): WSS connection OPEN');
    me.cconn = true;
    if (ZH.FireOnAgentConnectionOpen) ZH.FireOnAgentConnectionOpen();
    var net = ZH.CreateNetPerRequest(ZH.Agent);
    ZISL.ReconnectToCentral(net);
  };
  me.wss.onerror = function(err) {
    ZH.l('<<<<(C): WSS connection ERROR: ' + JSON.stringify(err));
    if (me.wss) delete(me.wss.URL); // Avoid chrome warning
    me.cconn  = false;
    me.synced = false;
    if (ZH.FireOnAgentConnectionError) ZH.FireOnAgentConnectionError();
    if (!ZH.AmBrowser) {
      kill_wss(function() {
        reconnect_agent();
      });
    }
  };
  me.wss.onclose = function() {
    ZH.l('<<<<(C): WSS connection CLOSE: ' + me.cconn);
    me.cconn  = false;
    me.synced = false;
    if (ZH.FireOnAgentConnectionClose) ZH.FireOnAgentConnectionClose();
    reconnect_agent();
  };
  me.wss.onmessage = function(evt) {
    ZH.l('AGENT: WSS Message recieved');
    var data;
    try {
      //TODO ZH.NetworkDeserialize() DEPRECATED, better parsing in ztls.js
      data = ZH.NetworkDeserialize(evt.data);
    } catch(e) { ZH.p(evt.data); throw(e); }
    handle_server_push(me, data);
  }
  next(null, null);
}

function kill_wss(next) {
  ZH.l('START: kill_wss');
  var me               = ZH.Agent;
  me.cconn             = false;
  me.synced            = false;
  if (!me.wss) next();
  else {
    ZH.l('KILLING');
    me.wss.onopen    = function() {}
    me.wss.onerror   = function() {};
    me.wss.onmessage = function() {};
    if (ZH.IsDefined(me.wss.terminate)) { // NODE.JS AGENT
      me.wss.onclose   = function() {}
      ZH.l('TERMINATING-->');
      me.wss.terminate();
      // NOTE: terminate() does not call wss.onclose()
      delete(me.wss);
      me.wss = null;
      next();
      ZH.l('-->TERMINATED');
    } else {                             // BROWSER AGENT
      ZH.l('CLOSING-->');
      me.wss.onclose = function() { next(); }
      me.wss.close();
      ZH.l('-->CLOSED');
      ZH.l('-->DELETE WSS');
      delete(me.wss);
      me.wss = null;
    }
  }
}

exports.RedirectWss = function(cnode, force) {
  if (!force && ZH.SameMaster(ZH.CentralMaster, cnode)) return;
  ZH.l('SWITCH -> CLUSTER-NODE'); ZH.p(cnode);
  ZH.CentralMaster.wss    = cnode.wss;
  ZH.CentralMaster.socket = cnode.socket;
  kill_wss(function() {
    exports.DoWssConnect(ZH.OnErrLog);
  });
}

exports.OpenAgentWss = function(next) {
  ZH.l('ZWss.OpenAgentWss'); ZH.p(ZH.CentralMaster);
  get_agent_isolation(function(gerr, val) {
    if (gerr) next (gerr, null);
    else {
      ZH.SetIsolation(val);
      if (ZH.Isolation) {
        ZH.l('Device is ISOLATED -> NOT CONNECTING TO CENTRAL');
        if (typeof(val) !== 'boolean') ZISL.StartupOnBackoff();
      } else {
        kill_wss(function() {
          exports.DoWssConnect(next);
        });
      }
    }
  });
}

exports.HandleIsolationOff = function(net, hres, next) {
  if (ZH.Agent.cconn) {
    ZISL.ReconnectToCentral(net);
    next(null, hres);
  } else {
    exports.DoWssConnect(function(cerr, cres) {
      if (cerr) ZH.e(cerr);
      next(null, hres);
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RECONNECT WSS -------------------------------------------------------------

var RetryConfigDCTimer = null;

function unset_retry_config_DC() {
  ZH.l('unset_retry_config_DC: DC: ' + ZH.Agent.datacenter);
  if (RetryConfigDCTimer) {
    clearTimeout(RetryConfigDCTimer);
    RetryConfigDCTimer = null;
  }
  ZH.l('exports.RetryConfigDC -> FALSE');
  exports.RetryConfigDC = false;
  RetryConfigDCTimer = setTimeout(function() {
    ZH.l('exports.RetryConfigDC -> TRUE');
    exports.RetryConfigDC = true;
    var sgf = ZISL.ShouldDoGeoFailover();
    if (sgf) ZISL.DoAgentGeoFailover();
  }, RetryConfigDCSleep);
}

function do_geo_failover(gnode) {
  ZH.l('do_geo_failover: gnode'); ZH.p(gnode);
  var glb_hostname  = gnode["wss"]["load_balancer"]["hostname"];
  var glb_port      = gnode["wss"]["load_balancer"]["port"];
  var cnode         = {device_uuid : gnode.device_uuid,
                       wss         : {
                         server : {
                           hostname : glb_hostname, //NOTE: LB_hostname
                           port     : glb_port      //NOTE: LB_port
                         }
                       }
                      };
  ZH.CentralMaster         = ZH.clone(cnode);
  ZH.MyDataCenter          = ZH.CentralMaster.device_uuid;
  ZH.l('do_geo_failover: NEW MASTER'); ZH.p(ZH.CentralMaster);
  return cnode;
}

function reconnect_agent() {
  if (ZH.Isolation) {
    ZH.l('reconnect_agent(): ISOLATED -> not reconnecting');
    return;
  }
  ZISL.ClearAgentGeoFailoverTimer();
  exports.CurrentNumberReconnectFails += 1;
  var nfails  = exports.CurrentNumberReconnectFails;
  var maxfail = MaxNumReconnectFailsBeforeFailover;
  ZH.l('reconnect_agent(): Reconnecting: TRY#: ' + nfails);
  if (nfails >= maxfail) {
    unset_retry_config_DC()
    if ((nfails % maxfail) === 0) { // GEO FAILOVER
      ZH.l('Geo-Failover: AgentGeoNodes[]'); ZH.p(ZISL.AgentGeoNodes);
      if (ZISL.AgentGeoNodes && ZISL.AgentGeoNodes.length !== 0) {
        LastGeoNode      += 1;
        if (LastGeoNode == ZISL.AgentGeoNodes.length) LastGeoNode = 0;
        var gnode         = ZISL.AgentGeoNodes[LastGeoNode];
        do_geo_failover(gnode);
      }
    }
  }

  exports.DoWssConnect(ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: called by zagent.js to disable WSS when FIXLOG is running
exports.KillWss = function(next) {
  ZH.l('ZWss.KillWss');
  kill_wss(next);
}

// NOTE: Called by ZISL.handle_geo_state_change_event()
exports.DoGeoFailover = function(gnode) {
  return do_geo_failover(gnode);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZWss']={} : exports);

