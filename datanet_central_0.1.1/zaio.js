"use strict";

var ZDelt, ZWss, ZISL, ZDack, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZDelt    = require('./zdeltas');
  ZWss     = require('./zwss');
  ZISL     = require('./zisolated');
  ZDack    = require('./zdack');
  ZS       = require('./zshared');
  ZH       = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var NumberDeltasPerBatch   = 5;
var BetweenBatchDrainSleep = 500; // 500 MS


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

var DebugIntentionalBadDeviceKeyAgentOnline = false;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function agent_wssend_error(err) {
  if (err) {
    ZH.e('AGENT: WSCONN.send: ERROR: ' + err);
    ZH.Agent.wss = null;
    ZWss.DoWssConnect(ZH.NoOp);
  }
}

function wss_send_to_central(jrpc_body) {
  var cbody = ZH.NetworkSerialize(jrpc_body);
  if (!ZH.AmBrowser) {
    if (ZH.Agent.wss) ZH.Agent.wss.send(cbody, agent_wssend_error); 
    else              agent_wssend_error(ZS.Errors.WssUnavailable);
  } else {
    if        (!ZH.Agent.cconn) {
      ZH.WssSendErr(new Error(ZS.Errors.CentralConnDown));
    } else if (!ZH.Agent.wss) {
      ZH.WssSendErr(new Error(ZS.Errors.WssUnavailable));
    } else {
      try      { ZH.Agent.wss.send(cbody); }
      catch(e) { ZH.WssSendErr(e); }
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEVICE ONLINE -------------------------------------------------------------

function send_central_agent_online(plugin, collections,
                                   created, ainfo, value, next) {
  var auth   = ZH.NobodyAuth;
  var method = 'AgentOnline';
  var id     = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data   = {device  : {uuid : ZH.MyUUID},
                created : created,
                value   : value};
  if (ainfo && ainfo.permissions) {
    data.permissions = ainfo.permissions;
  }
  if (ainfo && ainfo.subscriptions) {
    data.subscriptions = ainfo.subscriptions;
  }
  if (ainfo && ainfo.stationedusers) {
    data.stationedusers = ainfo.stationedusers;
  }
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

exports.SendCentralAgentOnline = function(plugin, collections, value, next) {
  ZH.l('|->(C): AgentOnline: value: ' + value);
  if (ZH.Agent.dconn) {
    ZDack.GetAgentLastCreated(plugin, collections, function(gerr, created) {
      if (gerr) next(gerr, null);
      else {
        ZISL.GetAgentInfo(plugin, collections, function(ierr, ainfo) {
          if (ierr) next(ierr, null);
          else {
            send_central_agent_online(plugin, collections,
                                      created, ainfo, value, next);
          }
        });
      }
    });
  } else {
    send_central_agent_online(plugin, collections, -1, {}, value, next);
  }
}

exports.SendCentralAgentRecheck = function(plugin, collections,
                                           ocreated, next) {
  var auth   = ZH.NobodyAuth;
  var method = 'AgentRecheck';
  var id     = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data   = {device  : {uuid : ZH.MyUUID},
                agent   : {uuid : ZH.MyUUID},
                created : ocreated};
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET AGENT KEYS ------------------------------------------------------------

exports.SendCentralAgentGetAgentKeys = function(plugin, collections,
                                                duuid, nkeys, minage, wonly,
                                                auth, next) {
  ZH.l('|->(C): AgentGetAgentKeys: N: ' + nkeys);
  var method    = 'AgentGetAgentKeys';
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data      = {device      : {uuid : ZH.MyUUID},
                   agent       : {uuid : duuid},
                   num_keys    : nkeys,
                   watch_only  : wonly,
                   minimum_age : minage};
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// APP-SERVER_CLUSTER & MEMCACHE ANNOUNCE-NEW-CLUSTER ------------------------

function send_central_announce_new_cluster_method(plugin, collections,
                                                  method, data, next) {
  var auth      = ZH.NobodyAuth;
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

exports.SendCentralAnnounceNewAppServerCluster =
                                       function(plugin, collections,
                                                clname, mcname, cnodes, next) {
  ZH.l('|->(C): AnnounceNewAppServerCluster: N: ' + clname);
  var method = 'AnnounceNewAppServerCluster';
  var data   = {device                : {uuid : ZH.MyUUID},
                cluster_name          : clname,
                memcache_cluster_name : mcname,
                cluster_nodes         : cnodes};
  send_central_announce_new_cluster_method(plugin, collections,
                                           method, data, next);
}

exports.SendCentralAnnounceNewMemcacheCluster = 
                                      function(plugin, collections,
                                               clname, ascname, cnodes, next) {

  ZH.l('|->(C): AnnounceNewMemcacheCluster: N: ' + clname);
  var method = 'AnnounceNewMemcacheCluster';
  var data   = {device                  : {uuid : ZH.MyUUID},
                cluster_name            : clname,
                app_server_cluster_name : ascname,
                cluster_nodes           : cnodes};
  send_central_announce_new_cluster_method(plugin, collections,
                                           method, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// APP-SERVER_CLUSTER MESSAGE & REQUEST --------------------------------------

function send_central_app_server_request(plugin, collections,
                                         mbody, auth, method, next) {
  ZH.l('|->(C): ' + method);
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data      = {device : {uuid : ZH.MyUUID},
                   body   : mbody};
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

exports.SendCentralAgentMessage = function(plugin, collections,
                                           mbody, auth, next) {
  var method = 'AgentMessage';
  send_central_app_server_request(plugin, collections,
                                  mbody, auth, method, next);
}

exports.SendCentralAgentRequest = function(plugin, collections,
                                           mbody, auth, next) {
  var method = 'AgentRequest';
  send_central_app_server_request(plugin, collections,
                                  mbody, auth, method, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT CACHE & EVICT -------------------------------------------------------

exports.SendCentralCache = function(plugin, collections,
                                    ks, watch, auth, next) {
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  ZH.l('|->(C): AgentCache: key: ' + ks.kqk);
  var method    = 'AgentCache';
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data      = {device : {uuid : ZH.MyUUID},
                   agent  : {uuid : ZH.MyUUID},
                   ks     : ks,
                   watch  : watch};
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

function send_central_evict_method(plugin, collections, method, ks, next) {
  var auth      = ZH.NobodyAuth; // Central does NOT USER-AUTHORIZE EVICTs
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data      = {device : {uuid : ZH.MyUUID},
                   agent  : {uuid : ZH.MyUUID},
                   ks     : ks};
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

exports.SendCentralEvict = function(plugin, collections, ks, next) {
  ZH.l('|->(C): AgentEvict: key: ' + ks.kqk);
  var method = 'AgentEvict';
  send_central_evict_method(plugin, collections, method, ks, next);
}

exports.SendCentralLocalEvict = function(plugin, collections, ks, next) {
  ZH.l('|->(C): AgentLocalEvict: key: ' + ks.kqk);
  var method = 'AgentLocalEvict';
  send_central_evict_method(plugin, collections, method, ks, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DELTA ---------------------------------------------------------------

function cleanup_delta_before_send_central(dentry) {
  var pdentry = ZH.clone(dentry);
  var meta    = pdentry.delta._meta;
  delete(meta.dirty_central);
  ZDelt.DeleteMetaTimings(pdentry)
  meta.agent_sent = ZH.GetMsTime();
  return pdentry;
}

//TODO AckAgentDelta does not exist -> NO CALLBACK HERE
exports.SendCentralAgentDelta = function(plugin, collections, ks, dentry, auth,
                                         next) {
  ZH.l('|->(C): AgentDelta: ' + ZH.SummarizeDelta(ks.key, dentry));
  if (DebugIntentionalBadDeviceKeyAgentOnline) {
    ZH.DeviceKey = "INTENTIONAL DEVICE-KEY ERROR"; // USED FOR TESTING
  }
  var pdentry   = cleanup_delta_before_send_central(dentry);
  var method    = 'AgentDelta';
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data      = {device : {uuid : ZH.MyUUID},
                   agent  : {uuid : ZH.MyUUID},
                   ks     : ks,
                   dentry : pdentry};
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DENTRIES ------------------------------------------------------------

function send_batch_central_agent_dentries(net, ks, dentries, next) {
  var method    = 'AgentDentries';
  var id        = ZDelt.GetNextAgentRpcID(net.plugin, net.collections);
  var data      = {device    : {uuid : ZH.MyUUID},
                   agent     : {uuid : ZH.MyUUID},
                   ks        : ks,
                   dentries  : dentries};
  var jrpc_body = ZH.CreateJsonRpcBody(method, ZH.NobodyAuth, data, id);
  wss_send_to_central(jrpc_body);
  next(null, null);
}

function do_send_central_agent_dentries(net, ks, dentries, next) {
  if (dentries.length <= NumberDeltasPerBatch) { // FINAL ONE
    ZH.l('do_send_central_agent_dentries: K: ' + ks.kqk + ' FINAL BATCH');
    send_batch_central_agent_dentries(net, ks, dentries, next);
  } else {
    ZH.l('do_send_central_agent_dentries: K: ' + ks.kqk + ' SEND BATCH');
    var pdentries = [];
    for (var i = 0; i < NumberDeltasPerBatch; i++) {
      pdentries.push(dentries.shift()); // SHIFT FROM DENTRIES[] TO PDENTRIES[]
      if (dentries.length === 0) break;
    }
    send_batch_central_agent_dentries(net, ks, pdentries, function(serr, sres) {
      if (serr) next(serr, null);
      else {
        if (dentries.length == 0) next(null, null);
        else {
          setTimeout(function() {
            do_send_central_agent_dentries(net, ks, dentries, next);
          }, BetweenBatchDrainSleep);
        }
      }
    });
  }
}

exports.SendCentralAgentDentries = function(net, ks, authdentries, next) {
  var dentries = [];
  for (var i = 0; i < authdentries.length; i++) {
    var ad               = authdentries[i];
    var dentry           = ZH.clone(ad.dentry);
    ZDelt.CleanupAgentDeltaBeforeSend(dentry);
    // NOTE AuthorDeltas send AUTHORIZATION via ad.auth
    dentry.authorization = ZH.clone(ad.auth);
    var pdentry          = cleanup_delta_before_send_central(dentry);
    dentries.push(pdentry);
  }
  do_send_central_agent_dentries(net, ks, dentries, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NEED MERGE ----------------------------------------------------------------

exports.SendCentralNeedMerge = function(plugin, collections,
                                        ks, gcv, nd, next) {
  ZH.l('|->(C): AgentNeedMerge: K: ' + ks.kqk);
  var auth   = ZH.NobodyAuth;
  var method = 'AgentNeedMerge';
  var id     = ZDelt.GetNextAgentRpcID(plugin, collections);
  var nkey   = ZS.GetNeedMergeRequestID(ks);
  plugin.do_set_field(collections.kinfo_coll, nkey, "id", id,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var data      = {device     : {uuid : ZH.MyUUID},
                       agent      : {uuid : ZH.MyUUID},
                       ks         : ks,
                       gc_version : gcv,
                       num_deltas : nd};
      var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
      var rdata     = ZH.clone(data);
      rdata.method              = method;
      ZH.AgentWssRequestMap[id] = rdata;
      wss_send_to_central(jrpc_body);
      next(null, null);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CHANNEL SUBSCRIBE ---------------------------------------------------------

function send_central_channel_request(plugin, collections, method, schanid,
                                      auth, next) {
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data      = {device  : {uuid : ZH.MyUUID},
                   agent   : {uuid : ZH.MyUUID},
                   channel : {id : schanid}};
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

exports.SendCentralSubscribe = function(plugin, collections, schanid, auth,
                                        next) {
  ZH.l('|->(C): AgentSubscribe: R: ' + schanid);
  var method = 'AgentSubscribe';
  send_central_channel_request(plugin, collections, method, schanid,
                               auth, next);
}

exports.SendCentralUnsubscribe = function(plugin, collections, schanid, auth,
                                          next) {
  ZH.l('|->(C): AgentUnsubscribe: R: ' + schanid);
  var method = 'AgentUnsubscribe';
  send_central_channel_request(plugin, collections, method, schanid,
                               auth, next);
}

exports.SendCentralAgentHasSubscribePermissions = function(plugin, collections,
                                                           schanid, auth,
                                                           next) {
  ZH.l('|->(C): AgentHasSubscribePermissions: R: ' + schanid);
  var method = 'AgentHasSubscribePermissions';
  send_central_channel_request(plugin, collections, method, schanid,
                               auth, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET PERMISSIONS -----------------------------------------------------------

function send_central_user_method(plugin, collections, method, auth, next) {
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data      = {device : {uuid : ZH.MyUUID},
                   agent  : {uuid : ZH.MyUUID}};
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

exports.SendCentralAgentAuthenticate = function(plugin, collections,
                                                auth, next) {
  ZH.l('|->(C): AgentAuthenticate: N: ' + auth.username);
  var method = 'AgentAuthenticate';
  send_central_user_method(plugin, collections, method, auth, next);
}

exports.SendCentralAdminAuthenticate = function(plugin, collections,
                                                auth, next) {
  ZH.l('|->(C): AdminAuthenticate: N: ' + auth.username);
  var method = 'AdminAuthenticate';
  send_central_user_method(plugin, collections, method, auth, next);
}

exports.SendCentralGetUserSubscriptions = function(plugin, collections, auth,
                                                   next) {
  ZH.l('|->(C): AgentGetUserSubscriptions: N: ' + auth.username);
  var method = 'AgentGetUserSubscriptions';
  send_central_user_method(plugin, collections, method, auth, next);
}

exports.SendCentralGetUserInfo = function(plugin, collections, auth, next) {
  ZH.l('|->(C): AgentGetUserInfo: N: ' + auth.username);
  var method = 'AgentGetUserInfo';
  send_central_user_method(plugin, collections, method, auth, next);
}

exports.SendCentralGetUserChannelPermissions = function(plugin, collections,
                                                        rchans, auth, next) {
  ZH.l('|->(C): AgentGetUserChannelPermissions: N: ' + auth.username);
  var method    = 'AgentGetUserChannelPermissions';
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var data      = {device               : {uuid : ZH.MyUUID},
                   agent                : {uuid : ZH.MyUUID},
                   replication_channels : rchans};
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STATION USER --------------------------------------------------------------

exports.SendCentralStationUser = function(plugin, collections, auth, next) {
  ZH.l('|->(C): AgentStationUser: N: ' + auth.username);
  var method = 'AgentStationUser';
  send_central_user_method(plugin, collections, method, auth, next);
}

exports.SendCentralDestationUser = function(plugin, collections, auth, next) {
  ZH.l('|->(C): AgentDestationUser: N: ' + auth.username);
  var method = 'AgentDestationUser';
  send_central_user_method(plugin, collections, method, auth, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// USER MANAGEMENT -----------------------------------------------------------

function send_central_admin_method(plugin, collections,
                                   method, data, auth, next) {
  ZH.l('|->(C): ' + method);
  var id        = ZDelt.GetNextAgentRpcID(plugin, collections);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var rdata     = ZH.clone(data);
  rdata.method              = method;
  rdata.next                = next;
  ZH.AgentWssRequestMap[id] = rdata;
  wss_send_to_central(jrpc_body);
}

exports.SendCentralAddUser = function(plugin, collections,
                                      username, password, role,
                                      auth, hres, next) {
  var method = 'AdminAddUser';
  var data   = {device   : {uuid : ZH.MyUUID},
                username : username,
                password : password,
                role     : role};
  send_central_admin_method(plugin, collections, method, data, auth, next);
}

exports.SendCentralRemoveUser = function(plugin, collections, username, auth,
                                         hres, next) {
  var method = 'AdminRemoveUser';
  var data   = {device : {uuid : ZH.MyUUID}, username : username};
  send_central_admin_method(plugin, collections, method, data, auth, next);
}

exports.SendCentralGrantUser = function(plugin, collections, username,
                                        schanid, priv, auth, hres, next) {
  var method = 'AdminGrantUser';
  var data      = {device    : {uuid : ZH.MyUUID},
                   username  : username,
                   channel   : {id        : schanid,
                                privilege : priv}
                  };
  send_central_admin_method(plugin, collections, method, data, auth, next);
}

exports.SendCentralRemoveDataCenter = function(plugin, collections,
                                               dcuuid, auth, next) {
  var method = 'AdminRemoveDataCenter';
  var data   = {device            : {uuid : ZH.MyUUID},
                remove_datacenter : dcuuid};
  send_central_admin_method(plugin, collections, method, data, auth, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by zc_proxy
exports.WssSendToCentral = function(jrpc_body) {
  wss_send_to_central(jrpc_body);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZAio']={} : exports);

