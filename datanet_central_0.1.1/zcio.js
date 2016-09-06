"use strict";

var https, ZDoc, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  https = require('https');
  ZDoc  = require('./zdoc');
  ZS    = require('./zshared');
  ZH    = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function handle_generic_agent_response(data, next) {
  var err = null;
  var res = null;
  if (ZH.IsDefined(data.error))  err = data.error;
  if (ZH.IsDefined(data.result)) res = data.result;
  next(err, res);
}

function send_agent_standard_request(cli, auth, method, id, data, next) {
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  var cbody     = ZH.NetworkSerialize(jrpc_body);
  var hr_opts   = cli.GetMyAgentReqOpts();
  var pdata     = '';
  var req       = https.request(hr_opts, function(res) {
    ZH.l('<-|(A): RESPONSE: ' + method);
    res.setEncoding('utf8');
    res.on('data', function (chunk) { pdata += chunk; });
    res.on('end', function () {
      var data = ZH.ParseJsonRpcCall(method, pdata);
      handle_generic_agent_response(data, next);
    });
  });
  req.on('error', function(err) {
    next(err, null);
  });
  req.on('close', function() {
    if (pdata.length === 0) next(new Error(ZS.Errors.HttpsSocketClose), null);
  });
  req.write(cbody);
  req.end();
}

function send_agent_subscription_request(cli, method, schanid, next) {
  var id        = ZH.GetNextClientRpcID();
  var data      = {channel : {id : schanid}};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

function send_agent_request_for_zdoc(cli, ns, cn, key, method, data, next) {
  ZH.l('|->(A): ' + method + ': key: ' + key);
  var id        = ZH.GetNextClientRpcID();
  var jrpc_body = ZH.CreateJsonRpcBody(method, cli.ClientAuth, data, id);
  var cbody     = ZH.NetworkSerialize(jrpc_body);
  var hr_opts   = cli.GetMyAgentReqOpts();
  var pdata     = '';
  var req       = https.request(hr_opts, function(res) {
    ZH.l('<-|(A): RESPONSE: ' + method);
    res.setEncoding('utf8');
    res.on('data', function (chunk) { pdata += chunk; });
    res.on('end', function () {
      var data = ZH.ParseJsonRpcCall(method, pdata);
      if (data.error) {
        next(data.error, null);
      } else {
        var ncrdt   = data.result.crdt;
        var njson   = ZH.CreatePrettyJson(ncrdt);
        var nzdoc   = new ZDoc.Create(ns, cn, njson, ncrdt);
        nzdoc.client = cli;
        next(null, nzdoc);
      }
    });
  });
  req.on('error', function(err) {
    next(err, null);
  });
  req.on('close', function() {
    if (pdata.length === 0) next(new Error(ZS.Errors.HttpsSocketClose), null);
  });
  req.write(cbody);
  req.end();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CACHE ---------------------------------------------------------------------

exports.SendAgentCache = function(cli, ns, cn, key, pin, watch, sticky, next) {
  var method    = "ClientCache";
  ZH.l('|->(A): ' + method + ': key: ' + key);
  var data      = {namespace  : ns,
                   collection : cn,
                   key        : key,
                   pin        : pin,
                   watch      : watch,
                   sticky     : sticky};
  var id        = ZH.GetNextClientRpcID();
  var jrpc_body = ZH.CreateJsonRpcBody(method, cli.ClientAuth, data, id);
  var cbody     = ZH.NetworkSerialize(jrpc_body);
  var hr_opts   = cli.GetMyAgentReqOpts();
  var pdata     = '';
  var req       = https.request(hr_opts, function(res) {
    res.setEncoding('utf8');
    res.on('data', function (chunk) { pdata += chunk; });
    res.on('end', function () {
      ZH.l('<-|(A): RESPONSE: ' + method);
      var data = ZH.ParseJsonRpcCall(method, pdata);
      if (data.error) {
        next(data.error, null);
      } else {
        var ncrdt = data.result.crdt;
        var njson = ZH.CreatePrettyJson(ncrdt);
        var nzdoc = new ZDoc.Create(ns, cn, njson, ncrdt);
        nzdoc.client = cli;
        next(null, nzdoc);
      }
    });
  });
  req.on('error', function(err) {
    next(err, null);
  });
  req.on('close', function() {
    if (pdata.length === 0) next(new Error(ZS.Errors.HttpsSocketClose), null);
  });
  req.write(cbody);
  req.end();
}

exports.SendAgentEvict = function(cli, ns, cn, key, next) {
  var method = "ClientEvict";
  var id     = ZH.GetNextClientRpcID();
  var data   = {namespace : ns, collection : cn, key : key};
  var auth   = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentLocalEvict = function(cli, ns, cn, key, next) {
  var method = "ClientLocalEvict";
  var id     = ZH.GetNextClientRpcID();
  var data   = {namespace : ns, collection : cn, key : key};
  var auth   = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXPIRE --------------------------------------------------------------------

exports.SendAgentExpire = function(cli, ns, cn, key, expire, next) {
  var method = "ClientExpire";
  var id     = ZH.GetNextClientRpcID();
  var data   = {namespace  : ns,
                collection : cn,
                key        : key,
                expire     : expire};
  var auth   = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}



// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT ISOLATION -----------------------------------------------------------

exports.SendAgentClientIsolation = function(cli, b, next) {
  ZH.l('|->(A): ClientIsolation: value: ' + b);
  var method    = 'ClientIsolation';
  var id        = ZH.GetNextClientRpcID();
  var data      = {value : b};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT NOTIFY --------------------------------------------------------------

exports.SendAgentSetNotify = function(cli, cmd, url, next) {
  ZH.l('|->(A): ClientNotfy: cmd: ' + cmd + ' url: ' + url);
  var method    = 'ClientNotify';
  var id        = ZH.GetNextClientRpcID();
  var data      = {command : cmd, url : url};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRITE-OPERATIONS ----------------------------------------------------------

exports.SendAgentStore = function(cli, ns, cn, key, sep, ex, json, ismc, next) {
  var method = 'ClientStore';
  var data   = {namespace   : ns,
                collection  : cn,
                key         : key,
                separate    : sep,
                expiration  : ex,
                json        : json,
                is_memcache : ismc}
  send_agent_request_for_zdoc(cli, ns, cn, key, method, data, next);
}

// EXPORT:  SendAgentCommit()
// PURPOSE: Client sends a COMMIT to Agent and processes response
//
exports.SendAgentCommit = function(cli, ns, cn, key, crdt, oplog, next){
  var method = 'ClientCommit';
  var data   = {namespace  : ns,
                collection : cn, 
                key        : key,
                crdt       : crdt,
                oplog      : oplog}
  send_agent_request_for_zdoc(cli, ns, cn, key, method, data, next);
}

// EXPORT:  SendAgentPull()
// PURPOSE: Client sends a PULL to Agent and processes response
//
exports.SendAgentPull = function(cli, ns, cn, key, crdt, oplog, next) {
  ZH.l('|->(A): ClientPull: key: ' + key);
  var method    = 'ClientPull';
  var id        = ZH.GetNextClientRpcID();
  var data      = {namespace : ns, collection : cn, 
                   key : key, crdt : crdt, oplog : oplog}
  var jrpc_body = ZH.CreateJsonRpcBody(method, cli.ClientAuth, data, id);
  var cbody     = ZH.NetworkSerialize(jrpc_body);
  var hr_opts   = cli.GetMyAgentReqOpts();
  var pdata     = '';
  var req       = https.request(hr_opts, function(res) {
    ZH.l('<-|(A): RESPONSE: ' + method);
    res.setEncoding('utf8');
    res.on('data', function (chunk) { pdata += chunk; });
    res.on('end', function () {
      var data = ZH.ParseJsonRpcCall(method, pdata);
      if (data.error) {
        next(data.error, null);
      } else {
        var ncrdt = data.result.crdt;
        var mjson = data.result.json;
        ZDoc.CreateZDocFromPullResponse(ns, cn, key, ncrdt, mjson, oplog,
        function(cerr, nzdoc) {
          if (cerr) next(cerr, null);
          else {
            nzdoc.client = cli;
            next(null, zdoc);
          }
        });
      }
    });
  });
  req.on('error', function(err) {
    next(err, null);
  });
  req.on('close', function() {
    if (pdata.length === 0) next(new Error(ZS.Errors.HttpsSocketClose), null);
  });
  req.write(cbody);
  req.end();
}

exports.SendAgentRemove = function(cli, ns, cn, key, next) {
  ZH.l('|->(A): ClientRemove: K: ' + key);
  var method    = 'ClientRemove';
  var id        = ZH.GetNextClientRpcID();
  var data      = {namespace : ns, collection : cn,  key : key};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentStatelessCommit = function(cli, ns, cn, key,
                                            oplog, rchans, next) {
  var method = 'ClientStatelessCommit';
  var data   = {namespace            : ns,
                collection           : cn, 
                key                  : key,
                oplog                : oplog,
                replication_channels : rchans};
  send_agent_request_for_zdoc(cli, ns, cn, key, method, data, next);
}

exports.SendAgentMemcacheCommit = function(cli, ns, cn, key, oplog, rchans,
                                           sep, ex, next){
  var method = 'ClientMemcacheCommit';
  var data   = {namespace            : ns,
                collection           : cn, 
                key                  : key,
                oplog                : oplog,
                replication_channels : rchans,
                separate             : sep,
                expiration           : ex};
  send_agent_request_for_zdoc(cli, ns, cn, key, method, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FETCH ---------------------------------------------------------------------

exports.SendAgentFetch = function(cli, ns, cn, key, next) {
  ZH.l('|->(A): ClientFetch: K: ' + key);
  var method    = 'ClientFetch';
  var id        = ZH.GetNextClientRpcID();
  var data      = {namespace : ns, collection : cn,  key : key};
  send_agent_request_for_zdoc(cli, ns, cn, key, method, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIND ----------------------------------------------------------------------

exports.SendAgentFind = function(cli, ns, cn, query, next) {
  ZH.l('|->(A): ClientFind');
  var method    = 'ClientFind';
  var data      = {namespace : ns, collection : cn, query : query};
  var id        = ZH.GetNextClientRpcID();
  var jrpc_body = ZH.CreateJsonRpcBody(method, cli.ClientAuth, data, id);
  var cbody     = ZH.NetworkSerialize(jrpc_body);
  var hr_opts   = cli.GetMyAgentReqOpts();
  var pdata     = '';
  var req       = https.request(hr_opts, function(res) {
    ZH.l('<-|(A): RESPONSE: ' + method);
    res.setEncoding('utf8');
    res.on('data', function (chunk) { pdata += chunk; });
    res.on('end', function () {
      var rdata = ZH.ParseJsonRpcCall(method, pdata);
      if (rdata.error) {
        next(rdata.error, null);
      } else {
        next(null, rdata.result.jsons);
      }
    });
  });
  req.on('error', function(err) {
    next(err, null);
  });
  req.on('close', function() {
    if (pdata.length === 0) next(new Error(ZS.Errors.HttpsSocketClose), null);
  });
  req.write(cbody);
  req.end();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SCAN ----------------------------------------------------------------------

exports.SendAgentScan = function(cli, ns, cn, next) {
  ZH.l('|->(A): ClientScan');
  var method    = 'ClientScan';
  var data      = {namespace : ns, collection : cn};
  var id        = ZH.GetNextClientRpcID();
  var jrpc_body = ZH.CreateJsonRpcBody(method, cli.ClientAuth, data, id);
  var cbody     = ZH.NetworkSerialize(jrpc_body);
  var hr_opts   = cli.GetMyAgentReqOpts();
  var pdata     = '';
  var req       = https.request(hr_opts, function(res) {
    ZH.l('<-|(A): RESPONSE: ' + method);
    res.setEncoding('utf8');
    res.on('data', function (chunk) { pdata += chunk; });
    res.on('end', function () {
      var rdata = ZH.ParseJsonRpcCall(method, pdata);
      if (rdata.error) {
        next(rdata.error, null);
      } else {
        ZDoc.CreateZDocs(ns, cn, rdata.result.crdts, next);
      }
    });
  });
  req.on('error', function(err) {
    next(err, null);
  });
  req.on('close', function() {
    if (pdata.length === 0) next(new Error(ZS.Errors.HttpsSocketClose), null);
  });
  req.write(cbody);
  req.end();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBE -----------------------------------------------------------------

exports.SendAgentSubscribe = function(cli, schanid, next) {
  ZH.l('|->(A): ClientSubscribe: R: ' + schanid);
  var method = 'ClientSubscribe';
  send_agent_subscription_request(cli, method, schanid, next);
}

exports.SendAgentUnsubscribe = function(cli, schanid, next) {
  ZH.l('|->(A): ClientUnsubscribe: R: ' + schanid);
  var method = 'ClientUnsubscribe';
  send_agent_subscription_request(cli, method, schanid, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STATION USER ON AGENT -----------------------------------------------------

exports.SendAgentStationUser = function(cli, auth, next) {
  ZH.l('|->(A): StationUser: UN: ' + auth.username);
  var method    = 'ClientStationUser';
  var id        = ZH.GetNextClientRpcID();
  var data      = {}; // NOTE data is just auth
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DESTATION USER FROM AGENT -------------------------------------------------

exports.SendAgentDestationUser = function(cli, next) {
  ZH.l('|->(A): Destation');
  var method    = 'ClientDestationUser';
  var id        = ZH.GetNextClientRpcID();
  var data      = {}; // NOTE data is just auth
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET STATIONED USERS -------------------------------------------------------

exports.SendAgentGetStationedUsers = function(cli, next) {
  ZH.l('|->(A): ClientGetStationedUsers');
  var method    = 'ClientGetStationedUsers';
  var id        = ZH.GetNextClientRpcID();
  var data      = {}; // NOTE data is just auth
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SWITCH USER ON AGENT ------------------------------------------------------

exports.SendAgentSwitchUser = function(cli, auth, next) {
  ZH.l('|->(A): SwitchUser: UN: ' + auth.username);
  var method    = 'ClientSwitchUser';
  var id        = ZH.GetNextClientRpcID();
  var data      = {}; // NOTE data is just auth
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET INFO CALLS ------------------------------------------------------------

exports.SendAgentGetAgentSubscriptions = function(cli, next) {
  ZH.l('|->(A): ClientGetAgentSubscriptions');
  var method    = 'ClientGetAgentSubscriptions';
  var id        = ZH.GetNextClientRpcID();
  var data      = {}; // NOTE data is just auth
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentGetUserSubscriptions = function(cli, next) {
  ZH.l('|->(A): ClientGetUserSubscriptions');
  var method    = 'ClientGetUserSubscriptions';
  var id        = ZH.GetNextClientRpcID();
  var data      = {}; // NOTE data is just auth
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentGetUserInfo = function(cli, auth, next) {
  ZH.l('|->(A): ClientGetUserInfo');
  var method    = 'ClientGetUserInfo';
  var id        = ZH.GetNextClientRpcID();
  var data      = {}; // NOTE data is just auth
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentGetClusterInfo = function(cli, next) {
  ZH.l('|->(A): ClientGetClusterInfo');
  var method    = 'ClientGetClusterInfo';
  var id        = ZH.GetNextClientRpcID();
  var data      = {}; // NOTE data is just auth
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentGetLatencies = function(cli, next) {
  ZH.l('|->(A): ClientGetLatencies');
  var method    = 'ClientGetLatencies';
  var id        = ZH.GetNextClientRpcID();
  var data      = {}; // NOTE data is just auth
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentGetMemcacheClusterState = function(cli, clname, next) {
  ZH.l('|->(A): ClientGetMemcacheClusterState: CL: ' + clname);
  var method    = 'ClientGetMemcacheClusterState';
  var id        = ZH.GetNextClientRpcID();
  var data      = {cluster_name : clname};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentIsCached = function(cli, ks, next) {
  ZH.l('|->(A): ClientIsCached');
  var method    = 'ClientIsCached';
  var id        = ZH.GetNextClientRpcID();
  var data      = {Ks : ks};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HEARTBEAT -----------------------------------------------------------------

exports.SendAgentHeartbeat = function(cli, cmd, field, uuid,
                                      mlen, trim, isi, isa, next) {
  ZH.l('|->(A): ClientHeartbeat');
  var method    = 'ClientHeartbeat';
  var id        = ZH.GetNextClientRpcID();
  var data      = {command      : cmd,
                   field        : field,
                   uuid         : uuid,
                   max_size     : mlen,
                   trim         : trim,
                   is_increment : isi,
                   is_array     : isa};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MESSAGE -------------------------------------------------------------------

exports.SendAgentMessage = function(cli, mtxt, next) {
  ZH.l('|->(A): ClientMessage');
  var method = 'ClientMessage';
  var id     = ZH.GetNextClientRpcID();
  var data   = {text : mtxt};
  var auth   = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentRequest = function(cli, mtxt, next) {
  ZH.l('|->(A): ClientRequest');
  var method = 'ClientRequest';
  var id     = ZH.GetNextClientRpcID();
  var data   = {text : mtxt};
  var auth   = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// USER MANAGEMENT -----------------------------------------------------------

exports.SendAgentAddUser = function(cli, username, password, role, next) {
  ZH.l('|->(A): ClientAddUser');
  var method    = 'ClientAddUser';
  var id        = ZH.GetNextClientRpcID();
  var data      = {username : username, password : password, role : role};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentRemoveUser = function(cli, username, next) {
  ZH.l('|->(A): ClientRemoveUser');
  var method    = 'ClientRemoveUser';
  var id        = ZH.GetNextClientRpcID();
  var data      = {username : username};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentGrantUser = function(cli, username, schanid, priv, next) {
  ZH.l('|->(A): ClientGrantUser');
  var method    = 'ClientGrantUser';
  var id        = ZH.GetNextClientRpcID();
  var data      = {username  : username,
                   channel   : {id : schanid},
                   privilege : priv};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}

exports.SendAgentRemoveDataCenter = function(cli, dcuuid, next) {
  ZH.l('|->(A): ClientRemoveDataCenter');
  var method    = 'ClientRemoveDataCenter';
  var id        = ZH.GetNextClientRpcID();
  var data      = {remove_datacenter : dcuuid};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT KEYS ----------------------------------------------------------------

exports.SendAgentGetAgentKeys = function(cli, duuid, nkeys,
                                         minage, wonly, next) {
  ZH.l('|->(A): ClientGetAgentKeys: U: ' + duuid);
  var method    = 'ClientGetAgentKeys';
  var id        = ZH.GetNextClientRpcID();
  var data      = {agent       : {uuid : duuid},
                   num_keys    : nkeys,
                   minimum_age : minage,
                   watch_only  : wonly};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SHUTDOWN ------------------------------------------------------------------

exports.SendAgentShutdown = function(cli, next) {
  ZH.l('|->(A): ClientShutdown');
  var method    = 'ClientShutdown';
  var id        = ZH.GetNextClientRpcID();
  var data      = {};
  var auth      = cli.ClientAuth;
  send_agent_standard_request(cli, auth, method, id, data, next);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZCio']={} : exports);

