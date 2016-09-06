
var T = require('./ztrace'); // TRACE (before strict)
"use strict";

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REQUIRES ------------------------------------------------------------------

var fs       = require('fs');

var ZPio     = require('./zpio');
var ZCLS     = require('./zcluster');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG SETTINGS ------------------------------------------------------------

var CatchMethodErrors = false;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

exports.Server  = null;
exports.Methods = null;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HTTP JSON RPC 2.0 ---------------------------------------------------------

function get_device_uuid(params) {
  if (ZH.IsDefined(params)                  &&
    ZH.IsDefined(params.data)               &&
    ZH.IsDefined(params.data.device)        &&
    ZH.IsDefined(params.data.device.uuid)) {
    return params.data.device.uuid; 
  } else {
    return null;
  }
}

exports.CreateHres = function(conn, params, id, wid, auth) {
 return {conn : conn, params : params, id : id, wid : wid, auth : auth};
}

function create_data_from_hres(hres) {
  var data = ZH.clone(hres);
  delete(data.conn);
  delete(data.params);
  delete(data.id);
  delete(data.auth);
  return data;
}

exports.CreateDataFromHres = function(hres) {
  return create_data_from_hres(hres);
}

exports.ChaosDropResponse = function(cnum) {
  ZH.e('CHAOS-MODE: ' + ZH.ChaosDescriptions[cnum]);
  return; // NOTE: this is a HARD drop -> do NOT even respond
}

exports.HandleCallbackError = function(method, cerr, hres) {
  if (cerr) {
    var etxt    = cerr.message ? cerr.message : cerr;
    var details = hres.error_details;
    ZH.e('Method: ' + method + ' CALLBACK-ERROR'); ZH.e(etxt);
    exports.RespondError(hres.conn, -32006, etxt,
                         hres.id, hres.wid, details, ZH.NoOp);
    return true;
  }
  return false;
}

exports.ReplyDenyDeviceKey = function(hres) {
  var err = ZS.Errors.DeviceKeyMismatch;
  exports.HandleCallbackError(null, err, hres);
}

exports.SendBackoff = function(duuid, hres) {
  ZH.l('send_backoff: U: ' + duuid);
  // CLONE collections  -> parallel events wont reset collections
  var net = ZH.CreateNetPerRequest(exports.Server);
  var sub = ZH.CreateSub(duuid);
  ZPio.SendAgentBackoff(net.plugin, net.collections, sub,
                        ZH.GetBackoffSeconds(), hres);
}

exports.HttpsRespondFile = function(response, body, type) {
  var rheaders = {'content-type'                : type,
                  'content-length'              : body.length,
                  'Access-Control-Allow-Origin' : '*' };
  response.writeHead(200, rheaders);
  response.write(body, 'utf8');
  response.end();
}

function respond_with_body(conn, id, cbody) {
  if        (conn.hsconn) {
    exports.HttpsRespondFile(conn.hsconn, cbody, 'application/json');
  } else if (conn.replier) {
    conn.replier.reply(id, cbody);
  } else if (conn.wsconn) {
    conn.wsconn.send(cbody, ZH.WssSendErr);
  } else { // SOCKET
    var cbuf = ZH.ConvertBodyToBinaryNetwork(cbody);
    conn.sock.write(cbuf, ZH.OnErrLog);
  }
}

exports.RespondError = function(conn, code, e, id, wid, details, next) {
  if (ZH.IsUndefined(id)) { id = 'null'; }
  var body  = {jsonrpc   : "2.0",
               id        : id,
               worker_id : wid,
               error     : {code       : code,
                            message    : e,
                            details    : details,
                            datacenter : ZH.MyDataCenter},
               status    : "ERROR"};
  var cbody = ZH.NetworkSerialize(body);
  respond_with_body(conn, id, cbody)
  next(e);
}

exports.Respond = function(hres, data) {
  var body  = {jsonrpc   : "2.0",
               id        : hres.id,
               worker_id : hres.wid,
               result    : data};
  var cbody = ZH.NetworkSerialize(body);
  respond_with_body(hres.conn, hres.id, cbody)
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEVICE-KEY & HTTPS-MAP ----------------------------------------------------

exports.RemoveHttpsMap = function(auuid, server) {
  ZH.e('ZCloud.RemoveHttpsMap: U: ' + auuid);
  var options = ZH.CentralHttpsMap[auuid];
  if (!options && !server) return;
  var cbu = options ? options.hostname + ':' + options.port :
                      server.hostname  + ':' + server.port;
  var ruuid   = ZH.ReverseHttpsMap[cbu];
  if (ruuid) { // NOTE: happens when HTTPS-Agent deletes ALL of its data
    ZH.e('REMOVE: DELETING: REPEAT HTTPS CALLBACK SERVER: U: ' + ruuid);
    delete(ZH.CentralHttpsMap   [ruuid]);
    delete(ZH.CentralHttpsKeyMap[ruuid]);
  }
  if (options) {
    delete(ZH.CentralHttpsMap   [auuid]);
    delete(ZH.CentralHttpsKeyMap[auuid]);
  }
  delete(ZH.ReverseHttpsMap[cbu]);
}

// NOTE: Needed on CENTRAL restart
function register_https_connection(net, auuid, params) {
  var server = params.data.server;
  if (server) {
    var dkey     = ZH.CentralDeviceKeyMap[auuid];
    var hostname = server.hostname;
    var port     = server.port;
    var skey     = server.key;
    var cbu      = hostname + ':' + port;
    var ruuid    = ZH.ReverseHttpsMap[cbu];
    if (ruuid) { // NOTE: happens when HTTPS-Agent deletes ALL of its data
      ZH.e('REGISTER: DELETING: REPEAT HTTPS CALLBACK SERVER: U: ' + ruuid);
      delete(ZH.CentralHttpsMap   [ruuid]);
      delete(ZH.CentralHttpsKeyMap[ruuid]);
    }
    var options = {hostname : hostname,
                   port     : port,
                   path     : '/subscriber',
                   method   : 'POST',
                   keepAlive      : true,
                   keepAliveMsecs : ZH.KeepAliveMSecs,
                   key      : fs.readFileSync(ZH.AgentConfig.ssl_key),
                   cert     : fs.readFileSync(ZH.AgentConfig.ssl_cert),
                   rejectUnauthorized: false
                  };
    ZH.CentralHttpsMap   [auuid] = options;
    ZH.CentralHttpsKeyMap[auuid] = skey;
    ZH.ReverseHttpsMap   [cbu]   = auuid;
    ZH.e('register_https_conn: U: ' + auuid + ' CB: ' + cbu);
    // NOTE: ZPio.GeoBroadcastHttpsAgent() is ASYNC
    ZPio.GeoBroadcastHttpsAgent(net.plugin, net.collections,
                                server, auuid, dkey);
  }
}

function initialize_central_device_key_map(net, duuid, params, next) {
  if (ZH.DisableDeviceKeyCheck) next(null, null);
  else {
    ZH.l('initialize_central_device_key_map: U: ' + duuid);
    var kkey = ZS.GetDeviceKey(duuid);
    net.plugin.do_get_field(net.collections.dk_coll, kkey, "device_key",
    function(gerr, dkey) {
      if (gerr) next(gerr, null);
      else {
        if (!dkey) {
          ZH.e('FAILED DEVICE-KEY NEVER ISSUED');
          next(new Error(ZS.Errors.DeviceKeyMismatch), null);
        } else {
          ZH.CentralDeviceKeyMap[duuid] = dkey;
          if (!ZH.DeviceKeyMatch(params.data.device)) {
            ZH.e('FAILED DEVICE-KEY CHECK');
            next(new Error(ZS.Errors.DeviceKeyMismatch), null);
          } else {
            next(null, null);
          }
        }
      }
    });
  }
}

function check_device_uuid(net, conn, params, dcheck, next) {
  if (!dcheck) next(null, false);
  else {
    var duuid = get_device_uuid(params);
    if      (!duuid)       next(null, true); // MISS
    else if (duuid === -1) next(null, false);
    else {
      initialize_central_device_key_map(net, duuid, params,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          ZH.l('check_device_uuid: U: ' + duuid + ' WS: ' + conn.wsconn +
               ' S: ' + conn.sock + ' HS: ' + conn.hsconn);
          if (conn.wsconn) {
            ZH.CentralWsconnMap[duuid] = conn.wsconn;
            next(null, false);
          } else if (conn.sock) {
            ZH.CentralSocketMap[duuid] = conn.sock;
            next(null, false);
          } else if (conn.hsconn) { // HTTPS
            var hso = ZH.CentralHttpsMap[duuid];
            if (!hso) {
              register_https_connection(net, duuid, params);
            }
            next(null, false);
          }
        }
      });
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE CLUSTER-METHOD & FRONTEND-REQUEST ----------------------------------

exports.HandleClusterMethod = function(method, conn, params, id, wid, data) {
  var m = exports.Methods[method];
  if (!m || typeof(m.handler) !== "function") {
    exports.RespondError(conn, -32601, 'Method (' + method + ') not found',
                         id, wid, null, ZH.NoOp);
    return;
  }
  var net    = ZH.CreateNetPerRequest(exports.Server);
  var dcheck = m.device_check || (method == "AgentOnline");
  check_device_uuid(net, conn, params, dcheck, function(cerr, pmiss) {
    if (cerr) {
      exports.RespondError(conn, -32022, cerr.message,
                           id, wid, method, ZH.NoOp);
    } else {
      if (pmiss) {
        exports.RespondError(conn, -32014, ZS.Errors.RequestMissingUUID,
                             id, wid, null, ZH.NoOp);
        return;
      }
      if (ZH.AmRouter && exports.Server.ready === false) {
        exports.RespondError(conn, -32100, ZS.Errors.HttpsServerNotReady,
                             id, wid, null, ZH.NoOp);
        return;
      }
      var auth = params.authentication;
      if (typeof(auth) === 'undefined') {
        exports.RespondError(conn, -32009, ZS.Errors.AuthenticationMissing,
                             id, wid, null, ZH.NoOp);
        return;
      }

      if (ZH.ClusterNetworkPartitionMode && !m.cluster_network_partition) {
        ZH.l('ClusterNetworkPartitionMode ACTIVE -> DROP REQUEST');
        return;
      }
      if (ZH.GeoMajority === false && !m.geo_network_partition) {
        ZH.l('NO GeoMajority -> DROP REQUEST');
        return;
      }

      if (exports.Server.synced === false && !m.no_sync) {
        exports.RespondError(conn, -32101, ZS.Errors.CentralNotSynced,
                             id, wid, null, ZH.NoOp);
        return;
      }

      if (ZH.FixLogActive && !m.fixlog) {
        ZH.l('ZH.FixLogActive: Internal-Request');
        exports.RespondError(conn, -32101, ZS.Errors.FixLogRunning,
                             id, wid, null, ZH.NoOp);
        return;
      }

      if (!ZH.DisableDeviceKeyCheck) {
        if (m.device_check) {
          if (!ZH.DeviceKeyMatch(params.data.device)) {
            ZH.e('FAILED DEVICE-KEY CHECK');
            var hres = exports.CreateHres(conn, params, id, wid, null);
            exports.ReplyDenyDeviceKey(hres);
            return;
          }
        }
      }

      if (m.geo_ready && !ZCLS.SelfGeoReady()) {
        exports.RespondError(conn, -32015, ZS.Errors.SelfGeoNotReady,
                             id, wid, null, ZH.NoOp);
        return;
      }
      if (ZH.AmRouter) { // ROUTER
        if (!m.router) {
          ZH.e('METHOD: ' + method + ' is a STORAGE METHOD');
          throw(new Error("LOGIC ERROR - ROUTER running STORAGE method"));
        }
      } else {           // STORAGE
        if (!m.storage) {
          ZH.e('METHOD: ' + method + ' is a ROUTER METHOD');
          throw(new Error("LOGIC ERROR - STORAGE running ROUTER method"));
        }
      }

      // CLONE collections  -> parallel events wont reset collections
      var next = m.next ? m.next : ZH.OnErrLog;
      try {
        m.handler(conn, params, id, wid, net, next);
      } catch(e) {
        if (CatchMethodErrors) {
          ZH.e('CENTRAL METHOD ERROR: name: ' + e.name +
               ' message: ' + e.message);
        } else {
          ZH.CloseLogFile(function() {
            throw(e);
          });
        }
      }
    }
  });
}

exports.HandleFrontendRequest = function(wss, wsconn, response, sock, mdata) {
  var net  = ZH.CreateNetPerRequest(exports.Server);
  var conn = {wss : wss, wsconn : wsconn, hsconn : response, sock : sock};
  var data;
  try { 
    //TODO ZH.NetworkDeserialize() is DEPRECATED, better parsing in ztls.js
    data = ZH.NetworkDeserialize(mdata);
  } catch(e) {
    exports.RespondError(conn, -32700, 'Parse error: (' + e + ')',
                         null, null, null, ZH.NoOp);
    return;
  }
  var id     = data.id;
  var wid    = data.worker_id;
  var method = data.method;
  var m      = exports.Methods[method];
  if (!m) {
    exports.RespondError(conn, -32601, 'Method (' + method + ') not found',
                         id, wid, null, ZH.NoOp);
    return;
  }
  var params = data.params;
  if (typeof(params) === 'undefined') {
    exports.RespondError(response, -32003, ZS.Errors.JsonRpcParamsMissing,
                         id, wid, null, ZH.NoOp);
    return;
  }
  var dcheck = m.device_check || (method == "AgentOnline");
  check_device_uuid(net, conn, params, dcheck, function(cerr, pmiss) {
    if (cerr) {
      exports.RespondError(conn, -32022, cerr.message,
                           id, wid, method, ZH.NoOp);
    } else {
      if (pmiss) {
        exports.RespondError(conn, -32014, ZS.Errors.RequestMissingUUID,
                             id, wid, null, ZH.NoOp);
        return;
      }
      var version = data.jsonrpc;
      if (version != '2.0') {
        exports.RespondError(conn, -32002, ZS.Errors.WrongJsonRpcId,
                             id, wid, null, ZH.NoOp);
        return;
      }
      var backoff = m.no_backoff ? false :
                    (ZH.ClusterVoteInProgress || ZH.YetToClusterVote ||
                     ZH.GeoVoteInProgress ||
                     ZH.FixLogActive || (ZCLS.GeoNodes.length === 0));
      ZH.l('backoff: '                   + backoff                  +
           ' m.no_backoff: '             + m.no_backoff                +
           ' ZH.ClusterVoteInProgress: ' + ZH.ClusterVoteInProgress +
           ' ZH.GeoVoteInProgress: '     + ZH.GeoVoteInProgress     +
           ' ZH.YetToClusterVote: '      + ZH.YetToClusterVote      +
           ' ZH.FixLogActive: '          + ZH.FixLogActive          +
           ' ZCLS.GeoNodes.length: '     + ZCLS.GeoNodes.length);
      if (backoff) {
        return exports.SendBackoff(params.data.device.uuid, {id : id});
      } else {
        if (ZH.ClusterNetworkPartitionMode) {
          ZH.l('ClusterNetworkPartitionMode ACTIVE -> REDIRECT OTHER DC');
          var hres = exports.CreateHres(conn, params, id, wid, null);
          redirect_other_known_datacenter(hres);
        } else if (ZH.GeoMajority === false) {
          ZH.l('NO GeoMajority -> REDIRECT OTHER DC');
          var hres = exports.CreateHres(conn, params, id, wid, null);
          redirect_other_known_datacenter(hres);
        } else {
          exports.HandleClusterMethod(method, conn, params, id, wid, data);
        }
      }
    }
  });
}

exports.InitializeMethods = function(methods, next) {
  exports.Methods = methods;
  var nmeth = Object.keys(exports.Methods).length;
  ZH.e('ZCloud.InitializeMethods: #M: ' + nmeth);
  next(null, null);
}

exports.AddMethods = function(methods, next) {
  if (!exports.Methods) throw(new Error("ZCloud.AddMethods LOGIC ERROR"));
  for (var name in methods) {
    exports.Methods[name] = methods[name];
  }
  var nmeth = Object.keys(exports.Methods).length;
  ZH.e('ZCloud.AddMethods: #M: ' + nmeth);
  next(null, null);
}

exports.Initialize = function(server) {
  exports.Server = server;
}

