"use strict";

var fs       = require('fs');
var tls      = require('tls');

var ZCLS     = require('./zcluster');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var MaxGeoConnErrors = 3;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// TLS SERVER ----------------------------------------------------------------

exports.Handler = null;

var ConnMap     = {};
function get_connmap_id(data_id, remote_addr, remote_port) {
  return data_id + '_' + remote_addr + '_' + remote_port;
}

var Reply = function() {
  var me = this;
  this.reply = function(id, data) {
    var cid  = get_connmap_id(id, me.remoteAddress, me.remotePort);
    //ZH.l('TLS SERVER REPLY: CID: ' + cid);
    var s = ConnMap[cid];
    if (s.writable) {
      s.write(data + "\n");
    } else {
      ZH.l('TLS SERVER: REPLY: ERROR: socket not writable -> CLOSE');
      s.destroy();
    }
  }
}

function handle_client_response(s, rdata) {
  var raddr             = s.remoteAddress;
  var rport             = s.remotePort;
  var cid               = get_connmap_id(rdata.id, raddr, rport);
  //ZH.l('TLS Server ON-DATA: CID: ' + cid);
  ConnMap[cid]          = s;
  var replier           = new Reply();
  replier.remoteAddress = raddr;
  replier.remotePort    = rport;
  exports.Handler(replier, rdata);
}

function on_data_handle(s, data, network_data, nkey, handler) {
  data      = String(data);
  //ZH.l('TLS RECEIVED: ' + data);
  var ldata = data.split("\n");
  var fdata = [];
  for (var i = 0; i < ldata.length; i++) {
    if (ldata[i].length !== 0) {
      fdata.push(ldata[i]);
    }
  }

  var sdata          = network_data[nkey];
  network_data[nkey] = null;

  if (sdata) { // PREPEND sdata to FIRST fdata
    sdata    += fdata[0];
    fdata[0]  = sdata;
  }

  //ZH.l('fdata'); ZH.p(fdata);
  var pdata = [];
  for (var i = 0; i < fdata.length; i++) {
    try {
      var d = JSON.parse(fdata[i]);
      pdata.push(d);
    } catch(err) {
      network_data[nkey] = fdata[i];
      break;
    }
  }

  //ZH.l('TLS RECEIVE: PARSE MANY'); ZH.p(pdata);
  for (var i = 0; i < pdata.length; i++) {
    var p = pdata[i];
    if (ZH.NetworkDebug === true) {
      var pr = (p.method !== 'GeoLeaderPing' && p.method !== 'GeoDataPing');
      if (ZH.DebugAllNetworkSerialization) pr = true;
      if (pr) {
        ZH.l('TLS: RECEIVED: ' + ZH.GetMsTime()); ZH.p(p);
      }
    }
    handler(s, p);
  }
}

var ServerNetworkData = {};

function handle_tls_connection(s) {
  var raddr = s.remoteAddress;
  var rport = s.remotePort;
  var rkey  = raddr + '-' + rport;
  ZH.l('TLS SERVER: Client connected: IP: ' + raddr + ' P: ' + rport);

  s.on('data', function(data) {
    on_data_handle(s, data, ServerNetworkData, rkey, handle_client_response);
  });

  s.on('error', function(err) {
    ZH.l('TLS SERVER: ERROR: ' + err.message);
  });

  //s.on('end',     function() { ZH.l('TLS SERVER: END'); });
  //s.on('timeout', function() { ZH.l('TLS SERVER: TIMEOUT'); });
  //s.on('drain',   function() { ZH.l('TLS SERVER: DRAIN'); });

  s.on("error", function (err) {
    ZH.l('TLS SERVER: ERROR: ' + err.toString());
    s.destroy();
  });

  s.on("close", function () {
    ZH.l('TLS SERVER: CLOSE: ' + raddr + ' P: ' + rport);
  });
}

exports.CreateTlsServer = function(cnode, handler, next) {
  exports.dc       = cnode.datacenter;
  exports.hostname = cnode.backend.server.hostname;
  exports.port     = cnode.backend.server.port;
  ZH.l('ZTLS.CreateTlsServer: H: ' + exports.hostname + ' P: ' + exports.port);
  exports.Handler  = handler;
  exports.options  = {key  : fs.readFileSync(ZH.AgentConfig.ssl_key),
                      cert : fs.readFileSync(ZH.AgentConfig.ssl_cert),
                      rejectUnauthorized: false
                     };
  exports.tserver  = tls.createServer(exports.options, handle_tls_connection);
  exports.tserver.listen(exports.port, function(lerr, lres) {
    if (lerr) next(lerr, null);
    else {
      ZH.l('TLS LISTENING ON: ' + exports.port);
      next(null, null);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// TLS CLIENT ----------------------------------------------------------------

var CallBackMap = {};
function handle_server_response(s, message) { // NOTE: 's' is not used
  var id   = message.id;
  var next = CallBackMap[id];
  if (next) next(message);
}

var SocketID = 0;
function nextSocketID() {
  SocketID += 1;
  return SocketID;
}

var ClientRkey = 0;
function get_unique_client_rkey() {
  ClientRkey += 1;
  return ClientRkey;
}

var ClientNetworkData = {};

var TlsClient = function (cnode, is_geo, leader, generic, gnfunc, timeout) {
  var me        = this;
  me.id         = nextSocketID();
  me.cnode      = cnode;
  me.duuid      = cnode.device_uuid;

  me.is_geo     = is_geo;
  me.leader     = leader;
  me.generic    = generic;
  me.gnfunc     = gnfunc;
  me.timeout    = timeout;
  me.options    = {key  : fs.readFileSync(ZH.AgentConfig.ssl_key),
                   cert : fs.readFileSync(ZH.AgentConfig.ssl_cert),
                   rejectUnauthorized: false
                  };
  me.cerrs      = 0;
  me.connected  = false;
  me.s          = null;
  me.rto        = null;

  function get_full_title() {
    var summary = me.duuid + ' ID: ' + me.id + ' H: ' + me.hostname +
                  ' P: ' + me.port;
    var title;
    if (me.is_geo) {
      if (me.leader)        title = 'TLS GEO-LEADER-CLIENT: GU: ';
      else                  title = 'TLS GEO-CLIENT: GU: ';
    } else if (me.generic)  title = 'CLIENT: U: '
    else                    title = 'TLS CLUSTER-CLIENT: U: ';
    return title + summary;
  }

  // NOTE: cluster_leader can change
  this.get_current_node_address = function() {
    if (me.generic) {
      me.hostname = me.cnode.backend.server.hostname;
      me.port     = me.cnode.backend.server.port;
    } else {
      var ccnode = me.gnfunc(me.duuid);
      if (ccnode && me.is_geo && me.cerrs >= MaxGeoConnErrors) {
        me.hostname = ccnode.backend.load_balancer.hostname;
        me.port     = ccnode.backend.load_balancer.port;
      } else {
        if (me.leader && me.cnode.cluster_leader) {
          me.hostname = me.cnode.cluster_leader.backend.server.hostname;
          me.port     = me.cnode.cluster_leader.backend.server.port;
        } else {
          me.hostname = me.cnode.backend.server.hostname;
          me.port     = me.cnode.backend.server.port;
        }
      }
    }
  }

  this.get_current_node_address();
  ZH.l('INIT: ' + get_full_title());

  this.client_handle_response = function(data) {
    on_data_handle(null, data,
                   ClientNetworkData, me.rkey, handle_server_response);
  };

  this.send = function(jrpc_body) {
    if (me.s && me.s.writable) {
      var cbody = ZH.NetworkSerialize(jrpc_body);
      me.s.write(cbody + "\n");
      return true;
    } else {
      return false;
    }
  }

  this.request = function(jrpc_body, next) {
    if (me.s && me.s.writable) {
      var id    = jrpc_body.id;
      if (next !== ZH.OnErrLog && next !== ZH.NoOp) {
        CallBackMap[id] = next;
      }
      var cbody = ZH.NetworkSerialize(jrpc_body);
      me.s.write(cbody + "\n");
    } else {
      var data = {error : ZS.Errors.TlsClientNotReady};
      next(data);
      me.reconnect(ZH.OnErrLog);
    }
  }

  this.connect = function(next) {
    ZH.l('CONNECT->: ' + get_full_title());
    me.connected = false;
    me.rkey      = get_unique_client_rkey();
    me.s = tls.connect(me.port, me.hostname, me.options, function () {
      ZH.l('->CONNECTED: ' + get_full_title());
      me.connected = true;
      me.cerrs     = 0;
      next(null, me);
    });

    me.s.on("data", me.client_handle_response);

    // TODO ChaosMode that does not destroy() on error
    me.s.on("error", function (err) {
      ZH.l('CLIENT ERROR: (' + err + ') ' + get_full_title());
      if (me.s) me.s.destroy();
    });

    function handle_socket_close() {
      ZH.l('TLS CLIENT CLOSE: ' + get_full_title());
      me.connected = false;
      if (me.generic) {
        me.reconnect(next);
      } else if (me.is_geo) { // GeoNodes are FOREVER
        me.reconnect(next);
      } else {         // ClusterNodes can disappear
        if (((me.duuid === ZH.MyUUID) || me.gnfunc(me.duuid))) {
          me.reconnect(next);
        } else {
          ZH.l('CLUSTER-NODE: NOT in cluster: ' + get_full_title());
          me.kill(next);
        }
      }
    }
    me.s.on("close", handle_socket_close);
  }

  this.reconnect = function(next) {
    ZH.l('CLIENT RECONNECT: connected: ' + me.connected + ' RTO: ' + me.rto);
    if (me.connected) return;
    if (me.rto) return;
    me.cerrs += 1;
    me.get_current_node_address();
    if (me.generic) {
      ZH.l('CLIENT: RECONNECT: cerrs: ' + me.cerrs + ': ' + get_full_title());
    } else if (!me.is_geo) { // CLUSTER
      ZH.l('CLUSTER: RECONNECT: cerrs: ' + me.cerrs + ': ' + get_full_title());
    } else { // IS_GEO
      ZH.l('GEO: RECONNECT: cerrs: ' + me.cerrs + ': ' + get_full_title());
      if (me.cerrs >= MaxGeoConnErrors) {
        var ngnode  = me.gnfunc(me.duuid);
        if (ngnode) {
          me.hostname = ngnode.backend.load_balancer.hostname;
          me.port     = ngnode.backend.load_balancer.port;
          ZH.l('GEO: RESET TO LOAD-BALANCER GEO-IP: ' + get_full_title());
        } else {
          ZH.l('ERROR: GEO: RESET TO LOAD-BALANCER FAILED');
        }
      }
    }

    var to = me.timeout;
    ZH.l('CLIENT: SLEEP: ' + to + ' ' + get_full_title());
    me.rto = setTimeout(function() {
      me.rto = null;
      ZH.l('CLIENT: DO RECONNECT: ' + get_full_title());
      me.connect(next)
    }, to);
  }

  this.destroy = function(next) {
    ZH.l('DESTROY: ' + get_full_title());
    if (me.s) {
      me.s.on("data",  function() {});
      me.s.on("error", function() {});
      me.s.on("close", function() {});
      me.s.on("error", function() {});
      me.s.destroy();
    }
    me.s = null;
    next(null, null);
  }

  this.kill = function(next) {
    ZH.l('KILL: ' + get_full_title());
    me.request                = ZH.NoOp;
    me.client_handle_response = ZH.NoOp;
    me.connect                = ZH.NoOp;
    me.reconnect              = ZH.NoOp;
    me.destroy(next);
  }
};

exports.CreateTlsClient = function(cnode, is_geo, leader, generic,
                                   gnfunc, timeout, next) {
  var cli = new TlsClient(cnode, is_geo, leader, generic, gnfunc, timeout);
  cli.connect(next);
  return cli;
}

