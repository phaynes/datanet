"use strict";

var fs              = require('fs');
var https           = require('https');
var WebSocket       = require('ws');
var WebSocketServer = require('ws').Server;

var ZDelt     = require('./zdeltas');
var ZS        = require('./zshared');
var ZH        = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

var FullDebugNotify = true;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

var WssConns  = {};
var WssStatus = {};

function handle_notify_wss_open(url, next) {
  ZH.l('NOTIFY: WSS: OPEN: URL: ' + url);
  WssStatus[url] = true;
  next(null, null);
}

// NOTE: this is called, but the WSS is still send()able
function handle_notify_wss_close(url) {
  ZH.l('NOTIFY: WSS: CLOSE: URL: ' + url);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA-CHANGE EVENT ---------------------------------------------------------

function send_notify(cbody, debug) {
  for (var url in ZS.EngineCallback.Notify) {
    if (WssStatus[url]) {
      if (debug) ZH.l('ZNotify.DATA-CHANGE: URL: ' + url);
      else       ZH.l('ZNotify.DEBUG: URL: ' + url);
      var wss = WssConns[url];
      wss.send(cbody, function(err) {
        if (err) delete(WssStatus[url]);
      });
    }
  }
}

// NOTE: NOTIFY REQUIRES ADMIN-AUTHENTICATION -> no permissions checking
exports.NotifyOnDataChange = function(plugin, collections,
                                      pc, full_doc, selfie) {
  var ks     = pc.ks;
  ZH.l('ZNotify.NotifyOnDataChange: K: ' + ks.kqk);
  var crdt   = pc.ncrdt;
  var remove = pc.remove;
  var pmd    = pc.post_merge_deltas;
  if (!crdt && !remove && !pmd) return; // Nothing happened -> NO-OP
  var id     = ZDelt.GetNextAgentRpcID(plugin, collections);
  var json   = crdt ? ZH.CreatePrettyJson(crdt) : null;
  var flags  = {selfie             : selfie,
                full_document_sync : full_doc,
                remove             : remove    ? true : false,
                initialize         : pc.store  ? true : false}
  var data   = {device            : {uuid : ZH.MyUUID},
                id                : id,
                kqk               : ks.kqk,
                json              : json,
                post_merge_deltas : pmd,
                flags             : flags};
  if (FullDebugNotify) {
    ZH.l('ZNotify.NotifyOnDataChange: DATA'); ZH.p(data);
  }
  var cbody = ZH.NetworkSerialize(data);
  send_notify(cbody, false);
}

exports.DebugNotify = function(ks, debug) {
  var data   = {kqk   : ks.kqk,
                debug : debug};
  var cbody = ZH.NetworkSerialize(data);
  send_notify(cbody, true);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT COMMAND HANDLERS ---------------------------------------------------

function create_connection(url, next) {
  ZH.l('create_connection: URL: ' + url);
  var wss;
  try {
    wss = new WebSocket(url);
  } catch(err) {
    ZH.e('create_connection: ERROR: ' + err.message);
    next(err, null);                               // NOTE: ERROR NEXT
    return;
  }
  wss.onopen  = handle_notify_wss_open(url, next); // NOTE: SUCCESS NEXT
  wss.onerror = function() {};
  wss.onclose = handle_notify_wss_close(url);
  WssConns[url] = wss;
}

function create_connections(aurls, next) {
  if (aurls.length === 0) next(null, null);
  else {
    var url = aurls.shift();
    create_connection(url, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(create_connections, aurls, next);
    });
  }
}

function kill_connection(url, next) {
  var wss = WssConns[url];
  if (wss) {
    wss.terminate();
    delete(WssConns[url]);
  }
  next(null, null);
}

function add_connection(url, next) {
  create_connection(url, function(cerr, cres) {
    if (cerr) next(cerr, null);
    else {
      if (!ZS.EngineCallback.Notify) ZS.EngineCallback.Notify = {};
      ZS.EngineCallback.Notify[url] = true;
      next(null, ZS.EngineCallback.Notify);
    }
  });
}

function remove_connection(url, next) {
  kill_connection(url, function(kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      if (ZS.EngineCallback.Notify) {
        delete(ZS.EngineCallback.Notify[url]);
      }
      next(null, ZS.EngineCallback.Notify);
    }
  });
}

function set_notify(net, cmd, url, next) {
  if (cmd !== 'ADD' && cmd !== 'REMOVE') {
    next(new Error(ZS.Errors.NotifyFormat), null);
  } else {
    var prot = url.substr(0, 6);
    prot     = prot.toUpperCase();
    if (prot != "WSS://") {
      next(new Error(ZS.Errors.NotifyURLNotWSS), null);
    } else {
      ZH.l('ZNotify.SetNotify: cmd: ' + cmd + ' url: ' + url);
      var nkey = ZS.NotifyURLs;
      var burl = ZH.B64Encode(url);
      if (cmd === "ADD") {
        net.plugin.do_set_field(net.collections.global_coll, nkey, burl, true,
        function(serr, sres) {
          if (serr) next(serr, null);
          else      add_connection(url, next);
        });
      } else { // REMOVE
        net.plugin.do_unset_field(net.collections.global_coll, nkey, burl,
        function(uerr, ures) {
          if (uerr) next(uerr, null);
          else      remove_connection(url, next);
        });
      }
    }
  }
}

exports.SetNotify = function(net, cmd, url, hres, next) {
  set_notify(net, cmd, url, function(serr, urls) {
    hres.urls = urls;
    next(serr, hres);
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT INITIALIZATION ------------------------------------------------------

exports.Initialize = function(plugin, collections, next) {
  var nkey = ZS.NotifyURLs;
  plugin.do_get(collections.global_coll, nkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var burls = gres[0];
        delete(burls._id);
        var urls  = {};
        for (var burl in burls) {
          var url   = ZH.B64Decode(burl);
          urls[url] = true;
        }
        ZS.EngineCallback.Notify = urls;
        ZH.e('ZNotify.InitConnections'); ZH.e(urls);
        var aurls = [];
        for (var url in urls) aurls.push(url);
        create_connections(aurls, next);
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SERVER PRIMITIVES ---------------------------------------------------------

function get_https_notify_server_url(ip, port) {
  return 'https://' +  ip + ':' + port + '/';
}

function get_wss_notify_server_url(ip, port) {
  return 'wss://'   +  ip + ':' + port + '/';
}

exports.GetNotifyServerURL = function(ish, ip, port) {
  if (ish) return get_https_notify_server_url(ip, port);
  else     return get_wss_notify_server_url  (ip, port);
}

exports.CreateNotifyServer = function(ish, ip, port, handler, next) {
  var hurl        = get_https_notify_server_url(ip, port);
  var wurl        = get_wss_notify_server_url  (ip, port);
  var surl        = ish ? hurl : wurl;
  var https_conf  = ZH.CentralConfig;
  var Server_opts = { key  : fs.readFileSync(https_conf.ssl_key),
                      cert : fs.readFileSync(https_conf.ssl_cert)};

  var Hserver     = https.createServer(Server_opts,
  function(request, response) {
    ZH.l('NOTIFY_SERVER: HTTPS REQUEST');
    var pdata = '';
    request.addListener('data', function(chunk) { pdata += chunk; });
    request.addListener('end', function() {
      var data = ZH.NetworkDeserialize(pdata);
      handler(hurl, data);
    });
  });
  Hserver.listen(port, ip);
  ZH.l('Listening on https://' + ip + ':' + port + '/');

  var Ewss = new WebSocketServer({server: Hserver});
  Ewss.on('listening', function() {
    ZH.l('WSS: listening: on: ' + wurl);
    next(null, surl);
  });
  Ewss.on('error', function(err) {
    ZH.l('WSS: error:  on: ' + wurl);
    next(err, null);
  });
  Ewss.on('connection', function(wsconn) {
    ZH.l('WSS: connection on: ' + wurl);
    wsconn.on('message', function (message) {
      ZH.l('WSS: Recieved message');
      var data = ZH.NetworkDeserialize(message);
      handler(wurl, data);
    });
    wsconn.on('error', function (err) {
      ZH.l('WSS: wsconn error on: ' + wurl);
      ZH.p(err);
    });
    wsconn.on('close', function () {
      ZH.l('WSS: wsconn close on: ' + wurl);
    });
  });
}


