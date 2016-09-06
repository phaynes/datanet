"use strict"

var https    = require('https');

var ZH       = require('../zhelper');

ZH.Debug        = true;
ZH.LogToConsole = true;
ZH.ZyncRole     = 'BFC';
ZH.MyUUID       = 'BFC';

var Cip      = process.argv[2];
var Cport    = process.argv[3];
var Username = process.argv[4];
var Password = process.argv[5];
var Ns       = process.argv[6];
var Cn       = process.argv[7];
var Key      = process.argv[8];
var Extra    = process.argv[9];

function get_rpc_id() {
  var now = ZH.GetMsTime();
  return "HTTPS_CLIENT_" + now;
}

function create_client_body(auth, ks) {
return {
         "jsonrpc" : "2.0",
         "id"      : get_rpc_id(),
         "params"  : {
           "data" : {
             "device"     : { "uuid" : -1 },
             "namespace"  : ks.ns,
             "collection" : ks.cn,
             "key"        : ks.key
           },
           "authentication" : {
             "username" : auth.username,
             "password" : auth.password
           }
         }
       };
}

function add_increment_to_oplog(oplog, field, byval) {
  if (!oplog) oplog = [];
  oplog.push({ "name" : "increment", "path" : field, "args" : [ byval ] });
  return oplog;
}

function create_client_fetch_body(auth, ks) {
  var data    = create_client_body(auth, ks);
  data.method = "ClientFetch";
  return data;
}

function create_client_remove_body(auth, ks) {
  var data    = create_client_body(auth, ks);
  data.method = "ClientRemove";
  return data;
}

function create_client_commit_body(auth, ks, crdt, oplog) {
  var data               = create_client_body(auth, ks);
  data.method            = "ClientCommit";
  data.params.data.crdt  = crdt;
  data.params.data.oplog = oplog;
  return data;
}

function create_client_stateless_commit_body(auth, ks, rchans, field, byval) {
  var data               = create_client_body(auth, ks);
  data.method            = "ClientStatelessCommit";
  data.params.data.replication_channels = rchans;
  var oplog              = add_increment_to_oplog(null, field, byval);
  data.params.data.oplog = oplog;
  return data;
}

function send_central_https_request(ip, port, data, next) {
  var method = "ClientFetch";
  var post_data;
  try {
    post_data = JSON.stringify(data);
  } catch(e) {
    return next(e, null);
  }
  var post_options = {
    host               : ip,
    port               : port,
    path               : '/',
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
      var data = ZH.ParseJsonRpcCall(method, pdata);
      next(null, data);
    });
  });
  post_request.on('error', function(e) {
    ZH.e('ERROR: send_central_https_request: https.request(): ' + e);
    return next(e, null);
  });
  ZH.l('send_central_https_request: WRITE'); ZH.l(post_data);
  post_request.write(post_data);
  post_request.end();
}

function send_central_crdt_request(data, next) {
  send_central_https_request(Cip, Cport, data, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var crdt = sres.result.crdt;
      next(null, crdt);
    }
  });
}

function fetch_incr_commit(ks, auth, field, byval, next) {
  var fdata = create_client_fetch_body(auth, ks);
  send_central_crdt_request(fdata, function(serr, crdt) {
    if (serr) next(serr, null);
    else {
      var oplog = add_increment_to_oplog(null, field, byval);
      var cdata = create_client_commit_body(auth, ks, crdt, oplog);
      send_central_crdt_request(cdata, next);
    }
  });
}

var next = ZH.OnErrLog;
var auth = {username : Username, password : Password};
var ks   = ZH.CompositeQueueKey(Ns, Cn, Key);

var rchans = [1];
var field  = "z";
var byval  = 3;

if (Extra === "REMOVE") {
  var rdata = create_client_remove_body(auth, ks);
  send_central_https_request(Cip, Cport, rdata, function(serr, sres) {
    ZH.e(sres);
  });
} else if (Extra === "STATELESS-COMMIT") {
  var fdata = create_client_stateless_commit_body(auth, ks,
                                                  rchans, field, byval);
  send_central_crdt_request(fdata, function(serr, sres) {
    ZH.e(sres);
  });
} else {
  fetch_incr_commit(ks, auth, field, byval, function(serr, sres) {
    ZH.e(sres);
  });
}

