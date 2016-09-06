"use strict";

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REQUIRES ------------------------------------------------------------------

var https   = require('https');
var fs      = require('fs');

var ZS      = require('./zshared');
var ZH      = require('./zhelper');

// Agent connects to Central via WSS
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"; // HACK for self signed certs


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var WatchdogAuth = {username : 'watchdog', password: 'password'};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARGV[] --------------------------------------------------------------------

this.config_file = process.argv[2];
if (typeof(this.config_file)  === 'undefined') Usage();

try {
  var data = fs.readFileSync(this.config_file, 'utf8');
  var json = JSON.parse(data);
} catch (e) {
  console.error(e);
  process.exit(-1);
}

if (!json.hostname) ConfigFileError();
if (!json.port)     ConfigFileError();

this.hostname = json.hostname;
this.port     = json.port;

if (typeof(this.hostname)    === 'undefined') ConfigFileError();
if (typeof(this.port)        === 'undefined') ConfigFileError();


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function Usage(err) {
  if (typeof(err) !== 'undefined') console.error('Error: ' + err);
  console.error('Usage: ' + process.argv[0] + ' ' + process.argv[1] +
                ' config_file');
  process.exit(-1);
}
function ConfigFileError(err) {
  if (typeof(err) !== 'undefined') console.error('Error: ' + err);
  console.error('CONFIG-FILE must contain: hostname, port ');
  process.exit(-1);
}

function watchdog_error_respond(err, hres) {
  var etxt = err.message ? err.message : err;
  ZH.e('watchdog_error_respond: ' + etxt);
  respond_error(hres.response, -32007, etxt, hres.id);
}

function worker_basic_auth(auth, next) {
  var ok = (auth.username === WatchdogAuth.username &&
            auth.password === WatchdogAuth.password);
  next(null, ok);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLERS ------------------------------------------------------------------

function handle_start_cluster_node(response, params, id) {
  var hres  = {response: response, params : params, id : id};
  var enext = watchdog_error_respond;
  var auth  = params.authentication;
  var guuid = params.data.datacenter;
  if (!params.data.cluster_node || !params.data.cluster_node.id) {
    enext(new Error(ZS.Errors.ClusterIDRequired), hres);
  }
  var cuuid = params.data.cluster_node.id;
  ZH.l('<-|(X): StartClusterNode: CU: ' + cuuid);

  worker_basic_auth(auth, function(aerr, ok) {
    if      (aerr) enext(aerr, hres);
    else if (!ok)  enext(new Error(Errors.BasicAuthFail), hres);
    else {
      var ctlfile = '/tmp/ZYNC_ROUTER_CTL_' + cuuid;
      try {
        fs.unlinkSync(ctlfile);
      } catch (e) {
        var err = 'fs.unlinkSync: ' + ctlfile + ' ERROR: ' + e;
        enext(err, hres);
        return;
      }
      var data = {datacenter : guuid,
                  device     : {uuid : cuuid},
                  alive      : true};
      respond(hres.response, data, hres.id);
    }
  });
}

function handle_central_is_sync(response, params, id) {
  var hres  = {response: response, params : params, id : id};
  var enext = watchdog_error_respond;
  var auth  = params.authentication;
  var guuid = params.data.datacenter;
  if (!params.data.cluster_node || !params.data.cluster_node.id) {
    enext(new Error(ZS.Errors.ClusterIDRequired), hres);
  }
  var cuuid = params.data.cluster_node.id;
  ZH.l('<-|(X): CentralIsSync: CU: ' + cuuid);

  worker_basic_auth(auth, function(aerr, ok) {
    if      (aerr) enext(aerr, hres);
    else if (!ok)  enext(new Error(Errors.BasicAuthFail), hres);
    else {
      try {
        var pidfile = '/tmp/ZYNC_ROUTER_PID_' + cuuid;
        var fdata   = fs.readFileSync(pidfile, 'utf8');
        if (!fdata) {
          var err = 'ERROR: EMPTY: pidfile: ' + pidfile;
          enext(err, hres);
          return;
        } else {
          var pid = Number(fdata);
          ZH.l('COMMAND: kill -s SIGWINCH ' + pid);
          process.kill(pid, 'SIGWINCH');
        }
      } catch(e) {
        var err = 'ERROR: handle_central_is_sync: ' + e;
        enext(err, hres);
        return;
      }
      var data = {datacenter : guuid,
                  device     : {uuid : cuuid},
                  synced     : true};
      respond(hres.response, data, hres.id);
    }
  });
}

function handle_kill_cluster_node(response, params, id) {
  var hres  = {response: response, params : params, id : id};
  var enext = watchdog_error_respond;
  var auth  = params.authentication;
  var guuid = params.data.datacenter;
  if (!params.data.cluster_node || !params.data.cluster_node.id) {
    enext(new Error(ZS.Errors.ClusterIDRequired), hres);
  }
  var cuuid = params.data.cluster_node.id;
  ZH.l('<-|(X): KillClusterNode: CU: ' + cuuid);

  worker_basic_auth(auth, function(aerr, ok) {
    if      (aerr) enext(aerr, hres);
    else if (!ok)  enext(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      try {
        var pidfile = '/tmp/ZYNC_ROUTER_PID_' + cuuid;
        var fdata   = fs.readFileSync(pidfile, 'utf8');
        if (!fdata) {
          var err = 'ERROR: EMPTY: pidfile: ' + pidfile;
          enext(err, hres);
          return;
        } else {
          var ctlfile = '/tmp/ZYNC_ROUTER_CTL_' + cuuid;
          fs.writeFileSync(ctlfile, 'KILLED', 'utf8');
          var pid     = Number(fdata);
          ZH.l('COMMAND: kill -s SIGINT ' + pid);
          process.kill(pid, 'SIGINT');
        }
      } catch(e) {
        var err = 'ERROR: handle_kill_cluster_node: ' + e;
        enext(err, hres);
        return;
      }
      var data = {datacenter : guuid,
                  device     : {uuid : cuuid},
                  alive      : false};
      respond(hres.response, data, hres.id);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// METHOD DECLARATIONS -------------------------------------------------------

var Methods = {'StartClusterNode' : {handler : handle_start_cluster_node},
               'CentralIsSync'    : {handler : handle_central_is_sync},
               'KillClusterNode'  : {handler : handle_kill_cluster_node},
              };


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HTTP JSON RPC 2.0 ---------------------------------------------------------

function respond_json(response, body) {
  response.writeHead(200, {'content-type'   : 'application/json',
                           'content-length' : body.length,
                           'Access-Control-Allow-Origin' : '*' });
  response.write(body, "utf8");
  response.end();
}

function respond_error(response, code, e, id) {
  if (typeof id === 'undefined') { id = 'null'; }
  var body  = { jsonrpc : "2.0", id : id, error : { code : code, message : e} };
  var cbody = ZH.NetworkSerialize(body);
  respond_json(response, cbody);
}

function respond(response, data, id) {
  var body  = { jsonrpc : "2.0", id : id, result : data };
  var cbody = ZH.NetworkSerialize(body);
  respond_json(response, cbody);
}

function process_request(request, response) {
  var id  = null;
  if (request.method !== 'POST') {
    return respond_error(response, -32000, ZS.Errors.MethodMustBePost, id);
  }

  var pdata = '';
  request.addListener('data', function(chunk) { pdata += chunk; });
  request.addListener('end', function() {
    var data;
    try { 
      data = JSON.parse(pdata);
    } catch(e) {
      return respond_error(response, -32700, 'Parse error: (' + e + ')', id);
    }
    if (ZH.NetworkDebug) {
      ZH.l('RECEIVED'); ZH.p(data);
    }
    id = data.id;
    var version = data.jsonrpc;
    if (version != '2.0') {
      return respond_error(response, -32002, ZS.Errors.WrongJsonRpcId, id);
    }
    var method  = data.method;
    if (!(method in Methods)) {
      return respond_error(response, -32601,
                           'Method (' + method + ') not found', id);
    }
    var params  = data.params;
    if (typeof(params) === 'undefined') {
      return respond_error(response, -32003,
                           ZS.Errors.JsonRpcParamsMissing, id);
    }
    var auth    = params.authentication;
    if (typeof(auth) === 'undefined') {
      return respond_error(response, -32009,
                          ZS.Errors.AuthenticationMissing, id);
    }

    var m = Methods[method];
    m.handler(response, params, id);
  });
}

process.on('SIGINT', function() {
  ZH.l("WATCHDOG: Caught signal: SIGINT");
  ZH.l("EXITING");
  process.exit();
});

// Open Agent HTTPS server
var server_opts     = {key  : fs.readFileSync(ZH.AgentConfig.ssl_key),
                       cert : fs.readFileSync(ZH.AgentConfig.ssl_cert)};
this.hserver        = https.createServer(server_opts, process_request);
this.hserver.listen(this.port, this.hostname);

ZH.l('ZAgent listening on https://' + this.hostname + ':' + this.port + '/');


