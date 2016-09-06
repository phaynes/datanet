"use strict";

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REQUIRES ------------------------------------------------------------------


var https    = require('https');
var fs       = require('fs');

var ZDelt    = require('./zdeltas');
var ZAS      = require('./zactivesync');
var ZISL     = require('./zisolated');
var ZADaem   = require('./zagent_daemon');
var ZDack    = require('./zdack');
var ZCache   = require('./zcache');
var ZChannel = require('./zchannel');
var ZSU      = require('./zstationuser');
var ZUM      = require('./zuser_management');
var ZHB      = require('./zheartbeat');
var ZGCReap  = require('./zgc_reaper');
var ZLat     = require('./zlatency');
var ZAio     = require('./zaio');
var ZWss     = require('./zwss');
var ZAuth    = require('./zauth');
var ZDS      = require('./zdatastore');
var ZFix     = require('./zfixlog');
var ZASC     = require('./zapp_server_cluster');
var ZMCC     = require('./zmemcache_server_cluster');
var ZMCG     = require('./zmemcache_get');
var ZMessage = require('./zmessage');
var ZNotify  = require('./znotify');
var ZQ       = require('./zqueue');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');
ZH.ZyncRole  = 'AGENT';
var ZDBP     = require('./zdb_plugin');
ZH.AmCentral = false;
require('./zsignal_handlers');

// Agent connects to Central via WSS
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"; // HACK for self signed certs


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARGV[] --------------------------------------------------------------------

this.config_file = process.argv[2];
if (typeof(this.config_file)  === 'undefined') Usage();

var json;
try {
  var data = fs.readFileSync(this.config_file, 'utf8');
  json     = JSON.parse(data);
} catch (e) {
  console.error(e);
  process.exit(-1);
}

if (!json.database) ConfigFileError();
var dbname = json.database.name;
if (dbname !== 'MONGO'  && dbname !== 'REDIS' &&
    dbname !== 'MEMORY' && dbname !== 'MEMCACHE') {
  ConfigFileError("plugin.name [MONGO, REDIS, MEMORY, MEMCACHE]");
}
if      (dbname === "MEMORY") ZDBP.SetPlugin('MEMORY');
else if (dbname === "REDIS")  ZDBP.SetPlugin('REDIS');
else if (dbname === "MONGO")  ZDBP.SetPlugin('MONGODB');
else                          ZDBP.SetPlugin('MEMCACHE');

if (dbname !== "MEMORY") {
  ZDBP.plugin.ip   = json.database.ip;
  ZDBP.plugin.port = json.database.port;
}

if (!json.device)  ConfigFileError();
if (!json.agent)   ConfigFileError();
if (!json.central) ConfigFileError();

this.duuid_file  = json.device.uuid_file;
this.hostname    = json.agent.hostname;
this.port        = json.agent.port;
this.datacenter  = json.central.datacenter;
this.dc_hostname = json.central.hostname;
this.dc_port     = json.central.port;

if (typeof(this.duuid_file)  === 'undefined') ConfigFileError();
if (typeof(this.hostname)    === 'undefined') ConfigFileError();
if (typeof(this.port)        === 'undefined') ConfigFileError();
if (typeof(this.datacenter)  === 'undefined') ConfigFileError();
if (typeof(this.dc_hostname) === 'undefined') ConfigFileError();
if (typeof(this.dc_port)     === 'undefined') ConfigFileError();

if (dbname !== "MEMORY") {
  if (typeof(ZDBP.plugin.ip) === 'undefined') ConfigFileError();
}

exports.CacheConfig = null;
if (json.cache) {
  if (!json.cache.max_bytes) ConfigCacheError();
  exports.CacheConfig = json.cache;
}

exports.AppServerClusterConfig = null;
if (json.app_server_cluster) {
  var asc = json.app_server_cluster;
  if (!asc.name           || !asc.database     || !asc.dataqueue ||
      !asc.backend        || !asc.server       ||
      !asc.database.name  || !asc.database.ip  || !asc.database.port ||
      !asc.dataqueue.name || !asc.dataqueue.ip || !asc.dataqueue.port ||
      !asc.backend.server    ||
      !asc.backend.server.ip || !asc.backend.server.port ||
      !asc.server.protocol ) {
    ConfigAppServerClusterError();
  }
  if (asc.server.protocol !== "HTTPS") ConfigAppServerClusterError();
  if (asc.server.protocol === "HTTPS" && 
      (!asc.server.port || !asc.server.path)) {
    ConfigAppServerClusterError();
  }
  exports.AppServerClusterConfig = asc;
}

exports.MemcacheClusterConfig = null;
exports.MemcacheAgentConfig   = null;
if (json.memcache_cluster) {
  var mcc = json.memcache_cluster;
  if (!mcc.name           || !mcc.database     || !mcc.backend       ||
      !mcc.app_server_cluster_name || !json.agent.request_ip         ||
      !mcc.database.name  || !mcc.database.ip  || !mcc.database.port ||
      !mcc.backend.server    ||
      !mcc.backend.server.ip || !mcc.backend.server.port) {
    ConfigMemcacheClusterError();
  }
  exports.MemcacheClusterConfig = mcc;
  exports.MemcacheAgentConfig   = json.agent;
}

// READ IN DEVICE.UUID from DUUID_FILE
this.device_uuid = -1;
try {
  var text         = fs.readFileSync(this.duuid_file, 'utf8');
  this.device_uuid = Number(text);
  if (!ZH.IsJsonElementInt(this.device_uuid)) this.device_uuid == -1;
} catch (e) {
  // NOTE: do not exit, non-existent duuid_file happens on cherry start
}

ZH.AmBrowser = false;
ZDBP.SetDeviceUUID(this.device_uuid);
ZDBP.SetDataCenter(this.datacenter);

if (json.log) {
  if (json.log.console) {
    ZH.LogToConsole = true;
    ZH.l('LogToConsole: TRUE');
  }
}

exports.Logfile = (this.device_uuid === -1) ? null :
                                      '/tmp/LOG_ZYNC_AGENT_' + this.device_uuid;

function init_pid_file(auuid) {
  var pid     = process.pid;
  var PidFile = '/tmp/ZYNC_AGENT_PID_' + auuid;
  ZH.e('init_pid_file: U: ' + auuid + ' P: ' + pid);
  fs.writeFileSync(PidFile, pid, 'utf8');
}

if (this.device_uuid !== -1) init_pid_file(this.device_uuid);

//ZH.SetLogLevel("ERROR");

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

ZH.NetworkDebug = true;
//ZH.Debug = false;

var CatchMethodErrors = true;
CatchMethodErrors = false;

var DebugIntentionalInitializationDeviceKeyError = false;


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
  console.error('CONFIG-FILE must contain: ' +
                ' device.uuid_file, agent.{hostname,port} ' +
                ' central.{datacenter,hostname,port}, database{name,ip,port}');
  process.exit(-1);
}

function ConfigAppServerClusterError(err) {
  if (typeof(err) !== 'undefined') console.error('Error: ' + err);
  console.error('"app_server_cluster" section must contain:' +
                ' name, database.[name,ip,port] dataqueue.[name,ip,port]' + 
                ' backend[ip.port] server[protocol,path,port]');
  console.error('additionally "agent" section must contain: request_ip');
  process.exit(-1);
}

function ConfigMemcacheClusterError(err) {
  if (typeof(err) !== 'undefined') console.error('Error: ' + err);
  console.error('"memcache_cluster" section must contain:' +
                ' name, app_server_cluster_name, ' +
                ' database.[name,ip,port] backend[ip.port]');
  process.exit(-1);
}

function ConfigCacheError(err) {
  if (typeof(err) !== 'undefined') console.error('Error: ' + err);
  console.error('"cache" section must contain: policy max_bytes');
  process.exit(-1);
}

function on_error_respond(err, method, hres) {
  if (err) {
    ZH.t('on_error_respond: M: ' + method);
    var etxt = err.message ? err.message : err;
    respond_error(hres.response, -32007, etxt, hres.id);
    return true;
  }
  return false;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FROM-DEVICE HANDLER CALLBACKS ---------------------------------------------

function respond_with_new_crdt(err, hres) {
  if (on_error_respond(err, 'ClientRequestZDoc', hres)) return;
  ZH.t('>>>>(X): respond_with_new_crdt: id: ' + hres.id);
  var data = {crdt   : hres.applied.crdt,
              status : "OK"};
  respond(hres.response, data, hres.id);
}

function station_user_on_agent_processed(err, hres) {
  ZH.t('>>>>(X): station_user_on_agent_processed');
  if (on_error_respond(err, 'ClientStationUser', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function destation_processed(err, hres) {
  ZH.t('>>>>(X): destation_processed');
  if (on_error_respond(err, 'ClientDestationUser', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function central_subscribe_processed(err, hres) {
  if (on_error_respond(err, 'AgentSubscribe', hres)) return;
  ZH.t('|->(X): central_subscribe_processed: id: ' + hres.id);
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function fetch_processed(err, hres) {
  if (on_error_respond(err, 'ClientFetch', hres)) return;
  ZH.t('>>>>(X): respond_with_new_crdt: id: ' + hres.id);
  var data = {datacenter : hres.datacenter,
              connected  : hres.connected,
              crdt       : hres.applied.crdt,
              status     : "OK"};
  respond(hres.response, data, hres.id);
}

function find_processed(err, hres) {
  if (on_error_respond(err, 'ClientFind', hres)) return;
  ZH.t('>>>>(X): find_processed: id: ' + hres.id);
  var data = {jsons : hres.jsons, status : "OK"};
  respond(hres.response, data, hres.id);
}

//TODO reply w/ JSONs
function scan_processed(err, hres) {
  if (on_error_respond(err, 'ClientScan', hres)) return;
  ZH.t('>>>>(X): scan_processed: id: ' + hres.id);
  var data = {crdts : hres.crdts, status : "OK"};
  respond(hres.response, data, hres.id);
}

function send_central_remove_processed(err, hres) {
  ZH.t('>>>>(X): send_central_remove_processed');
  if (on_error_respond(err, 'ClientRemove', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function handle_client_cache_processed(err, hres) {
  ZH.t('|->(X): central_cache_processed: id: ' + hres.id);
  if (on_error_respond(err, 'ClientCache', hres)) return;
  var data = {watch  : hres.watch,
              crdt   : hres.applied.crdt,
              status : "OK"};
  respond(hres.response, data, hres.id);
}

function evict_processed(err, hres) {
  if (on_error_respond(err, 'ClientEvict', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function local_evict_processed(err, hres) {
  if (on_error_respond(err, 'ClientLocalEvict', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function expire_processed(err, hres) {
  if (on_error_respond(err, 'ClientExpire', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function isolation_processed(err, hres) {
  ZH.t('>>>>(X): isolation_processed');
  if (on_error_respond(err, 'ClientIsolation', hres)) return;
  var data = {isolation : hres.isolation,
              status    : "OK"};
  respond(hres.response, data, hres.id);
}

function notify_processed(err, hres) {
  ZH.t('>>>>(X): notify_processed');
  if (on_error_respond(err, 'ClientNotify', hres)) return;
  var data = {urls      : hres.urls,
              status    : "OK"};
  respond(hres.response, data, hres.id);
}

function switch_user_processed(err, hres) {
  ZH.t('>>>>(X): switch_user_processed');
  if (on_error_respond(err, 'ClientSwitchUser', hres)) return;
  var data = {user : hres.user, status : "OK"};
  respond(hres.response, data, hres.id);
}

function get_susers_processed(err, hres) {
  ZH.t('>>>>(X): get_susers_processed');
  if (on_error_respond(err, 'ClientGetStationedUsers', hres)) return;
  var data = {users : hres.users, status : "OK"};
  respond(hres.response, data, hres.id);
}

function get_asubs_processed(err, hres) {
  ZH.t('>>>>(X): get_asubs_processed');
  if (on_error_respond(err, 'ClientGetAgentSubscriptions', hres)) return;
  var data = {subscriptions : hres.subscriptions, status : "OK"};
  respond(hres.response, data, hres.id);
}

function get_usubs_processed(err, hres) {
  ZH.t('>>>>(X): get_usubs_processed');
  if (on_error_respond(err, 'ClientGetUserSubscriptions', hres)) return;
  var data = {subscriptions : hres.subscriptions, status : "OK"};
  respond(hres.response, data, hres.id);
}

function get_info_processed(err, hres) {
  ZH.t('>>>>(X): get_info_processed');
  if (on_error_respond(err, 'ClientGet*Info', hres)) return;
  var data = {information : hres.information, status : "OK"};
  respond(hres.response, data, hres.id);
}

function get_latencies_processed(err, hres) {
  ZH.t('>>>>(X): get_latencies_processed');
  if (on_error_respond(err, 'ClientGetLatencies', hres)) return;
  var data = {information : hres.information, status : "OK"};
  respond(hres.response, data, hres.id);
}

function get_mcc_state_processed(err, hres) {
  ZH.t('>>>>(X): get_mcc_state_processed');
  if (on_error_respond(err, 'ClientMemcacheClusterState', hres)) return;
  var data = {state : hres.mstate, status : "OK"};
  respond(hres.response, data, hres.id);
}

function is_cached_processed(err, hres) {
  ZH.t('>>>>(X): is_cached_processed');
  if (on_error_respond(err, 'ClientIsCached', hres)) return;
  var data = {cached : hres.cached, status : "OK"};
  respond(hres.response, data, hres.id);
}

function heartbeat_processed(err, hres) {
  ZH.t('>>>>(X): heartbeat_processed');
  if (on_error_respond(err, 'Heartbeat', hres)) return;
  var data = {increment : hres.Increment,
              timestamp : hres.Timestamp,
              array     : hres.Array,
              status    : "OK"};
  respond(hres.response, data, hres.id);
}

function get_cinfo_processed(err, hres) {
  ZH.t('>>>>(X): get_cinfo_processed');
  if (on_error_respond(err, 'ClientGetClusterInfo', hres)) return;
  var data = {information : hres.information, status : "OK"};
  respond(hres.response, data, hres.id);
}

function message_processed(err, hres) {
  ZH.t('>>>>(X): message_processed');
  if (on_error_respond(err, 'ClientMessage', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function request_processed(err, hres) {
  ZH.t('>>>>(X): request_processed');
  if (on_error_respond(err, 'ClientRequest', hres)) return;
  var data = {r_data : hres.r_data, status : "OK"};
  respond(hres.response, data, hres.id);
}

function add_user_processed(err, hres) {
  ZH.t('>>>>(X): add_user_processed');
  if (on_error_respond(err, 'ClientAddUser', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function remove_user_processed(err, hres) {
  ZH.t('>>>>(X): remove_user_processed');
  if (on_error_respond(err, 'ClientRemoveUser', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function grant_user_processed(err, hres) {
  ZH.t('>>>>(X): grant_user_processed');
  if (on_error_respond(err, 'ClientGrantUser', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function remove_dc_processed(err, hres) {
  ZH.t('>>>>(X): remove_dc_processed');
  if (on_error_respond(err, 'ClientRemoveDataCenter', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

function get_agent_keys_processed(err, hres) {
  ZH.t('>>>>(X): get_agent_keys_processed');
  if (on_error_respond(err, 'ClientGetAgentKeys', hres)) return;
  var data = {kss    : hres.kss,
              status : "OK"};
  respond(hres.response, data, hres.id);
}

function shutdown_processed(err, hres) {
  ZH.t('>>>>(X): shutdown_processed');
  if (on_error_respond(err, 'ClientShutdown', hres)) return;
  var data = {status : "OK"};
  respond(hres.response, data, hres.id);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FROM-DEVICE HANDLERS ------------------------------------------------------

function handle_client_station_user(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  ZH.t('<-|(X): ClientStationUser: UN: ' + auth.username);
  var next  = station_user_on_agent_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZSU.AgentStationUser(net, auth, hres, next);
    }
  });
}

function handle_client_destation_user(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  ZH.t('<-|(X): ClientDestationUser: UN: ' + auth.username);
  var next  = destation_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZSU.AgentDestationUser(net.plugin, net.collections, auth, hres, next);
    }
  });
}

function handle_client_subscribe(response, params, id, net) {
  var hres    = {response: response, params : params, id : id};
  var auth    = params.authentication;
  var schanid = ZH.IfNumberConvert(params.data.channel.id);
  ZH.t('<-|(X): ClientSubscribe: R: ' + schanid);
  var next    = central_subscribe_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  ZAuth.HasAgentSubscribePermissions(net, auth, schanid, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.SubscribePermsFail), hres);
    else {
      ZChannel.AgentSubscribe(net.plugin, net.collections, schanid, auth,
                              hres, next);
    }
  });
}

function handle_client_unsubscribe(response, params, id, net) {
  var hres    = {response: response, params : params, id : id};
  var auth    = params.authentication;
  var schanid = ZH.IfNumberConvert(params.data.channel.id);
  ZH.t('<-|(X): ClientUnsubscribe: R: ' + schanid);
  var next    = central_subscribe_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZChannel.AgentUnsubscribe(net.plugin, net.collections, schanid, auth,
                                hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA I/O METHODS-----------------------------------------------------------

function handle_client_fetch(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var ns    = params.data.namespace;
  var cn    = params.data.collection;
  var key   = params.data.key;
  ZH.t('<-|(X): ClientFetch: key: ' + key);
  var next  = fetch_processed;
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.HasReadPermissions(net, auth, ks, ZH.MyUUID, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.NoDataFound), hres);
    else {
      ZDelt.AgentFetch(net, ks, hres, next);
    }
  });
}

function handle_client_find(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var ns    = params.data.namespace;
  var cn    = params.data.collection;
  var query = params.data.query;
  ZH.t('<-|(X): ClientFind');
  var next  = find_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZDS.FindPermsData(net, ns, cn, query, auth, hres, next);
    }
  });
}

function do_agent_store(net, ks, sep, ex, json, rchans, auth, hres, next) {
  ZAuth.HasAgentStorePermissions(net, auth, ks, rchans, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), hres);
    else {
      ZDelt.AgentStore(net, ks, sep, ex, json, auth, hres, next);
    }
  });
}

function handle_client_store(response, params, id, net) {
  var hres   = {response: response, params : params, id : id};
  var auth   = params.authentication;
  var ns     = params.data.namespace;
  var cn     = params.data.collection;
  var key    = params.data.key;
  var sep    = params.data.separate;
  var ex     = params.data.expiration;
  var json   = params.data.json;
  var ismc   = params.data.is_memcache;
  ZH.t('<-|(X): ClientStore: key: ' + key);
  var rchans = json._channels;
  var next   = respond_with_new_crdt;
  var ks     = ZH.CompositeQueueKey(ns, cn, key);
  if (ismc) {
    ZMCG.AgentCheckMemcacheKeyIsLocal(net, ks, function(perr, pres) {
      if (perr) next(perr, hres);
      else {
        do_agent_store(net, ks, sep, ex, json, rchans, auth, hres, next);
      }
    });
  } else {
    do_agent_store(net, ks, sep, ex, json, rchans, auth, hres, next);
  }
}

function handle_client_commit(response, params, id, net) {
  var hres    = {response: response, params : params, id : id};
  var auth    = params.authentication;
  var ns      = params.data.namespace;
  var cn      = params.data.collection;
  var key     = params.data.key;
  var crdt    = params.data.crdt;
  var oplog   = params.data.oplog;
  ZH.t('<-|(X): ClientCommit: key: ' + key);
  var rchans  = crdt._meta.replication_channels;
  var next    = respond_with_new_crdt;
  var ks      = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
  function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), hres);
    else {
      var ex = 0;
      ZDelt.AgentCommit(net, ks, crdt, oplog, ex, auth, hres, next);
    }
  });
}

function handle_client_pull(response, params, id, net) {
  var hres    = {response: response, params : params, id : id};
  var auth    = params.authentication;
  var ns      = params.data.namespace;
  var cn      = params.data.collection;
  var key     = params.data.key;
  var crdt    = params.data.crdt;
  var oplog   = params.data.oplog;
  ZH.t('<-|(X): ClientPull: key: ' + key);
  var rchans  = crdt._meta.replication_channels;
  var next    = respond_with_new_crdt;
  var ks      = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
  function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), hres);
    else {
      ZDelt.AgentPull(net, ks, crdt, oplog, auth, hres, next);
    }
  });
}

function handle_client_remove(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var ns    = params.data.namespace;
  var cn    = params.data.collection;
  var key   = params.data.key;
  ZH.t('<-|(X): ClientRemove key: ' + key);
  var next  = send_central_remove_processed;
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.HasWritePermissionsOnKey(net, auth, ks, ZH.MyUUID, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), hres);
    else {
      ZDelt.AgentRemove(net, ks, auth, hres, next);
    }
  });
}

function handle_client_stateless_commit(response, params, id, net) {
  var hres   = {response: response, params : params, id : id};
  var auth   = params.authentication;
  var ns     = params.data.namespace;
  var cn     = params.data.collection;
  var key    = params.data.key;
  var oplog  = params.data.oplog;
  var rchans = params.data.replication_channels;
  ZH.t('<-|(X): ClientStatelessCommit: key: ' + key);
  var next   = respond_with_new_crdt;
  var ks     = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
  function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), hres);
    else {
      ZDelt.AgentStatelessCommit(net, ks, oplog, rchans, auth, hres, next);
    }
  });
}

function handle_client_memcache_commit(response, params, id, net) {
  var hres   = {response: response, params : params, id : id};
  var auth   = params.authentication;
  var ns     = params.data.namespace;
  var cn     = params.data.collection;
  var key    = params.data.key;
  var oplog  = params.data.oplog;
  var rchans = params.data.replication_channels;
  var sep    = params.data.separate;
  var ex     = params.data.expiration;
  ZH.t('<-|(X): ClientMemcacheCommit: key: ' + key);
  var next   = respond_with_new_crdt;
  var ks     = ZH.CompositeQueueKey(ns, cn, key);
  ZMCG.AgentCheckMemcacheKeyIsLocal(net, ks, function(perr, pres) {
    if (perr) next(perr, hres);
    else {
      ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
        if      (aerr) next(aerr, hres);
        else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), hres);
        else {
          ZDelt.AgentMemcacheCommit(net, ks, oplog, rchans, sep, ex,
                                    auth, hres, next);
        }
      });
    }
  });
}

function handle_client_scan(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var ns    = params.data.namespace;
  var cn    = params.data.collection;
  ZH.t('<-|(X): ClientScan');
  var next  = scan_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZDS.AgentScanReadPermsData(net.plugin, net.collections, auth, hres, next);
    }
  });
}

// NOTE: ZH.Agent.cconn/ZH.Isolation check done in ZAio.SendCentralCache()
function handle_client_cache(response, params, id, net) {
  var hres   = {response: response, params : params, id : id};
  var auth   = params.authentication;
  var ns     = params.data.namespace;
  var cn     = params.data.collection;
  var key    = params.data.key;
  var pin    = params.data.pin;
  var watch  = params.data.watch;
  var sticky = params.data.sticky;
  ZH.t('<-|(X): ClientCache: key: ' + key + ' P: ' + pin + ' W: ' + watch + 
       ' S: ' + sticky);
  var next  = handle_client_cache_processed;
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      var force    = false;
      var internal = false;
      ZCache.AgentCache(net, ks, pin, watch, sticky, force, internal,
                        auth, hres, next);
    }
  });
}

function handle_client_evict(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var ns    = params.data.namespace;
  var cn    = params.data.collection;
  var key   = params.data.key;
  ZH.t('<-|(X): ClientEvict: key: ' + key);
  var next  = evict_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else { // NOTE: no 'auth' argument for ZCache.AgentEvict()
      ZCache.AgentEvict(net, ks, false, hres, next);
    }
  });
}

function handle_client_local_evict(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var ns    = params.data.namespace;
  var cn    = params.data.collection;
  var key   = params.data.key;
  ZH.t('<-|(X): ClientLocalEvict: key: ' + key);
  var next  = local_evict_processed;
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZCache.AgentLocalEvict(net, ks, false, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXPIRE --------------------------------------------------------------------

function handle_client_expire(response, params, id, net) {
  var hres   = {response: response, params : params, id : id};
  var auth   = params.authentication;
  var ns     = params.data.namespace;
  var cn     = params.data.collection;
  var key    = params.data.key;
  var expire = Number(params.data.expire);
  ZH.t('<-|(X): ClientExpire: key: ' + key + ' E: ' + expire);
  var next  = expire_processed;
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZDelt.AgentExpire(net, ks, expire, auth, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ISOLATION -----------------------------------------------------------------

function handle_client_isolation(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var val   = params.data.value;
  ZH.t('<-|(X): ClientIsolation: value: ' + val);
  var next  = isolation_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZISL.SendCentralAgentIsolation(net, val, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NOTIFY --------------------------------------------------------------------

function handle_client_notify(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var cmd   = params.data.command;
  var url   = params.data.url;
  ZH.t('<-|(X): ClientNotify: cmd: ' + cmd + ' url: ' + url);
  var next  = notify_processed;
  ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.AdminAuthFail), hres);
    else {
      ZNotify.SetNotify(net, cmd, url, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INFO METHODS --------------------------------------------------------------

function handle_client_switch_user(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  ZH.t('<-|(X): ClientSwitchUser: UN: ' + auth.username);
  var next  = switch_user_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      hres.user = auth.username; // Used in response
      next(null, hres);
    }
  });
}

function handle_client_get_susers(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  ZH.t('<-|(X): ClientGetStationedUsers');
  var next  = get_susers_processed;
  ZSU.AgentGetStationedUsers(net.plugin, net.collections, hres, next);
}

function handle_client_get_asubs(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  ZH.t('<-|(X): ClientGetAgentSubscriptions: UN: ' + auth.username);
  var next  = get_asubs_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZChannel.AgentGetAgentSubscriptions(net.plugin, net.collections,
                                          hres, next);
    }
  });
}

function handle_client_get_usubs(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  ZH.t('<-|(X): ClientGetUserSubscriptions: UN: ' + auth.username);
  var next  = get_usubs_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZChannel.AgentGetUserSubscriptions(net.plugin, net.collections, auth,
                                         hres, next);
    }
  });
}

function handle_client_get_uinfo(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  ZH.t('<-|(X): ClientGetUserInfo: UN: ' + auth.username);
  var next  = get_info_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZChannel.AgentGetUserInfo(net.plugin, net.collections, auth, hres, next);
    }
  });
}

function handle_client_get_cinfo(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  ZH.t('<-|(X): ClientGetClusterInfo: UN: ' + auth.username);
  var next  = get_info_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZISL.AgentGetClusterInfo(net.plugin, net.collections, hres, next);
    }
  });
}

function handle_client_get_latencies(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  ZH.t('<-|(X): ClientGetLatencies: UN: ' + auth.username);
  var next  = get_latencies_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZLat.AgentGetLatencies(net.plugin, net.collections, hres, next);
    }
  });
}

function handle_client_get_mcc_state(response, params, id, net) {
  var hres   = {response: response, params : params, id : id};
  var auth   = params.authentication;
  var clname = params.data.cluster_name;
  ZH.t('<-|(X): ClientGetMemcacheClusterState: CL: ' + clname);
  var next  = get_mcc_state_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZMCC.AgentGetMemcacheClusterState(net, clname, hres, next);
    }
  });
}

function handle_client_is_cached(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  ZH.t('<-|(X): ClientIsCached: UN: ' + auth.username);
  var next  = is_cached_processed;
  var ks    = params.data.ks;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZCache.AgentIsCached(net.plugin, net.collections, ks, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HEARTBEAT------------------------------------------------------------------

function handle_client_heartbeat(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var cmd   = params.data.command;
  var field = params.data.field;    // Either INCR or
  var uuid  = params.data.uuid;     // Ordered-List
  var mlen  = Number(params.data.max_size);
  var trim  = Number(params.data.trim);
  var isi   = params.data.is_increment;
  var isa   = params.data.is_array;
  ZH.t('<-|(X): ClientHeartbeat: UN: ' + auth.username);
  var next  = heartbeat_processed;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZHB.AgentDataHeartbeat(net, cmd, field, uuid, mlen, trim, isi, isa,
                             auth, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MESSAGE METHODS -----------------------------------------------------------

function handle_client_message(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var mtxt  = params.data.text;
  ZH.t('<-|(X): ClientMessage');
  var next  = message_processed;
  ZMessage.HandleClientMessage(net, mtxt, auth, hres, next);
}

function handle_client_request(response, params, id, net) {
  var hres  = {response: response, params : params, id : id};
  var auth  = params.authentication;
  var mtxt  = params.data.text;
  ZH.t('<-|(X): ClientRequest');
  var next  = request_processed;
  ZMessage.HandleClientRequest(net, mtxt, auth, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN METHODS -------------------------------------------------------------

function handle_client_add_user(response, params, id, net) {
  var hres     = {response: response, params : params, id : id};
  var auth     = params.authentication;
  var username = params.data.username;
  var password = params.data.password;
  var role     = params.data.role;
  ZH.t('<-|(X): ClientAddUser: UN: ' + username);
  var next     = add_user_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZUM.AgentAddUser(net.plugin, net.collections, username, password, role,
                       auth, hres, next);
    }
  });
}

function handle_client_remove_user(response, params, id, net) {
  var hres     = {response: response, params : params, id : id};
  var auth     = params.authentication;
  var username = params.data.username;
  ZH.t('<-|(X): ClientRemoveUser: UN: ' + username);
  var next     = remove_user_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZUM.AgentRemoveUser(net.plugin, net.collections, username, auth,
                          hres, next);
    }
  });
}

function handle_client_grant_user(response, params, id, net) {
  var hres     = {response: response, params : params, id : id};
  var auth     = params.authentication;
  var username = params.data.username;
  var schanid  = ZH.IfNumberConvert(params.data.channel.id);
  var priv     = params.data.privilege;
  ZH.t('<-|(X): ClientGrantUser: UN: ' + username);
  var next     = grant_user_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZUM.AgentGrantUser(net.plugin, net.collections, username, schanid, priv,
                         auth, hres, next);
    }
  });
}

function handle_client_remove_dc(response, params, id, net) {
  var hres   = {response: response, params : params, id : id};
  var auth   = params.authentication;
  var dcuuid = params.data.remove_datacenter;
  ZH.t('<-|(X): ClientRemoveDataCenter: DC: ' + dcuuid);
  var next   = remove_dc_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZAio.SendCentralRemoveDataCenter(net.plugin, net.collections,
                                       dcuuid, auth,
      function(serr, sres) {
        next(serr, hres);
      });
    }
  });
}

function handle_client_get_agent_keys(response, params, id, net) {
  var hres   = {response: response, params : params, id : id};
  var auth   = params.authentication;
  var duuid  = Number(params.data.agent.uuid);
  var nkeys  = Number(params.data.num_keys);
  var minage = Number(params.data.minimum_age);
  var wonly  = params.data.watch_only;
  ZH.t('<-|(X): ClientGetAgentKeys: U: ' + duuid);
  var next  = get_agent_keys_processed;
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), hres);
    return;
  }
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZAio.SendCentralAgentGetAgentKeys(net.plugin, net.collections,
                                        duuid, nkeys, minage, wonly, auth,
      function(derr, dres) {
        hres.kss = dres.kss;
        next(derr, hres);
      });
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SHUTDOWN ------------------------------------------------------------------

exports.DoShutdown = function() {
  ZH.Agent.net.plugin.do_shutdown(ZH.Agent.net.collections);
  ZH.e("EXITING");
  ZH.CloseLogFile();
  process.exit();
}

function handle_client_shutdown(response, params, id, net) {
  var hres = {response: response, params : params, id : id};
  var auth = params.authentication;
  ZH.t('<-|(X): ClientShutdown');
  var next = shutdown_processed;
  ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.AdminAuthFail), hres);
    else {
      next(null, hres);
      setTimeout(function() { exports.DoShutdown(); }, 1000);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// METHOD DECLARATIONS -------------------------------------------------------

var Methods = {'ClientStationUser'           :
                 {handler : handle_client_station_user,    stationed : false},
               'ClientDestationUser'         :
                 {handler : handle_client_destation_user,  stationed : true},
               'ClientSubscribe'             :
                 {handler : handle_client_subscribe,       stationed : true},
               'ClientUnsubscribe'           :
                 {handler : handle_client_unsubscribe,     stationed : true},

               'ClientStore'                 :
                 {handler : handle_client_store,           stationed : true},
               'ClientCommit'                :
                 {handler : handle_client_commit,          stationed : true},
               'ClientPull'                  :
                 {handler : handle_client_pull,            stationed : true},
               'ClientStatelessCommit'       :
                 {handler : handle_client_stateless_commit, stationed : true},
               'ClientMemcacheCommit'        :
                 {handler : handle_client_memcache_commit, stationed : true},
               'ClientFetch'                 :
                 {handler : handle_client_fetch,           stationed : true},
               'ClientCache'                 :
                 {handler : handle_client_cache,           stationed : true},
               'ClientEvict'                 :
                 {handler : handle_client_evict,           stationed : true},
               'ClientLocalEvict'            :
                 {handler : handle_client_local_evict,     stationed : true},
               'ClientRemove'                :
                 {handler : handle_client_remove,          stationed : true},
               'ClientScan'                  :
                 {handler : handle_client_scan,            stationed : true},
               'ClientExpire'                :
                 {handler : handle_client_expire,          stationed : true},

               'ClientIsolation'             :
                 {handler : handle_client_isolation,       stationed : false},
               'ClientNotify'                :
                 {handler : handle_client_notify,          stationed : false},

               'ClientSwitchUser'            :
                 {handler : handle_client_switch_user,     stationed : false},
               'ClientGetStationedUsers'     :
                 {handler : handle_client_get_susers,      stationed : true},
               'ClientGetAgentSubscriptions' :
                 {handler : handle_client_get_asubs,       stationed : true},

               'ClientIsCached'              :
                 {handler : handle_client_is_cached,       stationed : true},

               'ClientFind'                  :
                 {handler : handle_client_find,            stationed : false},
               'ClientHeartbeat'             :
                 {handler : handle_client_heartbeat,       stationed : false},

               'ClientGetUserSubscriptions'  :
                 {handler : handle_client_get_usubs,       stationed : false},
               'ClientGetUserInfo'           :
                 {handler : handle_client_get_uinfo,       stationed : false},
               'ClientGetClusterInfo'        :
                 {handler : handle_client_get_cinfo,       stationed : false},
               'ClientGetLatencies'          :
                 {handler : handle_client_get_latencies,   stationed : false},
               'ClientGetMemcacheClusterState' :
                 {handler : handle_client_get_mcc_state,   stationed : false},
               'ClientMessage'               :
                 {handler : handle_client_message,         stationed : false},
               'ClientRequest'               :
                 {handler : handle_client_request,         stationed : false},

               'ClientAddUser'               :
                 {handler : handle_client_add_user,        stationed : false},
               'ClientRemoveUser'            :
                 {handler : handle_client_remove_user,     stationed : false},
               'ClientGrantUser'             :
                 {handler : handle_client_grant_user,      stationed : false},
               'ClientRemoveDataCenter'      :
                 {handler : handle_client_remove_dc,       stationed : false},
               'ClientGetAgentKeys'          :
                 {handler : handle_client_get_agent_keys,  stationed : false},

               'ClientShutdown'              :
                 {handler : handle_client_shutdown,        stationed : false},
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
  if (Agent.dconn === false) {
    return respond_error(response, -32005, ZS.Errors.AgentConnectingToDB, id);
  }
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
      ZH.t('AGENT: RECEIVED: ' + ZH.GetMsTime()); ZH.t(data);
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

    if (ZH.FixLogActive) {
      ZWss.KillWss(function(werr, wres) {
        if (werr) ZH.e(werr);
        return respond_error(response, -32012, ZS.Errors.FixLogRunning, id);
      });
    }

    // CLONE collections  -> parallel events wont reset collections
    var net = ZH.CreateNetPerRequest(Agent);
    var m   = Methods[method];
    try {
      if (!m.stationed) {
        m.handler(response, params, id, net);
      } else { // ALL other methods require USER to be STATIONED on AGENT
        ZSU.AgentIsUserStationed(net.plugin, net.collections, auth,
        function(ierr, ok) {
          if      (ierr) respond_error(response, -32010, ierr.message, id);
          else if (!ok) {
           respond_error(response, -32011, ZS.Errors.NotStationed, id);
          } else {
            m.handler(response, params, id, net);
          }
        });
      }
    } catch(e) {
      if (CatchMethodErrors) {
        ZH.e('AGENT METHOD ERROR: name: ' + e.name + ' message: ' + e.message);
      } else {
        ZH.CloseLogFile(function() {
          throw(e);
        });
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZE AGENT ----------------------------------------------------------

function persist_agent_info(next) {
  ZH.l('persist_agent_info: U: ' + ZH.MyUUID);
  try {
    fs.writeFileSync(Agent.duuid_file, ZH.MyUUID, 'utf8');
  } catch (e) {
    ZH.e(e);
  }
  init_pid_file(ZH.MyUUID);
  ZDBP.PersistDeviceInfo(next);
}

function init_agent_data(next) {
  ZH.l('init_agent_data');
  var net  = ZH.Agent.net;
  net.plugin.do_populate_data(net, Agent.device_uuid);
  var dkey = ZS.AgentDataCenters;
  net.plugin.do_get_field(net.collections.global_coll, dkey, "geo_nodes",
  function(gerr, geo_nodes) {
    if (gerr) next(gerr, null);
    else {
      if (geo_nodes) {
        ZISL.AgentGeoNodes = ZH.clone(geo_nodes);
        ZH.l('init_agent_data: ZISL.AgentGeoNodes'); ZH.l(ZISL.AgentGeoNodes);
      }
      ZDack.GetAgentAllCreated(net.plugin, net.collections,
      function(gerr, gres) {
        if (gerr) next(gerr, null);
        else {
          if (gres.length !== 0) {
            var createds = gres[0];
            delete(createds._id);
            ZH.l('init_agent_data: CREATEDS'); ZH.l(createds);
            ZH.AgentLastCentralCreated = createds;
          }
          var kkey = ZS.GetAgentDeviceKey();
          net.plugin.do_get_field(net.collections.global_coll, kkey, "value",
          function(gerr, dkey) {
            if (gerr) next(gerr, null);
            else {
              if (dkey) ZH.DeviceKey = dkey;
              if (DebugIntentionalInitializationDeviceKeyError) {
                ZH.DeviceKey = "INTENTIONAL INITIALIZATION ERROR";
              }
              if (ZH.DeviceKey) {
                ZH.e('init_agent_data: SET DEVICE-KEY: ' + ZH.DeviceKey);
              }
              ZNotify.Initialize(net.plugin, net.collections, next);
            }
          });
        }
      });
    }
  });
}

ZH.InitNewDevice = function(duuid, next) {
  ZH.l('init_new_agent_device: duuid: ' + duuid);
  exports.Logfile = '/tmp/LOG_ZYNC_AGENT_' + duuid;
  persist_agent_info(function(perr, pres) {
    if (perr) next(perr, null);
    else {
      ZDBP.AdminConnect(false, function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          var net = ZH.Agent.net;
          ZASC.StartAppServerClusterDaemon(net);
          ZMCC.StartAppMemcacheClusterDaemon(net, json);
          init_agent_data(function(ierr, ires) {
            if (ierr) next(ierr, null);
            else      ZFix.Init(next);
          });
        }
      });
    }
  });
}

function init_agent_daemons() {
  ZADaem.StartupAgentToSyncKeys();
  ZADaem.StartAgentDirtyDeltasDaemon();
  ZGCReap.StartAgentGCPurgeDaemon();
  if (ZH.MyUUID !== -1) {
    ZASC.StartAppServerClusterDaemon(Agent.net);
    ZMCC.StartAppMemcacheClusterDaemon(Agent.net, json);
  }
}

function init_agent_https_server() {
  var server_opts = {key  : fs.readFileSync(ZH.AgentConfig.ssl_key),
                     cert : fs.readFileSync(ZH.AgentConfig.ssl_cert)};
  Agent.hserver   = https.createServer(server_opts, process_request);
  var port        = Agent.port;
  var hostname    = Agent.hostname;
  Agent.hserver.listen(port, hostname, function(serr, sres) {
    if (serr) throw(serr);
    else {
      ZH.l('ZAgent listening on https://' + hostname + ':' + port + '/');
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CONFIG --------------------------------------------------------------------

var InitNamespace = 'datastore';


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MAIN() --------------------------------------------------------------------

var Agent    = this;  // Global reference
Agent.synced = false;
ZWss.InitAgent(Agent);

ZH.CentralMaster.wss                 = {server : {}};
ZH.CentralMaster.wss.server.hostname = this.dc_hostname;
ZH.CentralMaster.wss.server.port     = this.dc_port;
ZH.CentralMaster.device_uuid         = this.datacenter
ZH.MyDataCenter                      = ZH.CentralMaster.device_uuid;

// Connect to Agent Database
ZDBP.PluginConnect(InitNamespace, function(cerr, zhndl) {
  if (cerr) throw cerr;
  else {
    Agent.net.zhndl       = zhndl;
    Agent.net.plugin      = zhndl.plugin;
    Agent.net.db_handle   = zhndl.db_handle;
    Agent.net.collections = zhndl.collections;
    ZDBP.AdminConnect(true, function(aerr, aconn) {
      if (aerr) throw(aerr);
      else {
        Agent.dconn = true;
        // NOTE: BELOW is ASYNC
        if (aconn !== null) init_agent_data(ZH.OnErrThrow)
        ZFix.Init(function(uerr, ures) {
          if (uerr) throw(uerr);
          else {
            if (!ZH.FixLogActive) ZWss.OpenAgentWss(ZH.OnErrThrow);
            init_agent_daemons();
            init_agent_https_server();
          }
        });

      }
    });
  }
});


