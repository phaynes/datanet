
var T = require('./ztrace'); // TRACE (before strict)
"use strict";

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CONFIG --------------------------------------------------------------------

var WhitelistedFileTypes = {
  './static/WebClient.html'        : {type : 'text/html'},
  './static/zync_all.js'           : {type : 'application/json'},
  './static/zync_helper.js'        : {type : 'application/json'},
  './static/d3.v3.min.js'          : {type : 'application/json'},
  './static/GlobalKeyMonitor.html' : {type : 'text/html'},
  './static/Sudoku.html'           : {type : 'text/html'},
  './static/Todo.html'             : {type : 'text/html'},
  './static/Tweets.html'           : {type : 'text/html'},
};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REQUIRES ------------------------------------------------------------------

var fs              = require('fs');
var Net             = require('net');
var url             = require('url');
var https           = require('https');
var WebSocketServer = require('ws').Server;

var ZCloud   = require('./zcloud_server');
var ZMCV     = require('./zmethods_cluster_vote');
var ZPub     = require('./zpublisher');
var ZRAD     = require('./zremote_apply_delta');
var ZNM      = require('./zneedmerge');
var ZCSub    = require('./zcluster_subscriber');
var ZGack    = require('./zgack');
var ZGDD     = require('./zgeo_dirty_deltas');
var ZED      = require('./zexpire_reaper');
var ZDConn   = require('./zdconn');
var ZPio     = require('./zpio');
var ZMessage = require('./zmessage');
var ZDirect  = require('./zdirect');
var ZCache   = require('./zcache');
var ZChannel = require('./zchannel');
var ZSU      = require('./zstationuser');
var ZUM      = require('./zuser_management');
var ZGCReap  = require('./zgc_reaper');
var ZAuth    = require('./zauth');
var ZCLS     = require('./zcluster');
var ZVote    = require('./zvote');
var ZPart    = require('./zpartition');
var ZFix     = require('./zfixlog');
var ZASC     = require('./zapp_server_cluster');
var ZMCC     = require('./zmemcache_server_cluster');
var ZDQ      = require('./zdata_queue');
var ZQ       = require('./zqueue');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');

ZH.ZyncRole  = 'CENTRAL';
var ZDBP     = require('./zdb_plugin');
ZH.AmCentral = true;
require('./zsignal_handlers');

init_settings();

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARGV[] --------------------------------------------------------------------

this.config_file = process.argv[2];
if (typeof(this.config_file)  === 'undefined') Usage();
ZH.l('Config file: ' + this.config_file);

var cdata;
try {
  var text = fs.readFileSync(this.config_file, 'utf8');
  cdata    = JSON.parse(text);
} catch (e) {
  console.error('ERROR PARSING CONFIG FILE');
  console.error(e);
  process.exit(-1);
}

if (!cdata.role) ConfigFileError("role");
this.role = cdata.role;
if (this.role === "BOTH") {
  ZH.AmRouter  = true;
  ZH.AmStorage = true;
  ZH.AmBoth    = true;
} else if (this.role === "ROUTER") {
  ZH.AmRouter  = true;
  ZH.AmStorage = false
  ZH.AmBoth    = false;
} else {
  ZH.AmRouter  = false;
  ZH.AmStorage = true;
  ZH.AmBoth    = false;
}
if (!cdata.uuid) ConfigFileError("uuid");
this.device_uuid = cdata.uuid;
if (typeof(this.device_uuid) !== 'number') ConfigFileError("uuid: NUMBER");

if (!cdata.datacenter) ConfigFileError("datacenter");
this.datacenter        = cdata.datacenter.name;
this.datacenter_synced = (cdata.datacenter.synced === true) ? true : false;
if (typeof(this.datacenter) === 'undefined') ConfigFileError("datacenter.name");

if (ZH.AmRouter) {
  if (!cdata.wss) ConfigFileError("wss");
  this.hostname = cdata.wss.server.hostname;
  this.ip       = cdata.wss.server.ip;
  this.port     = cdata.wss.server.port;
  if (cdata.wss.load_balancer) {
    this.wlb_hostname = cdata.wss.load_balancer.hostname;
    this.wlb_port     = cdata.wss.load_balancer.port;
  }
  if (typeof(this.hostname)     === 'undefined') {
    ConfigFileError("wss.server.hostname");
  }
  if (typeof(this.ip)           === 'undefined') {
    ConfigFileError("wss.server.ip");
  }
  if (typeof(this.port)         !== 'number')    {
    ConfigFileError("wss.server.port");
  }
  if (typeof(this.wlb_hostname) === 'undefined') {
    this.wlb_hostname = this.hostname;
  }
  if (typeof(this.wlb_port)     !== 'number')    {
    this.wlb_port     = this.port;
  }
  this.wss = {server : {
                hostname : this.hostname,
                port     : this.port
              },
              load_balancer : {
                hostname : this.wlb_hostname,
                port     : this.wlb_port
              }
             };

  if (cdata.socket) {
    this.socket_hostname = cdata.socket.server.hostname;
    this.socket_ip       = cdata.socket.server.ip;
    this.socket_port     = cdata.socket.server.port;
    if (cdata.socket.load_balancer) {
      this.slb_hostname = cdata.socket.load_balancer.hostname;
      this.slb_port     = cdata.socket.load_balancer.port;
    }
    if (typeof(this.socket_hostname) === 'undefined') {
      ConfigFileError("socket_hostname");
     }
    if (typeof(this.socket_ip)       === 'undefined') {
      ConfigFileError("socket_ip");
    }
    if (typeof(this.socket_port)     !== 'number') {
      ConfigFileError("socket_port");
    }
    if (typeof(this.slb_hostname)    === 'undefined') {
      this.slb_hostname = this.socket_hostname;
    }
    if (typeof(this.slb_port)        !== 'number') {
      this.slb_port     = this.socket_port;
    }
    this.socket = {server : {
                     hostname : this.socket_hostname,
                     port     : this.socket_port
                   },
                   load_balancer : {
                     hostname : this.slb_hostname,
                     port     : this.slb_port
                   }
                  };
  }
}

if (!cdata.backend) ConfigFileError("backend");
this.backend_hostname  = cdata.backend.server.hostname;
this.backend_ip        = cdata.backend.server.ip;
this.backend_port      = cdata.backend.server.port;
if (cdata.backend.load_balancer) {
  this.blb_hostname    = cdata.backend.load_balancer.hostname;
  this.blb_port        = cdata.backend.load_balancer.port;
}
if (typeof(this.backend_hostname) === 'undefined') {
  ConfigFileError("backend_hostname");
}
if (typeof(this.backend_ip)       === 'undefined') {
  ConfigFileError("backend_ip");
}
if (typeof(this.backend_port)     !== 'number') {
  ConfigFileError("backend_port");
}
if (ZH.AmRouter) {
  if (typeof(this.blb_hostname)   === 'undefined') {
    this.blb_hostname = this.backend_hostname;
  }
  if (typeof(this.blb_port)       !== 'number') {
    this.blb_port     = this.backend_port;
  }
}
this.backend = {server : {
                  hostname : this.backend_hostname,
                  port     : this.backend_port
                },
                load_balancer : {
                  hostname : this.blb_hostname,
                  port     : this.blb_port
                }
               };

if (!cdata.database) ConfigFileError("database");
var dbname = cdata.database.name;
if (dbname !== 'REDIS' && dbname !== 'MONGO' && dbname !== 'MEMORY') {
  ConfigFileError("plugin.name [REDIS, MONGO, MEMORY]");
}

if (ZH.AmRouter) {
  if (!cdata.discovery) ZH.CentralDisableDiscovery = true;
  else {
    this.d_hostname = cdata.discovery.hostname;
    this.d_port     = cdata.discovery.port;
    if ((typeof(this.d_hostname) === 'undefined') ||
       (typeof(this.d_port)     === 'undefined')) {
      ConfigFileError("cdata.discover.[hostname,port] missing");
    }
  }
}

if (!ZH.AmBoth) {
  if (!cdata.dataqueue) ConfigFileError("dataqueue");
  this.data_queue        = cdata.dataqueue;
  if (typeof(this.data_queue.name) === 'undefined') {
    ConfigFileError("data_queue.name");
  }
  if (typeof(this.data_queue.ip)   === 'undefined') {
    ConfigFileError("data_queue.ip");
  }
  if (typeof(this.data_queue.port) === 'undefined') {
    ConfigFileError("data_queue.port");
  }
  exports.DataQueue      = this.data_queue;
}

exports.AppServerClusterConfig = null;
if (cdata.app_server_cluster) {
  var asc = cdata.app_server_cluster;
  if (!asc.name           || !asc.dataqueue ||
      !asc.dataqueue.name || !asc.dataqueue.ip || !asc.dataqueue.port) {
    ConfigAppServerClusterError();
  }
  exports.AppServerClusterConfig = cdata.app_server_cluster;
}

// NOTE: InitializeAdminUserOnStartup is NOT a good idea in secure environments
//       it puts a very powerful admin password in a config-file
var InitializeAdminUserOnStartup = null;
if (cdata.admin) {
  if (cdata.admin.username && cdata.admin.password) {
    InitializeAdminUserOnStartup = cdata.admin;
  }
}

if (cdata.log) {
  if (cdata.log.console) {
    ZH.LogToConsole = true;
    ZH.l('LogToConsole: TRUE');
  }
}

exports.Logfile = '/tmp/LOG_ZYNC_' + this.role + '_' + this.device_uuid;
console.error('LOG-FILE: ' + exports.Logfile);

var pid         = process.pid;
exports.PidFile = '/tmp/ZYNC_' + this.role + '_PID_' + this.device_uuid;
fs.writeFileSync(exports.PidFile, pid, 'utf8');

if      (dbname === "REDIS") ZDBP.SetPlugin('REDIS');
else if (dbname === "MONGO") ZDBP.SetPlugin('MONGODB');
else                         ZDBP.SetPlugin('MEMORY');

ZDBP.plugin.ip         = cdata.database.ip;
ZDBP.plugin.port       = cdata.database.port;

if (dbname === 'REDIS' || dbname === 'MONGO') {
  if ((typeof(ZDBP.plugin.ip)   === 'undefined') ||
      (typeof(ZDBP.plugin.port) === 'undefined')) {
    ConfigFileError("REDIS & MONGO require config: database[ip,port]");
  }
}

ZH.MyDataCenter   = this.datacenter;

var MyClusterNode = {datacenter   : this.datacenter,
                     device_uuid  : this.device_uuid,
                     wss          : this.wss,
                     backend      : this.backend,
                     socket       : this.socket
                    };

ZH.MyGeoNode      = {device_uuid : this.datacenter,
                     wss         : this.wss,
                     backend     : this.backend,
                     socket      : this.socket
                    };

if (ZH.AmRouter) {
  ZH.Discovery = {hostname : this.d_hostname,
                  port     : this.d_port };
}

//ZH.SetLogLevel("ERROR");

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var Central = this;
ZH.Central  = Central;

function init_settings() {
  ZH.CentralDisableGeoNetworkPartitions     = true;
  ZH.CentralDisableCentralNetworkPartitions = true;

  ZH.CentralDisableCentralToSyncKeysDaemon  = false;
  ZH.CentralDisableCentralDirtyDeltasDaemon = false;
  ZH.CentralDisableCentralGCPurgeDaemon     = false;
  ZH.CentralDisableCentralExpireDaemon      = false;

  ZH.CentralDisableDiscovery                = false;

  ZH.CentralDisableRouterSprinkler          = false;

  ZH.CentralSubscriberEtherKSafetyTwo       = true;
  ZH.CentralExtendedTimestampsInDeltas      = false;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG SETTINGS ------------------------------------------------------------

ZH.NetworkDebug                 = true;
//ZH.Debug = false;

var ReplyToGeoLeaderPing        = false
var ReplyToGeoDataPing          = false

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var DiscoveryRedirectSleep     = 1000;
var ClusterLeaderRedirectSleep = 2000;


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
  if (err) ZH.e('Error: ' + err);
  if (ZH.AmRouter) {
    ZH.e('CONFIG-FILE must contain: [role, uuid, datacenter[name],' +
         ' database[name], data_queue.[name,ip,port], ' +
         ' wss[server[hostname,ip,port],load_balancer[hostname,port]], ' +
         ' socket[server[hostname,ip,port],load_balancer[hostname,port]], ' +
         ' backend[server[hostname,ip,port],load_balancer[hostname,port]]]');
  } else { // STORAGE
    ZH.e('CONFIG-FILE must contain: [role, uuid, datacenter[name],' +
         ' database[name], data_queue.[name,ip,port], ' +
         ' backend[server[hostname,ip,port]]]');
  }
  process.exit(-1);
}

function ConfigAppServerClusterError(err) {
  if (typeof(err) !== 'undefined') console.error('Error: ' + err);
  console.error('"app_server_cluster" section  must contain:' +
                ' name, dataqueue.[name,ip,port]');
  process.exit(-1);
}

function generic_error_response(hres) {
  var err = ZS.Errors.GenericError;
  ZCloud.HandleCallbackError(null, err, hres);
}

function agent_redirect(cnode, hres) {
  var data = {datacenter: ZH.MyDataCenter,
              device    : {
                cluster_node : {
                  wss    : cnode.wss,
                  socket : cnode.socket
                }
              },
              status    : "REDIRECT"};
  ZCloud.Respond(hres, data);
}

function geo_redirect(cnode, hres) {
  var data = {datacenter : ZH.MyDataCenter,
              device     : {cluster_node : cnode}, // FULL INFO
              status     : "REDIRECT"};
  ZCloud.Respond(hres, data);
}

function redirect_agent_master(is_geo, cnode, hres) {
  if (is_geo) geo_redirect  (cnode, hres);
  else        agent_redirect(cnode, hres);
}

function redirect_cluster_leader(hres) {
  ZH.t('redirect_cluster_leader: SLEEP: ' + ClusterLeaderRedirectSleep);
  setTimeout(function() {
    var cnode = ZCLS.GetClusterLeaderNode();
    if (!cnode) generic_error_response(hres);
    else        geo_redirect(cnode, hres);
  }, ClusterLeaderRedirectSleep);
}

function redirect_discovery(hres) {
  ZH.t('redirect_discovery: SLEEP: ' + DiscoveryRedirectSleep);
  setTimeout(function() {
    var cnode = ZCLS.GetDiscoveryNode();
    if (!cnode) generic_error_response(hres);
    else        geo_redirect(cnode, hres);
  }, DiscoveryRedirectSleep);
}

function redirect_other_datacenter(hres) {
  ZH.t('redirect_other_datacenter');
  var gnode = ZCLS.GetRandomGeoNode();
  if (!gnode) generic_error_response(hres);
  else        agent_redirect(gnode, hres);
}

function redirect_other_known_datacenter(hres) {
  ZH.t('redirect_other_known_datacenter');
  var gnode = ZCLS.GetRandomKnownGeoNode();
  if (!gnode) generic_error_response(hres);
  else        agent_redirect(gnode, hres);
}

function create_data_ok_response() {
  return {datacenter : ZH.MyDataCenter,
          device     : {uuid : ZH.MyUUID},
          status     : "OK"};
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-PING RESPONSES ------------------------------------------------------

function agent_ping_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentPing', err, hres)) return;
  ZH.t('>>>>(I): agent_ping_processed');
  var data = {device    : {uuid : ZH.MyUUID},
              responded : ZH.GetMsTime(),
              status    : "OK"};
  ZCloud.Respond(hres, data);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-ONLINE RESPONSES ----------------------------------------------------

function get_recheck_time() {
  if (ZH.TestNotReadyAgentRecheck) return 1000;
  else                             return (2 * ZDQ.RouterSprinklerSleep);
}

function agent_online_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentOnline', err, hres)) return;
  var udata = hres.user_data;
  var duuid = udata.device.uuid;
  var dkey  = udata.device.key;
  ZH.t('ZCloud.HandleCallbackError: U: ' + duuid + ' DK: ' + dkey);
  if (udata.offline) { // OFFLINE
    ZH.t('>>>>(A): agent_offline_processed: U: ' + duuid);
    var data = {device  : {uuid : duuid},
                offline : true,
                status  : "OK"};
    ZCloud.Respond(hres, data);
  } else {            // ONLINE
    var cnode = ZPart.GetClusterNode(duuid);
    if (!cnode) return ZCloud.SendBackoff(duuid, hres);
    var initd = udata.device.initialize_device;
    if (initd) {
      ZH.t('>>>>(A): agent_online_processed: INITIAL_UUID: ' + duuid);
    } else {
      ZH.t('>>>>(A): agent_online_processed: U: '            + duuid);
    }
    var ocreated = hres.original_created;
    var recheck  = ocreated ? get_recheck_time() : 0;
    var device = {uuid              : duuid,
                  key               : dkey,
                  initialize_device : initd,
                  cluster_node      : {wss    : cnode.wss,
                                       socket : cnode.socket}};
    var status = "REDIRECT"; // NOTE: Agents ignore REDUNDANT redirects
    var data   = {datacenter       : ZH.MyDataCenter,
                  device           : device,
                  geo_nodes        : ZCLS.SummarizeGeoNodesForAgent(),
                  pkss             : udata.pkss,
                  ckss             : udata.ckss,
                  rkss             : udata.rkss,
                  original_created : ocreated,
                  recheck          : recheck,
                  status           : status};
    ZCloud.Respond(hres, data);
  }
}

function agent_recheck_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentRecheck', err, hres)) return;
  var ocreated = hres.original_created;
  var udata    = hres.user_data;
  var nready   = udata.not_ready;
  var recheck  = nready ? get_recheck_time() : 0;
  ZH.t('>>>>(A): agent_recheck_processed');
  var data = {datacenter       : ZH.MyDataCenter,
              geo_nodes        : ZCLS.SummarizeGeoNodesForAgent(),
              pkss             : udata.pkss,
              //NOTE: ckss[] not needed for PING -> unbroken connection
              rkss             : udata.rkss,
              original_created : ocreated,
              recheck          : recheck,
              status           : "OK"};
  ZCloud.Respond(hres, data);
}

function get_agent_keys_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentGetAgentKeys', err, hres)) return;
  var udata = hres.user_data;
  ZH.t('>>>>(A): get_agent_keys_processed');
  var data  = {datacenter : ZH.MyDataCenter,
               kss        : udata.kss,
               status     : "OK"};
  ZCloud.Respond(hres, data);
}

function geo_datacenter_online_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoDataCenterOnline', err, hres)) return;
  var ddata = hres.dc_data;
  if (!ddata) {
    ZH.e('ERROR: geo_datacenter_online_processed -> NO DC_DATA'); ZH.e(hres);
    return;
  }
  ZH.t('>>>>(G): geo_datacenter_online_processed: GU: ' + ddata.guuid);
  var data = {kss             : ddata.kss,
              rkss            : ddata.rkss,
              users           : ddata.users,
              permissions     : ddata.permissions,
              subscriptions   : ddata.subscriptions,
              stationed_users : ddata.stationed_users,
              cached          : ddata.cached,
              device_keys     : ddata.device_keys,
              created         : ddata.created,
              status          : "OK"};
  ZCloud.Respond(hres, data);
}

function geo_broadcast_device_keys_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastDeviceKeys', err, hres)) return;
  ZH.t('>>>>(I): geo_broadcast_device_keys_processed');
  var data = {device : {uuid : ZH.MyUUID},
              status : "OK"};
  ZCloud.Respond(hres, data);
}

function cluster_status_processed(err, hres) {
  if (ZCloud.HandleCallbackError('BroadcastClusterStatus', err, hres)) return;
  ZH.t('>>>>(I): cluster_status_processed');
  var data = {device : {uuid : ZH.MyUUID},
              status : "OK"};
  ZCloud.Respond(hres, data);
}

function datacenter_discovery_processed(err, hres) {
  if (ZCloud.HandleCallbackError('DataCenterDiscovery', err, hres)) return;
  ZH.t('>>>>(G): datacenter_discovery_processed');
  var data = {device    : {uuid : ZH.MyUUID},
              geo_nodes : hres.geo_nodes,
              status    : "OK"};
  ZCloud.Respond(hres, data);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT CHANNEL BASED OP RESPONSES ------------------------------------------

function broadcast_subscribe_processed(err, hres) {
  if (ZCloud.HandleCallbackError('BroadcastSubscribe', err, hres)) return;
  ZH.t('>>>>(I): broadcast_subscribe_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function geo_subscribe_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastSubscribe', err, hres)) return;
  ZH.t('>>>>(G): geo_subscribe_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function agent_subscribe_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentSubscribe', err, hres)) return;
  ZH.t('>>>>(A): agent_subscribe_processed');
  var data = {permissions : hres.permissions,
              status      : "OK"};
  ZCloud.Respond(hres, data);
}

function agent_has_subscribe_permissions_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentHasSubscribePermissions', err, hres)) {
    return;
  }
  ZH.t('>>>>(A): agent_has_subscribe_permissions_processed');
  var data = {ok     : hres.ok,
              status : "OK"};
  ZCloud.Respond(hres, data);
}

function broadcast_unsubscribe_processed(err, hres) {
  if (ZCloud.HandleCallbackError('BroadcastUnsubscribe', err, hres)) return;
  ZH.t('>>>>(I): broadcast_unsubscribe_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function geo_unsubscribe_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastUnsubscribe', err, hres)) return;
  ZH.t('>>>>(G): geo_unsubscribe_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function agent_unsubscribe_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentUnsubscribe', err, hres)) return;
  ZH.t('>>>>(A): agent_unsubscribe_processed: U: ' + hres.duuid);
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STATION/DESTATION-USER RESPONSES ------------------------------------------

function geo_station_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastStationUser', err, hres)) return;
  ZH.t('>>>>(G): geo_station_user_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function agent_station_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentStationUser', err, hres)) return;
  ZH.t('>>>>(A): agent_station_user_processed');
  var data = {permissions   : hres.permissions,
              subscriptions : hres.subscriptions,
              pkss          : hres.pkss,
              status        : "OK"};
  ZCloud.Respond(hres, data);
}

function geo_destation_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastDestationUser', err, hres)) {
    return;
  }
  ZH.t('>>>>(G): geo_destation_user_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function agent_destation_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentDestationUser', err, hres)) return;
  ZH.t('>>>>(A): agent_destation_user_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT METHODS (USER BASED) -----------------------------------------------

function agent_authenticate_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentAuthenticate', err, hres)) return;
  ZH.t('>>>>(A): agent_authenticate_processed: UN: ' + hres.auth.username);
  var data = {authentication : {username : hres.auth.username},
              role           : hres.role,
              ok             : hres.ok,
              status         : "OK"};
  ZCloud.Respond(hres, data);
}

function agent_get_user_subscriptions_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentGetUserSubscriptions', err, hres)) {
    return;
  }
  ZH.t('>>>>(A): agent_get_user_subscriptions_processed');
  var data = {permissions   : hres.permissions,
              subscriptions : hres.subscriptions,
              status        : "OK"};
  ZCloud.Respond(hres, data);
}

function agent_get_user_info_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentGetUserInfo', err, hres)) {
    return;
  }
  ZH.t('>>>>(A): agent_get_user_info_processed');
  var data = {permissions   : hres.permissions,
              subscriptions : hres.subscriptions,
              devices       : hres.devices,
              status        : "OK"};
  ZCloud.Respond(hres, data);
}

function agent_get_user_channel_permissions_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentGetUserChannelPermissions', err, hres)) {
    return;
  }
  ZH.t('>>>>(A): agent_get_user_channel_permissions_processed');
  var data = {permissions : hres.permissions,
              status      : "OK"};
  ZCloud.Respond(hres, data);
}

function external_find_processed(err, hres) {
  if (ZCloud.HandleCallbackError('Find', err, hres)) return;
  ZH.t('>>>>(E): external_find_processed');
  var fdata = hres.f_data;
  var data = {jsons  : fdata.jsons,
              status : "OK"};
  ZCloud.Respond(hres, data);
}

function agent_get_cluster_info_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentGetClusterInfo', err, hres)) return;
  ZH.t('>>>>(A): agent_get_cluster_info_processed');
  var data = {datacenter          : ZH.MyDataCenter,
              device              : {uuid : ZH.MyUUID},
              cluster_synced      : hres.cluster_synced,
              geo_term_number     : hres.geo_term_number,
              geo_nodes           : hres.geo_nodes,
              cluster_term_number : hres.cluster_term_number,
              cluster_nodes       : hres.cluster_nodes,
              status              : "OK"};
  ZCloud.Respond(hres, data);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DELTA RESPONSES -----------------------------------------------------------

function agent_delta_processed(err, hres) {
  if (hres.conn.hsconn) { // ONLY HTTPS gets RESPONSE
    if (ZCloud.HandleCallbackError('AgentDelta', err, hres)) return;
    ZH.t('>>>>(A): agent_delta_processed');
    var data = create_data_ok_response();
    ZCloud.Respond(hres, data);
  }
}

function agent_dentries_processed(err, hres) {
  if (hres.conn.hsconn) { // ONLY HTTPS gets RESPONSE
    if (ZCloud.HandleCallbackError('AgentDentries', err, hres)) return;
    ZH.t('>>>>(A): agent_dentries_processed');
    var data = create_data_ok_response();
    ZCloud.Respond(hres, data);
  }
}

function geo_commit_delta_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoCommitDelta', err, hres)) return;
  ZH.t('>>>>(G): geo_commit_delta_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function agent_need_merge_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentNeedMerge', err, hres)) return;
  ZH.t('>>>>(A): agent_need_merge_processed');
  var mdata      = hres.merge_data;
  if (!mdata) {
    ZH.e('ERROR: agent_need_merge_processed -> NO MERGE_DATA'); ZH.e(hres);
    return;
  }
  if (hres.conn.hsconn) { // ONLY HTTPS gets RESPONSE
    var data = create_data_ok_response();
    ZCloud.Respond(hres, data);
  }
  var plugin      = ZH.Central.net.plugin;
  var collections = ZH.Central.net.collections;
  ZPio.SendSubscriberMergeData(plugin, collections, mdata.subscriber, mdata);
}

function geo_need_merge_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoNeedMerge', err, hres)) return;
  ZH.t('>>>>(A): geo_need_merge_processed');
  var mdata = hres.merge_data;
  if (!mdata) {
    ZH.e('ERROR: geo_need_merge_processed -> NO MERGE_DATA'); ZH.e(hres);
    return;
  }
  var data  = {merge_data : mdata, status : "OK"};
  ZCloud.Respond(hres, data);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CACHE/EVICT RESPONSES -----------------------------------------------------

function cluster_geo_cache_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterGeoCache', err, hres)) return;
  ZH.t('>>>>(I): cluster_geo_cache_processed: K: ' + hres.ks.kqk);
  var data = {ks : hres.ks, status : "OK"};
  ZCloud.Respond(hres, data);
}

function geo_cache_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastCache', err, hres)) return;
  ZH.t('>>>>(G): geo_cache_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function cluster_cache_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterCache', err, hres)) return;
  ZH.t('>>>>(I): cluster_cache_processed: K: ' + hres.ks.kqk);
  // NOTE: no response here, response is async via exports.ClusterCacheCallback
}

// NOTE: Used by ZCache.push_storage_cluster_cache()
exports.ClusterCacheCallback = function(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterCache', err, hres)) return;
  ZH.t('ZH.Central.ClusterCacheCallback: K: ' + hres.ks.kqk);
  var data = {ks         : hres.ks,
              watch      : hres.watch,
              merge_data : hres.merge_data,
              status     : "OK"};
  ZCloud.Respond(hres, data);
}

function agent_cache_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentCache', err, hres)) return;
  ZH.t('>>>>(A): agent_cache_processed: K: ' + hres.ks.kqk);
  var data = {ks         : hres.ks,
              watch      : hres.watch,
              merge_data : hres.merge_data,
              status     : "OK"};
  ZCloud.Respond(hres, data);
}

function cluster_geo_evict_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterGeoEvict', err, hres)) return;
  ZH.t('>>>>(I): cluster_geo_evict_processed: K: ' + hres.ks.kqk);
  var data = {ks : hres.ks, status : "OK"};
  ZCloud.Respond(hres, data);
}

function cluster_geo_local_evict_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterGeoLocalEvict', err, hres)) return;
  ZH.t('>>>>(I): cluster_geo_local_evict_processed: K: ' + hres.ks.kqk);
  var data = {ks : hres.ks, status : "OK"};
  ZCloud.Respond(hres, data);
}

function geo_evict_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastEvict', err, hres)) return;
  ZH.t('>>>>(G): geo_evict_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function geo_local_evict_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastLocalEvict', err, hres)) return;
  ZH.t('>>>>(G): geo_local_evict_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function cluster_evict_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterEvict', err, hres)) return;
  ZH.t('>>>>(I): cluster_evict_processed: K: ' + hres.ks.kqk);
  var data = {ks : hres.ks, status : "OK"};
  ZCloud.Respond(hres, data);
}

function agent_evict_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentEvict', err, hres)) return;
  ZH.t('>>>>(A): agent_evict_processed: K: ' + hres.ks.kqk);
  var data = {ks         : hres.ks,
              need_merge : hres.need_merge,
              status     : "OK"};
  ZCloud.Respond(hres, data);
}

function agent_local_evict_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentLocalEvict', err, hres)) return;
  ZH.t('>>>>(A): agent_local_evict_processed: K: ' + hres.ks.kqk);
  var data = {ks : hres.ks, status : "OK"};
  ZCloud.Respond(hres, data);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN RESPONSES -----------------------------------------------------------

function admin_authenticate_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AdminAuthenticate', err, hres)) return;
  ZH.t('>>>>(A): admin_authenticate_processed: UN: ' + hres.auth.username);
  var data = {authentication : {username : hres.auth.username},
              ok             : hres.ok,
              status         : "OK"};
  ZCloud.Respond(hres, data);
}

function geo_add_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastAddUser', err, hres)) return;
  ZH.t('>>>>(G): geo_add_user_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function admin_add_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AdminAddUser', err, hres)) return;
  ZH.t('>>>>(A): admin_add_user_processed: UN: ' + hres.username);
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function broadcast_remove_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('BroadcastRemoveUser', err, hres)) return;
  ZH.t('>>>>(I): broadcast_remove_user_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function geo_remove_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastRemoveUser', err, hres)) return;
  ZH.t('>>>>(G): geo_remove_user_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function admin_remove_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AdminRemoveUser', err, hres)) return;
  ZH.t('>>>>(A): admin_remove_user_processed: UN: ' + hres.username);
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function broadcast_grant_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('BroadcastGrantUser', err, hres)) return;
  ZH.t('>>>>(I): broadcast_grant_user_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function geo_grant_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoBroadcastGrantUser', err, hres)) return;
  ZH.t('>>>>(G): geo_grant_user_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function admin_grant_user_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AdminGrantUser', err, hres)) return;
  ZH.t('>>>>(A): admin_grant_user_processed: UN: ' + hres.username);
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function admin_remove_dc_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AdminRemoveDataCenter', err, hres)) return;
  ZH.t('>>>>(A): admin_remove_dc_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// VOTE RESPONSES (LEADERS) --------------------------------------------------

function geo_state_change_vote_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoStateChangeVote', err, hres)) {
    return;
  }
  ZH.t('>>>>(G): geo_state_change_vote_processed');
  var data = {datacenter                     : ZH.MyDataCenter,
              term_number                    : hres.term_number,
              vote_id                        : hres.vote_id,
              geo_nodes                      : hres.geo_nodes,
              geo_network_partition          : hres.geo_network_partition,
              geo_majority                   : hres.geo_majority,
              central_synced                 : hres.central_synced,
              status                         : "OK"};
  ZCloud.Respond(hres, data);
}

function geo_state_change_commit_processed(err, hres) {
  if (ZCloud.HandleCallbackError('GeoStateChangeCommit', err, hres)) {
    return;
  }
  ZH.t('>>>>(G): geo_state_change_commit_processed');
  var data = {term_number : hres.term_number,
              vote_id     : hres.vote_id,
              status      : "OK"};
  ZCloud.Respond(hres, data);
}

function geo_leader_ping_processed(err, hres) {
  if (!ReplyToGeoLeaderPing) return;
  if (ZCloud.HandleCallbackError('GeoLeaderPing', err, hres)) return;
  ZH.t('>>>>(G): geo_leader_ping_processed');
  var data = {status : "OK"};
  ZCloud.Respond(hres, data);
}

function geo_data_ping_processed(err, hres) {
  if (!ReplyToGeoDataPing) return;
  if (ZCloud.HandleCallbackError('GeoDataPing', err, hres)) return;
  ZH.t('>>>>(G): geo_data_ping_processed');
  var data = {status : "OK"};
  ZCloud.Respond(hres, data);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// APP-SERVER-CLUSTER RESPONSES ----------------------------------------------

function announce_new_app_server_cluster_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AnnounceNewAppServerCluster', err, hres)) {
    return;
  }
  ZH.t('>>>>(A): announce_new_app_server_cluster_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function agent_message_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentMessage', err, hres)) return;
  ZH.t('>>>>(A): agent_message_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}

function agent_request_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AgentRequest', err, hres)) return;
  ZH.t('>>>>(A): agent_request_processed');
  var data = {r_data : hres.r_data, status : "OK"};
  ZCloud.Respond(hres, data);
}

function announce_new_memcache_cluster_processed(err, hres) {
  if (ZCloud.HandleCallbackError('AnnounceNewMemcacheCluster', err, hres)) {
    return;
  }
  ZH.t('>>>>(A): announce_new_memcache_cluster_processed');
  var data = create_data_ok_response();
  ZCloud.Respond(hres, data);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT (DIRECT) RESPONSES --------------------------------------------------

function client_direct_processed(hres) {
  var data = {ks     : hres.ks,
              json   : hres.json,
              crdt   : hres.crdt,
              status : "OK"};
  ZCloud.Respond(hres, data);
}

function client_store_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClientStore', err, hres)) return;
  ZH.t('>>>>(A): client_store_processed: K: ' + hres.ks.kqk);
  hres.json = ZH.CreatePrettyJson(hres.crdt);
  client_direct_processed(hres);
}

function cluster_client_store_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterClientStore', err, hres)) return;
  ZH.t('>>>>(A): cluster_client_store_processed: K: ' + hres.ks.kqk);
  client_direct_processed(hres);
}

function client_fetch_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClientFetch', err, hres)) return;
  ZH.t('>>>>(A): client_fetch_processed: K: ' + hres.ks.kqk);
  hres.json = ZH.CreatePrettyJson(hres.crdt);
  client_direct_processed(hres);
}

function cluster_client_fetch_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterClientFetch', err, hres)) return;
  ZH.t('>>>>(A): cluster_client_fetch_processed: K: ' + hres.ks.kqk);
  client_direct_processed(hres);
}

function cluster_client_commit_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterClientCommit', err, hres)) return;
  ZH.t('>>>>(A): cluster_client_commit_processed: K: ' + hres.ks.kqk);
  client_direct_processed(hres);
}

function client_commit_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClientCommit', err, hres)) return;
  ZH.t('>>>>(A): client_commit_processed: K: ' + hres.ks.kqk);
  hres.json = ZH.CreatePrettyJson(hres.crdt);
  client_direct_processed(hres);
}

function cluster_client_remove_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterClientRemove', err, hres)) return;
  ZH.t('>>>>(A): cluster_client_remove_processed: K: ' + hres.ks.kqk);
  var data = {ks : hres.ks, status : "OK"};
  ZCloud.Respond(hres, data);
}

function client_remove_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClientRemove', err, hres)) return;
  ZH.t('>>>>(A): client_remove_processed: K: ' + hres.ks.kqk);
  var data = {ks : hres.ks, status : "OK"};
  ZCloud.Respond(hres, data);
}

function cluster_client_stateless_commit_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClusterClientStatelessCommit', err, hres)) {
    return;
  }
  ZH.t('>>>>(A): cluster_client_stateless_commit_processed: K: ' + hres.ks.kqk);
  client_direct_processed(hres);
}

function client_stateless_commit_processed(err, hres) {
  if (ZCloud.HandleCallbackError('ClientStatelessCommit', err, hres)) return;
  ZH.t('>>>>(A): client_stateless_commit_processed: K: ' + hres.ks.kqk);
  hres.json = ZH.CreatePrettyJson(hres.crdt);
  client_direct_processed(hres);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO-PING ------------------------------------------------------------------

function handle_geo_leader_ping(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var is_geo  = true;
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var next    = geo_leader_ping_processed;
  var cnode   = ZCLS.GetClusterLeaderNode();
  if (!cnode) return; // GeoLeaderPing comes before ClusterVote finishes
  if (cnode.device_uuid !== ZH.MyUUID) return redirect_cluster_leader(hres);
  var gnode   = params.data.geo_node;
  ZCLS.HandleGeoLeaderPing(net.plugin, net.collections, gnode, hres, next);
}

function handle_geo_data_ping(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var is_geo  = true;
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var next    = geo_data_ping_processed;
  var device  = params.data.device;
  var cnode   = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return; // GeoDataPing comes before ClusterVote finishes
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var gnode   = params.data.geo_node;
  ZCLS.HandleGeoDataPing(net.plugin, net.collections, gnode, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-PING ----------------------------------------------------------------

function handle_agent_ping(conn, params, id, wid, net, next) {
  var is_geo  = false;
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device  = params.data.device;
  var duuid   = device.uuid;
  if (duuid) {
    var cnode = ZPart.GetClusterNode(duuid);
    if (!cnode) { // Ongoing election
      return ZCloud.SendBackoff(duuid, hres);
    }
    if (cnode.device_uuid !== ZH.MyUUID) {
      return redirect_agent_master(is_geo, cnode, hres);
    }
  }
  next(null, hres);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-ONLINE --------------------------------------------------------------

// NOTE: 'AgentOnline' calls do NOT AUTHENTICATE
function handle_agent_online(conn, params, id, wid, net, next) {
  var is_geo  = false;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, null);
  if (ZH.ChaosMode === 14) {
    ZH.e('CHAOS-MODE: ' + ZH.ChaosDescriptions[14]);
    return redirect_other_datacenter(hres);
  }
  var device  = params.data.device;
  if (device.uuid !== -1) {
    var cnode = ZPart.GetClusterNode(device.uuid);
    if (!cnode) { // Ongoing election
      return ZCloud.SendBackoff(params.data.device.uuid, hres);
    }
    if (cnode.device_uuid !== ZH.MyUUID) {
      return redirect_agent_master(is_geo, cnode, hres);
    }
  }
  var dkey = device.key;
  if (!ZH.DisableDeviceKeyCheck) {
    if (dkey) {
      if (!ZH.DeviceKeyMatch(params.data.device)) {
        return ZCloud.ReplyDenyDeviceKey(hres);
      }
    }
  }
  var created = params.data.created;
  var perms   = params.data.permissions;
  var schans  = params.data.subscriptions;
  var susers  = params.data.stationedusers;
  var b       = params.data.value;
  var server  = params.data.server;
  ZH.t('<-|(A): AgentOnline: U: '  + device.uuid);
  ZDConn.RouterHandleAgentOnline(net, b, device, created, perms,
                                 schans, susers, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO/BROADCAST DEVICE-KEY --------------------------------------------------

function handle_broadcast_https_agent(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var rguuid = params.data.agent.datacenter;
  var auuid  = params.data.agent.uuid;
  var dkey   = params.data.agent.key;
  var server = params.data.agent.server;
  ZH.t('<-|(I): BroadcastHttpsAgent');
  ZDConn.HandleBroadcastHttpsAgent(net, rguuid, auuid, dkey, server, next);
}

function handle_geo_broadcast_https_agent(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var rguuid = params.data.datacenter;
  var auuid  = params.data.agent.uuid;
  var dkey   = params.data.agent.key;
  var server = params.data.agent.server;
  ZH.t('<-|(G): GeoBroadcastHttpsAgent');
  // NOTE: ZPio.BroadcastHttpsAgent() is ASYNC
  ZPio.BroadcastHttpsAgent(net.plugin, net.collections,
                           rguuid, server, auuid, dkey);
  var dkeys    = {};
  dkeys[auuid] = dkey;
  ZDQ.PushStorageDeviceKeys(net.plugin, net.collections, dkeys, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-PING ----------------------------------------------------------------

// NOTE: 'AgentRecheck' calls do NOT AUTHENTICATE
function handle_agent_recheck(conn, params, id, wid, net, next) {
  var is_geo  = false;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, null);
  var device  = params.data.device;
  var cnode = ZPart.GetClusterNode(device.uuid);
  if (!cnode) { // Ongoing election
    return ZCloud.SendBackoff(params.data.device.uuid, hres);
  }
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var agent   = params.data.agent;
  var created = params.data.created;
  ZH.t('<-|(A): AgentRecheck: U: '  + agent.uuid + ' C: ' + created);
  ZDConn.CentralHandleAgentRecheck(net, agent, created, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET AGENT KETS ------------------------------------------------------------

function handle_get_agent_keys(conn, params, id, wid, net, next) {
  var is_geo  = false;
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device  = params.data.device;
  var cnode   = ZPart.GetClusterNode(device.uuid);
  if (!cnode) { // Ongoing election
    return ZCloud.SendBackoff(params.data.device.uuid, hres);
  }
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var agent   = params.data.agent;
  var nkeys   = params.data.num_keys;
  var minage  = params.data.minimum_age;
  var wonly   = params.data.watch_only;
  ZH.t('<-|(A): AgentGetAgentKeys: U: '  + device.uuid);
  ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.AdminAuthFail), hres);
    else {
      ZDConn.CentralHandleAgentGetAgentKeys(net.plugin, net.collections,
                                            agent, nkeys, minage, wonly,
                                            hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// KEY SERIALIZATION QUEUE ---------------------------------------------------

function flow_central_key_serialization(plugin, collections, qe, next) { 
  var op = qe.op;
  var pfunc;
  switch(op) {
    case 'CLUSTER_DELTA'                     :
      pfunc = flow_handle_internal_delta;
      break;
    case 'STORAGE_APPLY_DELTA'               :
      pfunc = flow_handle_storage_apply_delta;
      break;
    case 'STORAGE_APPLY_DENTRIES'            :
      pfunc = flow_handle_storage_apply_dentries;
      break;
    case 'STORAGE_FREEZE_KEY'                :
      pfunc = flow_handle_storage_freeze_key;
      break;
    case 'CLUSTER_DENTRIES'                  :
      pfunc = flow_handle_cluster_dentries;
      break;
    case 'STORAGE_GEO_DELTA_ACK'             :
      pfunc = ZGack.FlowHandleStorageAckGeoDelta;
      break;
    case 'STORAGE_GEO_DENTRIES_ACK'          :
      pfunc = ZGack.FlowHandleStorageAckGeoDentries;
      break;
    case 'GEO_COMMIT'                        :
      pfunc = flow_handle_geo_cluster_commit;
      break;
    case 'STORAGE_GEO_COMMIT'                :
      pfunc = flow_handle_storage_geo_cluster_commit;
      break;
    case 'STORAGE_ACK_GEO_COMMIT'            :
      pfunc = flow_handle_storage_geo_ack_cluster_commit;
      break;
    case 'STORAGE_GEO_SUBSCRIBER_COMMIT'     :
      pfunc = flow_handle_storage_geo_subscriber_commit_delta;
      break;
    case 'DRAIN_GEO_DELTAS'                  :
      pfunc = ZGDD.FlowHandleDrainGeoDeltas;
      break;
    case 'SERIALIZED_DRAIN_GEO_DELTAS'       :
      pfunc = ZGDD.FlowSerializedDrainGeoDeltas;
      break;
    case 'STORAGE_FIX_OOO_GCV_DELTA'         :
      pfunc = ZRAD.FlowFixOOOGCVDelta;
      break;
    case 'STORAGE_NEW_GEO_CLUSTER_SEND_REORDER_DELTA' :
      pfunc = ZNM.NewGeoClusterSendReorderDelta;
      break;
    case 'STORAGE_GCV_REAP'                  :
      pfunc = ZGCReap.FlowDoStorageGCVReap;
      break;

    case 'STORAGE_NEED_MERGE'                :
      pfunc = flow_handle_storage_need_merge;
      break;
    case 'STORAGE_GEO_NEED_MERGE'            :
      pfunc = flow_handle_storage_geo_need_merge;
      break;
    case 'STORAGE_ACK_GEO_NEED_MERGE'        :
      pfunc = flow_handle_storage_ack_geo_need_merge;
      break;
    case 'ROUTER_GEO_NEED_MERGE_RESPONSE'    :
      pfunc = ZNM.FlowHandleRouterGeoNeedMergeResponse;
      break;

    case 'CLUSTER_CACHE'                     :
      pfunc = flow_handle_cluster_cache;
      break;
    case 'STORAGE_CLUSTER_CACHE'             :
      pfunc = flow_handle_storage_cluster_cache;
      break;
    case 'GEO_CACHE'                         :
      pfunc = flow_handle_geo_cluster_cache;
      break;
    case 'CLUSTER_EVICT'                     :
      pfunc = flow_handle_cluster_evict;
      break;
    case 'STORAGE_CLUSTER_EVICT'             :
      pfunc = flow_handle_storage_cluster_evict;
      break;
    case 'STORAGE_CLUSTER_LOCAL_EVICT'       :
      pfunc = flow_handle_storage_cluster_local_evict;
      break;
    case 'GEO_CLUSTER_EVICT'                 :
      pfunc = flow_handle_geo_cluster_evict;
      break;
    case 'GEO_CLUSTER_LOCAL_EVICT'           :
      pfunc = flow_handle_geo_cluster_local_evict;
      break;

    case 'CENTRAL_EXPIRE'                    :
      pfunc = ZED.FlowCentralExpire;
      break;

    case 'STORAGE_CLUSTER_CLIENT_CALL'       :
      pfunc = ZDirect.FlowHandleStorageClusterClientCall;
      break;

    default                                  :
      throw(new Error('PROGRAM ERROR(KeySerialization)'));
  }
  pfunc(plugin, collections, qe, next);
}

exports.FlowCentralKeySerialization = function(plugin, collections, qe, next) { 
  flow_central_key_serialization(plugin, collections, qe, next);
}

exports.GetCentralKeySerializationId = function(qe) {
  var op = qe.op;
  var id;
  if (qe.ks && qe.ks.kqk) {
    id = qe.ks.kqk;
  } else {
    ZH.e('GetCentralKeySerializationId: OP: ' + op);
    throw(new Error('PROGRAM ERROR(GetCentralKeySerializationId)'));
  }
  return id;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO/CLUSTER/SUBSCRIBER-COMMIT-DELTA ---------------------------------------

function flow_handle_storage_geo_subscriber_commit_delta(plugin, collections,
                                                         qe, next) {
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var net    = qe.net;
  var ks     = qe.ks;
  ZH.t('<-|(I): StorageGeoSubscriberCommitDelta: K: ' + ks.kqk);
  var data   = qe.data;
  var author = data.author;
  var rchans = data.rchans;
  ZGack.HandleStorageGeoSubscriberCommitDelta(net, ks, author, rchans,
  function(serr, sres) {
    qnext(serr, qhres);
    next(null, null);
  });
}

exports.HandleStorageGeoSubscriberCommitDelta = function(net, ks, author,
                                                         rchans, next) {
  var data     = {ks : ks, author : author, rchans : rchans};
  var mname    = 'STORAGE_GEO_SUBSCRIBER_COMMIT';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, next);
  var flow     = {k : ks.kqk,
                  q : ZQ.StorageKeySerializationQueue,
                  m : ZQ.StorageKeySerialization,
                  f : flow_central_key_serialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

// NOTE: QUEUE via ZQ.PubSubQueue[]
function handle_cluster_subscriber_commit_delta(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var ks     = params.data.ks;
  ZH.t('<-|(I): ClusterSubscriberCommitDelta: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  ZCSub.HandleClusterSubscriberCommitDelta(net, ks, author, rchans);
}

function handle_cluster_geo_subscriber_commit_delta(conn, params,
                                                    id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var ks     = params.data.ks;
  ZH.t('<-|(I): ClusterGeoSubscriberCommitDelta: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  ZDQ.PushStorageGeoSubscriberCommitDelta(net, ks, author, rchans,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      // NOTE: ZCSub.HandleGeoSubscriberCommitDelta() is ASYNC
      ZCSub.HandleGeoSubscriberCommitDelta(net, ks, author, rchans);
      next(null, null);
    }
  });
}

function handle_geo_subscriber_commit_delta(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var ks     = params.data.ks;
  ZH.t('<-|(I): GeoSubscriberCommitDelta: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  // NOTE: ZPio.SendClusterGeoSubscriberCommitDelta() is ASYNC
  ZPio.SendClusterGeoSubscriberCommitDelta(net, ks, author, rchans);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ACK-GEO-COMMIT-SUBSCRIBER-DELTA -------------------------------------------

function flow_handle_storage_geo_ack_cluster_commit(plugin, collections,
                                                    qe, next) {
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var net    = qe.net;
  var ks     = qe.ks;
  ZH.t('<-|(I): StorageClusterAckGeoCommitDelta: K: ' + ks.kqk);
  var data   = qe.data;
  var author = data.author;
  var rchans = data.rchans;
  var rguuid = data.rguuid;
  ZGack.HandleStorageAckGeoCommitDelta(net, ks, author, rchans, rguuid,
  function(serr, sres) {
    qnext(serr, qhres);
    next(null, null);
  });
}

exports.HandleStorageClusterAckGeoCommitDelta = function(net, ks, author,
                                                         rchans, rguuid, next) {
  var data     = {ks : ks, author : author, rchans : rchans, rguuid : rguuid};
  var mname    = 'STORAGE_ACK_GEO_COMMIT';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, next);
  var flow     = {k : ks.kqk,
                  q : ZQ.StorageKeySerializationQueue,
                  m : ZQ.StorageKeySerialization,
                  f : flow_central_key_serialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_cluster_ack_geo_commit_delta(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var ks     = params.data.ks;
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var rguuid = params.data.datacenter;
  ZH.t('<-|(G): ClusterAckGeoCommitDelta: K: ' + ks.kqk + ' RU: ' + rguuid);
  ZDQ.PushStorageAckGeoCommitDelta(net, ks, author, rchans, rguuid, next);
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_ack_geo_commit_delta(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks     = params.data.ks;
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var rguuid = params.data.datacenter;
  ZH.t('<-|(G): AckGeoCommitDelta: K: ' + ks.kqk + ' RU: ' + rguuid);
  // NOTE: ZPio.SendClusterAckGeoCommitDelta() is ASYNC
  ZPio.SendClusterAckGeoCommitDelta(net.plugin, net.collections,
                                    ks, author, rchans, rguuid, hres, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMIT-SUBSCRIBER-DELTA [CLUSTER, GEO, GEO-CLUSTER] -----------------------

function flow_handle_storage_geo_cluster_commit(plugin, collections, qe, next) {
  var tn     = T.G();
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var net    = qe.net;
  var ks     = qe.ks;
  ZH.t('<-|(I): StorageClusterGeoCommitDelta: K: ' + ks.kqk);
  var data   = qe.data;
  var author = data.author;
  var rchans = data.rchans;
  var rguuid = data.rguuid;
  ZGack.HandleStorageGeoClusterCommitDelta(net, ks, author, rchans, rguuid,
  function(serr, sres) {
    T.X(tn);
    qnext(serr, qhres);
    next(null, null);
  });
}

exports.HandleStorageClusterGeoCommitDelta = function(net, ks, author,
                                                      rchans, rguuid, next) {
  var data     = {ks : ks, author : author, rchans : rchans, rguuid : rguuid};
  var mname    = 'STORAGE_GEO_COMMIT';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, next);
  var flow     = {k : ks.kqk,
                  q : ZQ.StorageKeySerializationQueue,
                  m : ZQ.StorageKeySerialization,
                  f : flow_central_key_serialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function flow_handle_geo_cluster_commit(plugin, collections, qe, next) {
  var tn     = T.G();
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var net    = qe.net;
  var ks     = qe.ks;
  ZH.t('<-|(I): ClusterGeoCommitDelta: K: ' + ks.kqk);
  var data   = qe.data;
  var author = data.author;
  var rchans = data.rchans;
  var rguuid = data.rguuid;
  ZGack.RouterHandleGeoClusterCommitDelta(net.plugin, net.collections,
                                          ks, author, rchans, rguuid,
  function(serr, sres) {
    T.X(tn);
    qnext(serr, qhres);
    next(null, null);
  });
}

function handle_cluster_geo_commit_delta(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) {
    return next(new Error(ZS.Errors.GenericSecurity), null);
  }
  // NOTE: Neither AgentMaster nor KeyMaster -> SKIP cnode check
  var ks     = params.data.ks;
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var rguuid = params.data.datacenter;
  var data   = {ks : ks, author : author, rchans : rchans, rguuid : rguuid};
  var mname  = 'GEO_COMMIT';
  ZQ.AddToKeySerializationFlow(ZQ.RouterKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, next);
  var flow   = {k : ks.kqk,
                q : ZQ.RouterKeySerializationQueue,
                m : ZQ.RouterKeySerialization,
                f : flow_central_key_serialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_geo_commit_delta(conn, params, id, wid, net, next) {
  if (ZH.ChaosMode === 8) return ZCloud.ChaosDropResponse(8);
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var is_geo   = true;
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var ks     = params.data.ks;
  ZH.t('<-|(G): GeoCommitDelta: K: ' + ks.kqk);
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var rguuid = params.data.datacenter;
  ZPio.SendClusterGeoCommitDelta(net.plugin, net.collections,
                                 ks, author, rchans, rguuid, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DELTAS [AGENT, CLUSTER-AGENT, CLUSTER-SUBSCRIBER] -------------------------

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_ack_geo_delta(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var ks     = params.data.ks;
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var rguuid = params.data.datacenter;
  ZH.t('<-|(G): AckGeoDelta: K: ' + ks.kqk + ' RU: ' + rguuid);
  ZGack.HandleAckGeoDelta(net, ks, author, rchans, rguuid, next);
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_ack_geo_delta_error(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var ks     = params.data.ks;
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var rguuid = params.data.datacenter;
  var error  = params.data.error;
  ZH.t('<-|(G): AckGeoDeltaError: K: ' + ks.kqk + ' RU: ' + rguuid);
  ZGack.HandleAckGeoDeltaError(net, ks, author, rchans, error, rguuid, next);
}

// NOTE: QUEUE via ZQ.PubSubQueue[]
function handle_cluster_subscriber_delta(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks       = params.data.ks;
  ZH.t('<-|(I): ClusterSubscriberDelta: K: ' + ks.kqk);
  var dentry   = params.data.dentry;
  var do_watch = params.data.do_watch;
  ZCSub.HandleClusterSubscriberDelta(net, ks, dentry, do_watch, hres);
}

// NOTE: QUEUE via ZQ.PubSubQueue[]
function handle_cluster_subscriber_dentries(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: NO REDIRECT -> PAYLOAD IS TOO IMPORTANT
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks       = params.data.ks;
  ZH.t('<-|(I): ClusterSubscriberDentries: K: ' + ks.kqk);
  var agent    = params.data.agent;
  var dentries = params.data.dentries;
  ZCSub.HandleClusterSubscriberDentries(net, ks, agent, dentries, hres);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DELTA/DENTRIES ERRORS -----------------------------------------------

// NOTE: QUEUE via ZQ.PubSubQueue[]
function handle_cluster_subscriber_agent_delta_error(conn, params,
                                                     id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: Neither AgentMaster nor KeyMaster -> SKIP cnode check
  var ks     = params.data.ks;
  ZH.t('<-|(I): ClusterSubscriberAgentDeltaError: K: ' + ks.kqk);
  var agent  = params.data.agent;
  var author = params.data.author;
  var rchans = params.data.replication_channels;
  var error  = params.data.error;
  ZCSub.HandleClusterSubscriberAgentDeltaError(net, ks, agent,
                                               author, rchans, error);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INTERNAL (CLUSTER & GEOCLUSTER) DELTA/DENTRIES ----------------------------

exports.HasClusterDeltaPermissions = function(net, auth, ks, duuid, rchans,
                                              next) {
  ZAuth.HasWritePermissions(net, auth, ks, duuid, rchans, function(aerr, ok) {
    if (aerr && aerr.message !== ZS.Errors.AuthError) next(aerr, null);
    else {
      if (ok) next(null, true);
      else {
        ZAuth.HasSecurityTokenPermissions(net, ks, function(aerr, ok) {
          next(aerr, ok);
        });
      }
    }
  });
}

function do_flow_handle_internal_delta(plugin, collections, qe, hres, next) {
  var net      = qe.net;
  var auth     = qe.auth;
  var ks       = qe.ks;
  var data     = qe.data;
  var rguuid   = data.rguuid;
  var dentry   = data.dentry;
  var rchans   = data.rchans;
  var acache   = data.acache;
  var is_geo   = data.is_geo;
  var username = auth.username;
  var duuid    = dentry._;
  hres.auth    = auth; // NOTE: Used in ClusterDelta/Dentries ERROR responses
  var mname    = is_geo ? 'ClusterGeoDelta' : 'ClusterDelta';
  ZH.t('<-|(I): ' + mname + ': ' + ZH.SummarizeDelta(ks.key, dentry));
  if (is_geo) { // NOTE: GEO are pre-AUTHed
    ZPub.HandleClusterDelta(net, ks, dentry, rguuid, username, is_geo,
                            hres, next);
  } else {
    // NOTE Auth done at KeyMaster to get correct AV for error messages
    exports.HasClusterDeltaPermissions(net, auth, ks, duuid, rchans,
    function(aerr, ok) {
      if (aerr) next(aerr, hres);
      else {
        if (!ok) next(new Error(ZS.Errors.WritePermsFail), hres);
        else {
          ZPub.HandleClusterDelta(net, ks, dentry, rguuid, username, is_geo,
                                  hres, next);
          if (acache) {
            var agent = {uuid : duuid};
            // NOTE: ZPio.GeoBroadcastCache is ASYNC
            ZPio.GeoBroadcastCache(plugin, collections, ks, false, agent);
          }
        }
      }
    });
  }
}

function flow_handle_internal_delta(plugin, collections, qe, next) {
  var tn     = T.G();
  var net    = qe.net;
  var qnext  = qe.next;
  var qhres  = qe.hres;
  do_flow_handle_internal_delta(plugin, collections, qe, qhres,
  function(serr, sres) {
    T.X(tn);
    qnext(serr, qhres);
    next (null, null);
  });
}

function handle_internal_delta(conn, params, id, wid, net, is_geo, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks      = params.data.ks;
  var cnode   = ZPart.GetKeyNode(ks);
  if (!cnode || cnode.device_uuid !== ZH.MyUUID) {
    return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  }
  var rguuid  = params.data.datacenter;
  var dentry  = params.data.dentry;
  var meta    = dentry.delta._meta;
  var rchans  = meta.replication_channels;
  var acache  = meta.auto_cache;
  var data    = {rguuid   : rguuid,
                 dentry   : dentry,
                 rchans   : rchans,
                 acache   : acache,
                 is_geo   : is_geo};
  var mname   = 'CLUSTER_DELTA';
  ZQ.AddToKeySerializationFlow(ZQ.RouterKeySerializationQueue, ks, mname,
                               net, data, auth, hres, next);
  var flow = {k : ks.kqk,
              q : ZQ.RouterKeySerializationQueue,
              m : ZQ.RouterKeySerialization,
              f : flow_central_key_serialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function handle_cluster_delta(conn, params, id, wid, net, next) {
  handle_internal_delta(conn, params, id, wid, net, false, next);
}

function handle_geo_cluster_delta(conn, params, id, wid, net, next) {
  handle_internal_delta(conn, params, id, wid, net, true, next);
}

function flow_handle_storage_apply_delta(plugin, collections, qe, next) {
  var tn     = T.G();
  var net    = qe.net;
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var ks     = qe.ks;
  var data   = qe.data;
  var dentry = data.dentry;
  var rguuid = data.rguuid;
  ZH.t('<-|(R): StorageApplyDelta: K ' + ks.kqk);
  ZRAD.StorageHandleApplyDelta(net, ks, dentry, rguuid, false, qhres,
  function(serr, sres) {
    T.X(tn);
    qnext(serr, qhres);
    next (null, null);
  });
}

function flow_handle_storage_apply_dentries(plugin, collections, qe, next) {
  var net      = qe.net;
  var qnext    = qe.next;
  var qhres    = qe.hres;
  var ks       = qe.ks;
  var data     = qe.data;
  var dentries = data.dentries;
  var rguuid   = data.rguuid;
  ZH.t('<-|(R): StorageApplyDentries: K ' + ks.kqk);
  ZRAD.StorageHandleApplyDentries(net, ks, dentries, rguuid, qhres,
  function(serr, sres) {
    qnext(serr, qhres);
    next (null, null);
  });
}

function flow_handle_storage_freeze_key(plugin, collections, qe, next) {
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  var ks    = qe.ks;
  ZH.t('<-|(R): StorageFreezeKey: K ' + ks.kqk);
  ZDConn.StorageHandleFreezeKey(net, ks, function(serr, sres) {
    qnext(serr, qhres);
    next (null, null);
  });
}

function flow_handle_cluster_dentries(plugin, collections, qe, next) {
  var net       = qe.net;
  var qnext     = qe.next;
  var qhres     = qe.hres;
  var auth      = qe.auth;
  var ks        = qe.ks;
  var data      = qe.data;
  var rguuid    = data.rguuid;
  var agent     = data.agent;
  var dentries  = data.dentries;
  var freeze    = data.freeze;
  var is_geo    = data.is_geo;
  var username  = auth.username;
if (!dentries) { ZH.e('flow_handle_cluster_dentries'); ZH.e(data); }
  qhres.auth    = auth; // NOTE: Used in ClusterDelta/Dentries ERROR responses
  var mname     = is_geo ? 'ClusterGeoDentries' : 'ClusterDentries';
  ZH.t('<-|(I): ' + mname + ': #Ds: ' + dentries.length);
  ZPub.HandleClusterDentries(net, ks, agent, dentries, freeze, rguuid,
                             username, is_geo, qhres,
  function(perr, pres) {
    qnext(perr, qhres);
    next (null, null);
  });
}

function handle_internal_dentries(is_geo, conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth      = params.authentication;
  var hres      = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks        = params.data.ks;
  var rguuid    = params.data.datacenter;
  var agent     = params.data.agent;
  var dentries  = params.data.dentries;
  var freeze    = params.data.freeze;
  var cnode     = ZPart.GetKeyNode(ks);
  if (!cnode || cnode.device_uuid !== ZH.MyUUID) {
    return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  }
  var data    = {rguuid   : rguuid,
                 agent    : agent,
                 dentries : dentries,
                 freeze   : freeze,
                 is_geo   : is_geo};
  var mname   = 'CLUSTER_DENTRIES';
  ZQ.AddToKeySerializationFlow(ZQ.RouterKeySerializationQueue, ks, mname,
                               net, data, auth, hres, next);
  var flow = {k : ks.kqk,
              q : ZQ.RouterKeySerializationQueue,
              m : ZQ.RouterKeySerialization,
              f : flow_central_key_serialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function handle_cluster_dentries(conn, params, id, wid, net, next) {
  handle_internal_dentries(false, conn, params, id, wid, net, next);
}

function handle_cluster_geo_dentries(conn, params, id, wid, net, next) {
  handle_internal_dentries(true, conn, params, id, wid, net, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNAL (AGENT & GEO) DELTA/DENTRIES -------------------------------------

function handle_external_dentries(is_geo, conn, params, id, wid, net, next) {
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device  = params.data.device;
  var cnode   = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var ks        = params.data.ks;
  var mname     = is_geo ? 'GEO_DENTRIES' : 'AGENT_DENTRIES';
  ZH.t('<-|(E): ' + mname + ': K: ' + ks.kqk);
  var agent     = params.data.agent;
  var dentries  = params.data.dentries;
  if (ZH.CentralExtendedTimestampsInDeltas) {
    var now = ZH.GetMsTime();
    for (var i = 0; i < dentries.length; i++) {
      var dentry = dentries[i];
      dentry.delta._meta.geo_received = now;
    }
  }
  if (is_geo) {
    var freeze = params.data.freeze;
    var rguuid = params.data.datacenter;
    ZPio.SendClusterGeoDentries(net.plugin, net.collections,
                                ks, agent, dentries, freeze, rguuid,
                                auth, hres, next);
  } else {
    var rguuid = ZH.MyDataCenter;
    ZPio.SendClusterDentries(net.plugin, net.collections,
                             ks, agent, dentries, rguuid, auth, hres, next);
    // NOTE: agent_dentries_processed() is ASYNC -> ONLY FOR HTTPS
    agent_dentries_processed(null, hres);
  }
}

// NOTE: NO FLOWS, flows are handled in Datanet layer, not customer layer
function handle_geo_dentries(conn, params, id, wid, net, next) {
  if (ZH.ChaosMode === 10) return ZCloud.ChaosDropResponse(10);
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;         T.G();
  handle_external_dentries(true, conn, params, id, wid, net, next);      T.X();
}

// NOTE: NO FLOWS, flows are handled in Datanet layer, not customer layer
function handle_agent_dentries(conn, params, id, wid, net, next) {
  if (ZH.ChaosMode === 17) return ZCloud.ChaosDropResponse(17);          T.G();
  handle_external_dentries(false, conn, params, id, wid, net, next);     T.X();
}

function handle_external_delta(is_geo, conn, params, id, wid, net, next) {
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device  = params.data.device;
  var cnode   = ZPart.GetClusterNode(device.uuid);
  if (!cnode) {
    if (is_geo) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
    else        return ZCloud.SendBackoff(device.uuid, hres);
  }
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var mname   = is_geo ? 'GEO_DELTA' : 'AGENT_DELTA';
  var ks      = params.data.ks;
  ZH.t('<-|(E): ' + mname + ': K: ' + ks.kqk);
  var dentry  = params.data.dentry;
  if (ZH.CentralExtendedTimestampsInDeltas) {
    if (is_geo) dentry.delta._meta.geo_received   = ZH.GetMsTime();
    else        dentry.delta._meta.agent_received = ZH.GetMsTime();
  }
  if (is_geo) {
    var rguuid = params.data.datacenter;
    ZPio.SendClusterGeoDelta(net.plugin, net.collections, 
                             ks, dentry, rguuid, auth, hres, next);
  } else {
    var rguuid = ZH.MyDataCenter;
    ZPio.SendClusterDelta(net.plugin, net.collections,
                          ks, dentry, rguuid, auth, hres, next);
    // NOTE: agent_delta_processed() is ASYNC -> ONLY FOR HTTPS
    agent_delta_processed(null, hres);
  }
}

// NOTE: NO FLOWS, flows are handled in Datanet layer, not customer layer
function handle_agent_delta(conn, params, id, wid, net, next) {
  if (ZH.ChaosMode === 17) return ZCloud.ChaosDropResponse(17);          T.G();
  handle_external_delta(false,  conn, params, id, wid, net, next);       T.X();
}

// NOTE: NO FLOWS, flows are handled in Datanet layer, not customer layer
function handle_geo_delta(conn, params, id, wid, net, next) {
  if (ZH.ChaosMode === 10) return ZCloud.ChaosDropResponse(10);
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;         T.G();
  handle_external_delta(true,  conn, params, id, wid, net, next);        T.X();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NEED-MERGE [AGENT, CLUSTER] -----------------------------------------------

// NOTE: QUEUE via ZQ.PubSubQueue[]
function handle_cluster_subscriber_merge(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  // NOTE: Neither AgentMaster nor KeyMaster -> SKIP cnode check
  var ks      = params.data.ks;
  ZH.t('<-|(I): ClusterSubscriberMerge: K: ' + ks.kqk);
  var cavrsns = params.data.central_agent_versions;
  var crdt    = params.data.crdt;
  var gcsumms = params.data.gc_summary;
  var ether   = params.data.ether;
  var remove  = params.data.remove;
  ZCSub.HandleClusterSubscriberMerge(net, ks, crdt, cavrsns,
                                     gcsumms, ether, remove);
}

function flow_handle_storage_ack_geo_need_merge(plugin, collections, qe, next) {
  var tn       = T.G();
  var net      = qe.net;
  var qnext    = qe.next;
  var qhres    = qe.hres;
  var ks       = qe.ks;
  var data     = qe.data;
  var crdt     = data.crdt;
  var cavrsns  = data.cavrsns;
  var gcsumms  = data.gcsumms;
  var ether    = data.ether;
  var remove   = data.remove;
  ZH.t('<-|(I): StorageAckGeoNeedMerge: K: ' + ks.kqk);
  ZNM.FlowHandleStorageAckGeoNeedMerge(net, ks, crdt, cavrsns,
                                       gcsumms, ether, remove,
  function(perr, pres) {
    T.X(tn);
    qnext(perr, qhres);
    next (null, null);
  });
}

function do_flow_handle_need_merge(plugin, collections,
                                   qe, is_geo, hres, next) {
  var net    = qe.net;
  var auth   = qe.auth;
  var ks     = qe.ks;
  var data   = qe.data;
  var device = data.device;
  if (is_geo) {
    var guuid = data.datacenter;
    ZH.t('<-|(I): StorageGeoNeedMerge: U: ' + guuid + ' K: ' + ks.kqk);
    ZNM.FlowHandleStorageGeoNeedMerge(net, ks, guuid, device, hres, next);
  } else {
    var gcv   = data.gc_version;
    var rid   = data.request_id;
    var agent = data.agent;
    ZH.t('<-|(I): StorageNeedMerge: U: ' + agent.uuid + ' K: ' + ks.kqk);
    ZNM.FlowHandleStorageNeedMerge(net, ks, gcv, rid, agent, hres, next);
  }
}

function flow_handle_need_merge(plugin, collections, qe, is_geo, next) {
  var tn    = T.G();
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  do_flow_handle_need_merge(plugin, collections, qe, is_geo, qhres,
  function(serr, sres) {
    T.X(tn);
    qnext(serr, qhres);
    next (null, null);
  });
}

function flow_handle_storage_need_merge(plugin, collections, qe, next) {
  flow_handle_need_merge(plugin, collections, qe, false, next);
}

function flow_handle_storage_geo_need_merge(plugin, collections, qe, next) { 
  flow_handle_need_merge(plugin, collections, qe, true, next);
}

function handle_external_need_merge(conn, params, id, wid, net, is_geo, next) {
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device = params.data.device;
  var cnode  = ZPart.GetClusterNode(device.uuid);
  if (!cnode) {
    if (is_geo) {
      return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
    } else {
      return ZCloud.SendBackoff(device.uuid, hres);
    }
  }
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var ks     = params.data.ks;
  var mname  = is_geo ? 'GeoNeedMerge' : 'AgentNeedMerge';
  ZH.t('<-|(A): ' + mname + ': K: ' + ks.kqk);
  if (is_geo) { // GEO_NEED_MERGE
    var guuid = params.data.datacenter;
    ZDQ.StorageQueueRequestGeoNeedMerge(net.plugin, net.collections,
                                        ks, guuid, auth, hres, next);
  } else {      // AGENT_NEED_MERGE
    var gcv   = params.data.gc_version;
    var agent = params.data.agent;
    ZAuth.HasSecurityTokenPermissions(net, ks, function(aerr, ok) {
      if (aerr) next(aerr, hres);
      else {
        if (!ok) next(new Error(ZS.Errors.ReadPermsFail), hres);
        else {
          // NOTE: BLOCKING REQUEST OK, because call is NOT in KQK-QUEUE
          ZDQ.StorageQueueRequestAgentNeedMerge(net.plugin, net.collections,
                                                id, ks, gcv, agent,
                                                auth, hres, next);
        }
      }
    });
  }
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_agent_need_merge(conn, params, id, wid, net, next) {
  if (ZH.ChaosMode === 12) return ZCloud.ChaosDropResponse(12);           T.G();
  handle_external_need_merge(conn, params, id, wid, net, false, next);    T.X();
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_geo_need_merge(conn, params, id, wid, net, next) {
  if (ZH.ChaosMode === 11) return ZCloud.ChaosDropResponse(11);
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;          T.G();
  handle_external_need_merge(conn, params, id, wid, net, true, next);     T.X();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CACHE ---------------------------------------------------------------------

function do_flow_handle_storage_cluster_cache(plugin, collections,
                                              qe, is_evict, is_local,
                                              hres, next) {
  var net   = qe.net;
  var ks    = qe.ks;
  var auth  = qe.auth;
  var data  = qe.data;
  var agent = data.agent;
  var mname = is_local ? "StorageClusterLocalEvict" :
              is_evict ? "StorageClusterEvict"      :
                         "StorageClusterCache" ;
  ZH.t('<-|(R): ' + mname + ': U: ' + agent.uuid + ' K: ' + ks.kqk);
  if (is_local) {
    ZCache.HandleStorageClusterLocalEvict(net, ks, agent, hres, next);
  } else if (is_evict) {
    ZCache.HandleStorageClusterEvict(net, ks, agent, hres, next);
  } else {
    var need_body = data.need_body;
    var watch     = data.watch;
    ZCache.HandleStorageClusterCache(net, ks, watch, agent, need_body,
                                     auth, hres, next);
  }
}

function __flow_handle_storage_cluster_cache(plugin, collections,
                                             qe, is_evict, is_local, next) { 
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  do_flow_handle_storage_cluster_cache(plugin, collections,
                                       qe, is_evict, is_local, qhres,
  function(serr, sres) {
    qnext(serr, qhres);
    next (null, null);
  });
}

function flow_handle_storage_cluster_cache(plugin, collections, qe, next) { 
  __flow_handle_storage_cluster_cache(plugin, collections,
                                      qe, false, false, next);
}

function flow_handle_storage_cluster_evict(plugin, collections, qe, next) { 
  __flow_handle_storage_cluster_cache(plugin, collections, qe,
                                      true, false, next);
}

function flow_handle_storage_cluster_local_evict(plugin, collections,
                                                 qe, next) { 
  __flow_handle_storage_cluster_cache(plugin, collections,
                                      qe, true, true, next);
}

function do_cache(net, ks, watch, agent, is_geo, auth, hres, next) {
  if (is_geo) {
    ZCache.CentralHandleGeoClusterCache(net, ks, watch, agent, hres, next);
  } else {
    ZCache.CentralHandleClusterCache(net, ks, watch, agent, auth, hres, next);
  }
}

function flow_handle_cache(plugin, collections, qe, is_geo, next) {
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  var auth  = qe.auth;
  var ks    = qe.ks;
  var data  = qe.data;
  var watch = data.watch;
  var agent = data.agent;
  var duuid = agent.uuid;
  var mname = is_geo ? 'ClusterGeoCache' : 'ClusterCache';
  ZH.t('<-|(I): ' + mname + ': K: ' + ks.kqk + ' W: ' + watch);
  if (is_geo) { // NOTE: GEO is pre-AUTHed
    do_cache(net, ks, watch, agent, is_geo, auth, qhres, function(perr, pres) {
      qnext(perr, qhres);
      next (null, null);
    });
  } else {
    ZAuth.HasCentralCachePermissions(net, auth, ks, duuid, function(aerr, ok) {
      if (aerr || !ok) {
        if (aerr) qnext(aerr, qhres);
        else      qnext(new Error(ZS.Errors.ReadPermsFail), qhres);
        next(null, null);
      } else {
        do_cache(net, ks, watch, agent, is_geo, auth, qhres,
        function(perr, pres) {
          qnext(perr, qhres);
          next (null, null);
        });
      }
    });
  }
}

function flow_handle_cluster_cache(plugin, collections, qe, next) { 
  flow_handle_cache(plugin, collections, qe, false, next);
}

function flow_handle_geo_cluster_cache(plugin, collections, qe, next) { 
  flow_handle_cache(plugin, collections, qe, true, next);
}

function handle_internal_cache(conn, params, id, wid, net, is_geo, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks     = params.data.ks;
  var cnode  = ZPart.GetKeyNode(ks);
  if (!cnode || cnode.device_uuid !== ZH.MyUUID) {
    return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  }
  var watch  = params.data.watch;
  var agent  = params.data.agent;
  var data   = {watch : watch, agent : agent};
  ZQ.AddToKeySerializationFlow(ZQ.RouterKeySerializationQueue, ks,
                               is_geo ? 'GEO_CACHE' : 'CLUSTER_CACHE',
                               net, data, auth, hres, next);
  var flow = {k : ks.kqk,
              q : ZQ.RouterKeySerializationQueue,
              m : ZQ.RouterKeySerialization,
              f : flow_central_key_serialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function handle_cluster_cache(conn, params, id, wid, net, next) {
  handle_internal_cache(conn, params, id, wid, net, false, next);
}

function handle_geo_cluster_cache(conn, params, id, wid, net, next) {
  handle_internal_cache(conn, params, id, wid, net, true, next);
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_external_cache(conn, params, id, wid, net, is_geo, next) {
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device = params.data.device;
  var cnode  = ZPart.GetClusterNode(device.uuid);
  if (!cnode) {
    if (is_geo) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
    else        return ZCloud.SendBackoff(device.uuid, hres);
  }
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var ks     = params.data.ks;
  var watch  = params.data.watch;
  var agent  = params.data.agent;
  var mname  = is_geo ? 'GeoBroadcastCache' : 'AgentCache';
  ZH.t('<-|(A): ' + mname + ': K: ' + ks.kqk);
  if (is_geo) {
    ZPio.SendClusterGeoCache(net.plugin, net.collections,
                             ks, watch, agent, auth, hres, next);
  } else {
    ZPio.SendClusterCache(net.plugin, net.collections,
                          ks, watch, agent, auth, hres, next);
  }
}
function handle_agent_cache(conn, params, id, wid, net, next) {
  handle_external_cache(conn, params, id, wid, net, false, next);
}
function handle_geo_cache(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_external_cache(conn, params, id, wid, net, true, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EVICT ---------------------------------------------------------------------

function do_evict(net, ks, agent, is_geo, is_local, hres, next) {
  if (is_local) {
    ZCache.CentralHandleGeoClusterLocalEvict(net, ks, agent, hres, next);
  } else if (is_geo) {
    ZCache.CentralHandleGeoClusterEvict     (net, ks, agent, hres, next);
  } else {
    ZCache.CentralHandleClusterEvict        (net, ks, agent, hres, next);
  }
}

function flow_handle_evict(plugin, collections, qe, is_geo, is_local, next) {
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  var auth  = qe.auth;
  var ks    = qe.ks;
  var data  = qe.data;
  var agent = data.agent;
  var duuid = agent.uuid;
  var mname = is_local ? 'ClusterGeoLocalEvict' :
              is_geo   ? 'ClusterGeoEvict'      :
                         'ClusterEvict';
  ZH.t('<-|(I): ' + mname + ': K: ' + ks.kqk);
  // NOTE: CENTRAL DOES NOT USER-AUTHORIZE EVICTS
  do_evict(net, ks, agent, is_geo, is_local, qhres, function(perr, pres) {
    qnext(perr, qhres);
    next (null, null);
  });
}

function flow_handle_cluster_evict(plugin, collections, qe, next) { 
  flow_handle_evict(plugin, collections, qe, false, false, next);
}

function flow_handle_geo_cluster_evict(plugin, collections, qe, next) { 
  flow_handle_evict(plugin, collections, qe, true, false, next);
}

function flow_handle_geo_cluster_local_evict(plugin, collections, qe, next) { 
  flow_handle_evict(plugin, collections, qe, true, true, next);
}

function handle_internal_evict(conn, params, id, wid, net,
                               is_geo, is_local, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks     = params.data.ks;
  var cnode  = ZPart.GetKeyNode(ks);
  if (!cnode || cnode.device_uuid !== ZH.MyUUID) {
    return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  }
  var agent  = params.data.agent;
  var data   = {agent : agent};
  var fname  = is_local ? 'GEO_CLUSTER_LOCAL_EVICT' :
               is_geo   ? 'GEO_CLUSTER_EVICT'       :
                          'CLUSTER_EVICT';
  ZQ.AddToKeySerializationFlow(ZQ.RouterKeySerializationQueue, ks,
                               fname, net, data, auth, hres, next);
  var flow = {k : ks.kqk,
              q : ZQ.RouterKeySerializationQueue,
              m : ZQ.RouterKeySerialization,
              f : flow_central_key_serialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function handle_cluster_evict(conn, params, id, wid, net, next) {
  handle_internal_evict(conn, params, id, wid, net, false, false, next);
}

function handle_geo_cluster_evict(conn, params, id, wid, net, next) {
  handle_internal_evict(conn, params, id, wid, net, true, false, next);
}

function handle_geo_cluster_local_evict(conn, params, id, wid, net, next) {
  handle_internal_evict(conn, params, id, wid, net, true, true, next);
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_external_evict(conn, params, id, wid, net,
                               is_geo, is_local, next) {
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device = params.data.device;
  var cnode  = ZPart.GetClusterNode(device.uuid);
  if (!cnode) {
    if (is_geo) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
    else        return ZCloud.SendBackoff(device.uuid, hres);
  }
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var ks     = params.data.ks;
  var agent  = params.data.agent;
  var nm     = params.data.need_merge; // NOTE: echoed back to Agent
  var mname  = is_geo ? 'GeoBroadcastEvict' : 'AgentEvict';
  ZH.t('<-|(A): ' + mname + ': K: ' + ks.kqk);
  if (is_local) { // LOCAL-EVICT
    if (is_geo) {
      ZPio.SendClusterGeoLocalEvict(net.plugin, net.collections,
                                    ks, agent, auth, hres, next);
    } else { // NOTE: NO ClusterLocalDelta -> straight to GeoLocalDelta
      // NOTE: ZPio.GeoBroadcastLocalEvict() is ASYNC
      ZPio.GeoBroadcastLocalEvict(net.plugin, net.collections, ks, agent);
      hres.ks = ks; // Used in response
      next(null, hres);
    }
  } else {        // EVICT
    if (is_geo) {
      ZPio.SendClusterGeoEvict(net.plugin, net.collections,
                               ks, agent, auth, hres, next);
    } else {
      hres.ks         = ks; // Used in response
      hres.need_merge = nm; // Used in response
      ZPio.SendClusterEvict(net.plugin, net.collections,
                            ks, agent, auth, hres, next);
    }
  }
}

function handle_agent_evict(conn, params, id, wid, net, next) {
  handle_external_evict(conn, params, id, wid, net, false, false, next);
}

function handle_geo_evict(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_external_evict(conn, params, id, wid, net, true, false, next);
}

function handle_geo_local_evict(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_external_evict(conn, params, id, wid, net, true, true, next);
}

function handle_agent_local_evict(conn, params, id, wid, net, next) {
  handle_external_evict(conn, params, id, wid, net, false, true, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER-MAP METHODS ----------------------------------------------------

function handle_broadcast_update_subscriber_map(conn, params, id,
                                                wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var ks       = params.data.ks;
  var rchans   = params.data.replication_channels;
  var username = params.data.username;
  var agent    = params.data.agent;
  var watch    = params.data.watch;
  var is_cache = params.data.is_cache;
  ZH.t('<-|(I): BroadcastUpdateSubscriberMap: K: '  + ks.kqk);
  ZPub.HandleUpdateSubscriberMap(net, ks, rchans, username, agent, watch,
                                 is_cache, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL AGENT HANDLERS - (CHANNEL BASED) ----------------------------------

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_broadcast_subscribe(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var schanid = params.data.channel.id;
  var perms   = params.data.channel.permissions;
  ZH.t('<-|(I): BroadcastSubscribe: R: '  + schanid);
  ZChannel.CentralHandleBroadcastSubscribe(net.plugin, net.collections,
                                           schanid, perms, auth, hres, next);
}

function do_subscribe(net, auuid, schanid, is_geo, auth, hres, next) {
  if (is_geo) {
    ZChannel.CentralHandleGeoSubscribe(net.plugin, net.collections,
                                       auuid, schanid, auth, hres, next);
  } else {
    ZChannel.CentralHandleAgentSubscribe(net.plugin, net.collections,
                                         auuid, schanid, auth, hres, next);
  }
}

function handle_subscribe(conn, params, id, wid, net, is_geo, next) {
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device  = params.data.device;
  var cnode   = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var auuid   = params.data.agent.uuid;
  var schanid = params.data.channel.id;
  var mname   = is_geo ? 'GeoBroadcastSubscribe' : 'AgentSubscribe';
  ZH.t('<-|(A): ' + mname + ': U: ' + auuid + ' R: ' + schanid);
  if (is_geo) { // NOTE: GEO is pre-AUTHed
    do_subscribe(net, auuid, schanid, is_geo, auth, hres, next);
  } else {
    ZAuth.HasCentralSubscribePermissions(net, auth, schanid,
    function(aerr, ok) {
      if      (aerr) next(aerr, hres);
      else if (!ok)  next(new Error(ZS.Errors.SubscribePermsFail), hres);
      else {
        do_subscribe(net, auuid, schanid, is_geo, auth, hres, next);
      }
    });
  }
}

function handle_agent_subscribe(conn, params, id, wid, net, next) {
  handle_subscribe(conn, params, id, wid, net, false, next);
}

function handle_geo_subscribe(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_subscribe(conn, params, id, wid, net, true, next);
}

function handle_agent_has_subscribe_permissions(conn, params,
                                                id, wid, net, next) {
  var is_geo  = false;
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device  = params.data.device;
  var cnode   = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var schanid = params.data.channel.id;
  ZH.t('<-|(A): AgentHasSubscribePermissions: UN: ' + auth.username +
       ' R: ' + schanid);
  ZAuth.HandleCentralHasSubscriberPermissions(net.plugin, net.collections,
                                              auth, schanid, hres, next);
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_broadcast_unsubscribe(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var schanid = params.data.channel.id;
  var perms   = params.data.channel.permissions;
  ZH.t('<-|(I): BroadcastUnsubscribe: R: '  + schanid);
  ZChannel.CentralHandleBroadcastUnsubscribe(net.plugin, net.collections,
                                             schanid, auth, hres, next);
}

function do_unsubscribe(net, uuid, schanid, is_geo, auth, hres, next) {
  if (is_geo) {
    ZChannel.CentralHandleGeoUnsubscribe(net.plugin, net.collections,
                                         uuid, schanid, auth, hres, next);
  } else {
    ZChannel.CentralHandleAgentUnsubscribe(net.plugin, net.collections,
                                           uuid, schanid, auth, hres, next);
  }
}

function handle_unsubscribe(conn, params, id, wid, net, is_geo, next) {
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device  = params.data.device;
  var cnode   = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var uuid    = params.data.agent.uuid;
  var schanid = params.data.channel.id;
  var mname   = is_geo ? 'GeoBroadcastUnsubscribe' : 'AgentUnsubscribe';
  ZH.t('<-|(A): ' + mname + ': U: ' + uuid + ' R: ' + schanid);
  var adata   = {schanid : schanid, auth : auth}
  if (is_geo) { // NOTE: GEO is pre-AUTHed
    do_unsubscribe(net, uuid, schanid, is_geo, auth, hres, next);
  } else {
    ZAuth.HasCentralSubscribePermissions(net, auth, schanid,
    function(aerr, ok) {
      if      (aerr) next(aerr, hres);
      else if (!ok)  next(new Error(ZS.Errors.SubscribePermsFail), hres);
      else {
        do_unsubscribe(net, uuid, schanid, is_geo, auth, hres, next);
      }
    });
  }
}

function handle_agent_unsubscribe(conn, params, id, wid, net, next) {
  handle_unsubscribe(conn, params, id, wid, net, false, next);
}

function handle_geo_unsubscribe(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_unsubscribe(conn, params, id, wid, net, true, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL AGENT HANDLERS - (DEVICE BASED) -----------------------------------

function handle_station_user(conn, params, id, wid, net, is_geo, next) {
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device  = params.data.device;
  var cnode   = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var mname   = is_geo ? 'GeoBroadcastStationUser' : 'AgentStationUser';
  ZH.t('<-|(A): ' + mname + ': UN: ' + auth.username);
  var auuid   = params.data.agent.uuid;
  if (is_geo) {
    ZSU.CentralHandleGeoStationUser(net.plugin, net.collections,
                                    auuid, auth, hres, next);
  } else {
    ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
      if      (aerr) next(aerr, hres);
      else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
      else {
        ZSU.CentralHandleAgentStationUser(net.plugin, net.collections,
                                          auuid, auth, hres, next);
      }
    });
  }
}

function handle_geo_station_user(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_station_user(conn, params, id, wid, net, true, next);
}

function handle_agent_station_user(conn, params, id, wid, net, next) {
  handle_station_user(conn, params, id, wid, net, false, next);
}

function handle_destation_user(conn, params, id, wid, net, is_geo) {
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var next    = is_geo ? geo_destation_user_processed :
                         agent_destation_user_processed;
  var device  = params.data.device;
  var cnode   = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var mname  = is_geo ? 'GeoBroadcastDestationUser' : 'AgentDestationUser';
  ZH.t('<-|(A): ' + mname + ': UN: ' + auth.username);
  var auuid   = params.data.agent.uuid;
  if (is_geo) {
    ZSU.CentralHandleGeoDestationUser(net.plugin, net.collections,
                                      auuid, auth, hres, next);
  } else {
    ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
      if      (aerr) next(aerr, hres);
      else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
      else {
        ZSU.CentralHandleAgentDestationUser(net.plugin, net.collections,
                                            auuid, auth, hres, next);
      }
    });
  }
}

function handle_geo_destation_user(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_destation_user(conn, params, id, wid, net, true);
}

function handle_agent_destation_user(conn, params, id, wid, net) {
  handle_destation_user(conn, params, id, wid, net, false);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLERS - READ OPERATIONS ----------------------------------------

function handle_agent_authenticate(conn, params, id, wid, net, next) {
  var is_geo = false;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  ZH.t('<-|(A): AgentAuthenticate: UN: ' + auth.username);
  ZAuth.HandleCentralAgentAuthenticate(net, auth, hres, next);
}

function handle_agent_get_user_subscriptions(conn, params, id, wid, net, next) {
  var is_geo = false;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  ZH.t('<-|(A): AgentGetUserSubscriptions: UN: ' + auth.username);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZChannel.CentralHandleGetUserSubscriptions(net.plugin, net.collections,
                                                 auth, hres, next);
    }
  });
}

function handle_agent_get_user_info(conn, params, id, wid, net, next) {
  var is_geo = false;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  ZH.t('<-|(A): AgentGetUserSubscriptions: UN: ' + auth.username);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZChannel.CentralHandleGetUserInfo(net, auth, hres, next);
    }
  });
}

function handle_agent_get_user_channel_permissions(conn, params,
                                                   id, wid, net, next) {
  var is_geo = false;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var rchans = params.data.replication_channels;
  ZH.t('<-|(A): AgentGetUserChannelPermissions: UN: ' + auth.username);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZUM.CentralHandleGetUserChannelPermissions(net.plugin, net.collections,
                                                 rchans, auth, hres, next);
    }
  });
}

function handle_external_find(conn, params, id, wid, net, next) {
  var is_geo = false;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ns     = params.data.namespace;
  var cn     = params.data.collection;
  var query  = params.data.query;
  ZH.t('<-|(E): Find: Q: ' + query);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZNM.CentralHandleFind(net.plugin, net.collections,
                            ns, cn, query, auth, hres, next);
    }
  });
}

function handle_agent_get_cluster_info(conn, params, id, wid, net, next) {
  var is_geo = false;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  ZH.t('<-|(A): AgentGetClusterInfo: UN: ' + auth.username);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), hres);
    else {
      ZCLS.CentralHandleGetClusterInfo(net.plugin, net.collections, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN HANDLERS - (USER-BASED (NOTE: ADMIN ROLE REQUIRED)) -----------------

function handle_admin_authenticate(conn, params, id, wid, net, next) {
  var is_geo = false;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  ZH.t('<-|(A): AdminAuthenticate: UN: ' + auth.username);
  ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
    if (aerr) next(aerr, hres);
    else { // NOTE: if (!ok) check NOT needed
      hres.ok = ok; // Used in response
      next(null, hres);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN HANDLERS - GLOBAL (NOTE: ADMIN ROLE REQUIRED) -----------------------

function handle_add_user(conn, params, id, wid, net, is_geo, next) {
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device   = params.data.device;
  var cnode    = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var username = params.data.username;
  var password = params.data.password;
  var role     = params.data.role;
  if (role !== 'ADMIN' && role !== 'USER') {
    return next(new Error(ZS.Errors.BadUserRole), hres);
  }
  var mname    = is_geo ? 'GeoBroadcastAddUser' : 'AdminAddUser';
  ZH.t('<-|(A): ' + mname + ': UN: ' + username);
  if (is_geo) { // NOTE: GEO is pre-AUTHed
    ZUM.CentralHandleGeoAddUser(net.plugin, net.collections,
                                username, password, role, false, hres, next);
  } else {
    ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
      if      (aerr) next(aerr, hres);
      else if (!ok)  next(new Error(ZS.Errors.AdminAuthFail), hres);
      else {
        //TODO check_valid_username(username)
        ZUM.CentralHandleAdminAddUser(net.plugin, net.collections,
                                      username, password, role, hres, next);
      }
    });
  }
}

function handle_admin_add_user(conn, params, id, wid, net, next) {
  handle_add_user(conn, params, id, wid, net, false, next);
}

function handle_geo_add_user(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_add_user(conn, params, id, wid, net, true, next);
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_broadcast_remove_user(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth = params.authentication;
  var hres = ZCloud.CreateHres(conn, params, id, wid, auth);
  var subs = params.data.subscribers;
  ZH.t('<-|(I): BroadcastRemoveUser: UN: ' + auth.username);
  ZUM.CentralHandleBroadcastRemoveUser(net.plugin, net.collections,
                                       subs, auth, hres, next);
}

function handle_remove_user(conn, params, id, wid, net, is_geo, next) {
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device   = params.data.device;
  var cnode    = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var username = params.data.username;
  var mname    = is_geo ? 'GeoBroadcastRemoveUser' : 'AdminRemoveUser';
  ZH.t('<-|(A): ' + mname + ': UN: ' + username);
  if (is_geo) { // NOTE: GEO is pre-AUTHed
    ZUM.CentralHandleGeoRemoveUser(net.plugin, net.collections, username,
                                   hres, next);
  } else {
    ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
      if      (aerr) next(aerr, hres);
      else if (!ok)  next(new Error(ZS.Errors.AdminAuthFail), hres);
      else {
        ZUM.CentralHandleRemoveUser(net.plugin, net.collections, username,
                                    hres, next);
      }
    });
  }
}

function handle_admin_remove_user(conn, params, id, wid, net, next) {
  handle_remove_user(conn, params, id, wid, net, false, next);
}

function handle_geo_remove_user(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_remove_user(conn, params, id, wid, net, true, next);
}

// NOTE: EVENT-FREE code path -> NO EVENTS -> NO QUEUEing
function handle_broadcast_grant_user(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var schanid  = params.data.channel.id;
  var priv     = params.data.channel.privilege;
  var subs     = params.data.subscribers;
  var do_unsub = params.data.do_unsubscribe;
  ZH.t('<-|(I): BroadcastGrantUser: UN: ' + auth.username + ' R: ' + schanid +
       ' P: ' + priv);
  ZUM.CentralHandleBroadcastGrantUser(net.plugin, net.collections,
                                      schanid, priv, do_unsub, subs,
                                      auth, hres, next);
}

function handle_grant_user(conn, params, id, wid, net, is_geo, next) {
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device   = params.data.device;
  var cnode    = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var username = params.data.username;
  var schanid  = params.data.channel.id;
  var priv     = params.data.channel.privilege;
  var mname    = is_geo ? 'GeoBroadcastGrantUser' : 'AdminGrantUser';
  ZH.t('<-|(A): ' + mname + ': UN: ' + username + ' R: ' + schanid +
       ' P: ' + priv);
  if (is_geo) { // NOTE: GEO is pre-AUTHed
    var do_unsub = params.data.do_unsubscribe;
    ZUM.CentralHandleGeoGrantUser(net.plugin, net.collections,
                                  username, schanid, priv, do_unsub, 
                                  hres, next);
  } else {
    ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
      if      (aerr) next(aerr, hres);
      else if (!ok)  next(new Error(ZS.Errors.AdminAuthFail), hres);
      else {
        ZUM.CentralHandleGrantUser(net.plugin, net.collections,
                                   username, schanid, priv, hres, next);
      }
    });
  }
}

function handle_admin_grant_user(conn, params, id, wid, net, next) {
  handle_grant_user(conn, params, id, wid, net, false, next);
}

function handle_geo_grant_user(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_grant_user(conn, params, id, wid, net, true, next);
}

function handle_remove_dc(conn, params, id, wid, net, is_geo, next) {
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device = params.data.device;
  var cnode  = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var dcuuid = params.data.remove_datacenter;
  var mname  = is_geo ? 'GeoRemoveDataCenter' : 'AdminRemoveDataCenter';
  ZH.t('<-|(A): ' + mname + ': DC: ' + dcuuid);
  if (is_geo) { // NOTE: GEO is pre-AUTHed
    ZCLS.CentralHandleGeoRemoveDataCenter(net, dcuuid, next);
  } else {
    ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
      if      (aerr) next(aerr, hres);
      else if (!ok)  next(new Error(ZS.Errors.AdminAuthFail), hres);
      else {
        // NOTE: ZPio.GeoBroadcastRemoveDataCenter() is ASYNC
        ZPio.GeoBroadcastRemoveDataCenter(net.plugin, net.collections, dcuuid);
        next(null, hres);
      }
    });
  }
}

function handle_admin_remove_dc(conn, params, id, wid, net, next) {
  handle_remove_dc(conn, params, id, wid, net, false, next);
}

function handle_geo_remove_dc(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  handle_remove_dc(conn, params, id, wid, net, true, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLUSTER VOTE HANDLERS -----------------------------------------------------

function handle_announce_new_cluster(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var cnode  = ZCLS.GetClusterLeaderNode();
  if (!cnode) return;
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_cluster_leader(hres);
  }
  var guuid  = params.data.datacenter;
  if (guuid === ZH.MyDataCenter) return; // IGNORE from SELF
  var cnodes = params.data.cluster_nodes;
  ZH.t('<-|(G): AnnounceNewCluster: GU: ' + guuid);
  ZCLS.HandleAnnounceNewCluster(net.plugin, net.collections,
                                guuid, cnodes, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO VOTE HANDLERS (NOTE: GEO-LEADER OPS) ----------------------------------

function handle_geo_state_change_vote(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var is_geo   = true;
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var next     = geo_state_change_vote_processed;
  var cnode    = ZCLS.GetClusterLeaderNode();
  if (!cnode) return;
  if (cnode.device_uuid !== ZH.MyUUID) return redirect_cluster_leader(hres);
  var term     = params.data.term_number;
  var vid      = params.data.vote_id;
  ZH.t('<-|(G): GeoStateChangeVote: ' + term + ' V: ' + vid);
  var gnodes   = params.data.geo_nodes;
  var gnetpart = params.data.geo_network_partition;
  var gmaj     = params.data.geo_majority;
  ZVote.HandleGeoStateChangeVote(net.plugin, net.collections,
                                 term, vid, gnodes, gnetpart, gmaj, hres, next);
}

function handle_geo_state_change_commit(conn, params, id, wid, net) {
  if (ZH.ChaosMode === 7) return ZCloud.ChaosDropResponse(7);
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var is_geo   = true;
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var next     = geo_state_change_commit_processed;
  var cnode    = ZCLS.GetClusterLeaderNode();
  if (!cnode) return;
  if (cnode.device_uuid !== ZH.MyUUID) return redirect_cluster_leader(hres);
  var term     = params.data.term_number;
  var vid      = params.data.vote_id;
  ZH.t('<-|(G): GeoStateChangeCommit: T: ' + term + ' V: ' + vid);
  var gnodes   = params.data.geo_nodes;
  var gnetpart = params.data.geo_network_partition;
  var gmaj     = params.data.geo_majority;
  ZVote.HandleGeoStateChangeCommit(net, term, vid, gnodes, gnetpart, gmaj,
                                   hres, next);
}

function handle_announce_new_geo_cluster(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var duuid    = params.data.device.uuid;
  var gterm    = params.data.geo_term_number;
  var gnodes   = params.data.geo_nodes;
  ZH.t('<-|(I): BroadcastAnnounceNewGeoCluster GT: ' + gterm +
       ' #DC: ' + gnodes.length);
  var gnetpart = params.data.geo_network_partition;
  var gmaj     = params.data.geo_majority;
  var csynced  = params.data.cluster_synced;
  var send_dco = params.data.send_datacenter_online;
  ZCLS.HandleAnnounceNewGeoCluster(net, duuid, gterm, gnodes, gnetpart,
                                   gmaj, csynced, send_dco, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATACENTER ONLINE HANDLERS ------------------------------------------------

function handle_geo_datacenter_online(conn, params, id, wid, net, next) {
  if (ZH.ChaosMode === 16) return ZCloud.ChaosDropResponse(16);
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var is_geo = true;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device = params.data.device;
  var cnode  = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }                                                                       T.G();
  var guuid  = params.data.datacenter;
  var dkeys  = params.data.device_keys;
  ZH.t('<-|(G): GeoDataCenterOnline: GU: ' + guuid);
  ZDConn.CentralHandleGeoDataCenterOnline(net, guuid, dkeys, hres, next); T.X();
}

function handle_geo_broadcast_device_keys(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var is_geo = true;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var device = params.data.device;
  var cnode  = ZPart.GetClusterNode(device.uuid);
  if (!cnode) return next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
  if (cnode.device_uuid !== ZH.MyUUID) {
    return redirect_agent_master(is_geo, cnode, hres);
  }
  var guuid  = params.data.datacenter;
  var dkeys  = params.data.device_keys;
  ZH.t('<-|(G): GeoBroadcastDeviceKeys: GU: ' + guuid);
  ZDConn.CentralHandleGeoBroadcastDeviceKeys(net, dkeys, hres, next);
}

function handle_cluster_status(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var csynced = params.data.cluster_synced;
  ZH.t('<-|(I): BroadcastClusterStatus: SYNC: ' + csynced);
  ZCLS.HandleClusterStatus(net.plugin, net.collections, csynced, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DISCOVERY -----------------------------------------------------------------

function handle_datacenter_discovery(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var cnode  = ZCLS.GetClusterLeaderNode();
  if (!cnode) return redirect_discovery(hres);
  if (cnode.device_uuid !== ZH.MyUUID) return redirect_cluster_leader(hres);
  var guuid  = params.data.datacenter;
  ZH.t('<-|(G): DataCenterDiscovery: GU: ' + guuid);
  var dgnode = params.data.geo_node;
  ZCLS.HandleDataCenterDiscovery(net.plugin, net.collections,
                                 guuid, dgnode, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// APP-SERVER-CLUSTER --------------------------------------------------------

function handle_announce_new_app_server_cluster(conn, params,
                                                id, wid, net, next) {
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var clname = params.data.cluster_name;
  var mcname = params.data.memcache_cluster_name;
  var cnodes = params.data.cluster_nodes;
  ZH.t('<-|(A): AnnounceNewAppServerCluster: N: ' + clname); //ZH.e(cnodes);
  ZASC.CentralHandleAnnounceNewAppServerCluster(net, clname, mcname, cnodes,
                                                hres, next);
}

function handle_cluster_subscriber_message(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var suuid = params.data.subscriber;
  ZH.t('<-|(I): ClusterSubscriberMessage: U: ' + suuid);
  var msg   = params.data.message;
  ZCSub.HandleClusterSubscriberMessage(net, suuid, msg);
}

function handle_cluster_user_message(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var username = params.data.username;
  ZH.t('<-|(I): ClusterUserMessage: UN: ' + username);
  var msg      = params.data.message;
  ZCSub.HandleClusterUserMessage(net, username, msg, next);
}

function handle_geo_message(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth = params.authentication;
  var hres = ZCloud.CreateHres(conn, params, id, wid, auth);
  var msg  = params.data.message;
  ZH.t('<-|(G): GeoMessage');
  ZMessage.CentralHandleGeoMessage(net, msg, hres, next);
}

function handle_agent_message(conn, params, id, wid, net, next) {
  var auth   = params.authentication;
  var device = params.data.device;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var body   = params.data.body;
  ZH.t('<-|(A): AgentMessage');
  ZMessage.CentralHandleAgentMessage(net, body, device, auth, hres, next);
}

function handle_agent_request(conn, params, id, wid, net, next) {
  var auth   = params.authentication;
  var device = params.data.device;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var body   = params.data.body;
  ZH.t('<-|(A): AgentRequest');
  ZMessage.CentralHandleAgentRequest(net, body, device, auth, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MEMCACHE-CLUSTER ----------------------------------------------------------

function handle_broadcast_announce_new_memcache_cluster(conn, params,
                                                        id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var duuid = params.data.device.uuid;
  var state = params.data.cluster_state;
  ZH.t('<-|(I): BroadcastAnnounceNewMemcacheCluster');
  ZCSub.HandleBroadcastAnnounceNewMemcacheCluster(net, state, next);
}

function handle_announce_new_memcache_cluster(conn, params,
                                              id, wid, net, next) {
  var auth    = params.authentication;
  var hres    = ZCloud.CreateHres(conn, params, id, wid, auth);
  var clname  = params.data.cluster_name;
  var ascname = params.data.app_server_cluster_name;
  var cnodes  = params.data.cluster_nodes;
  ZH.t('<-|(A): AnnounceNewMemcacheCluster: N: ' + clname); //ZH.e(cnodes);
  ZMCC.CentralHandleAnnounceNewMemcacheCluster(net, clname, ascname, cnodes,
                                               hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DIRECT CLIENT METHODS -----------------------------------------------------

function handle_cluster_client_store(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks     = params.data.ks;
  var json   = params.data.json;
  var rchans = json._channels;
  var duuid  = -1;
  ZAuth.HasWritePermissions(net, auth, ks, duuid, rchans, function(aerr, ok) {
    if (aerr) next(aerr, hres);
    else {
      if (!ok) next(new Error(ZS.Errors.WritePermsFail), hres);
      else {
        ZDirect.CentralHandleClusterClientStore(net, ks, json, hres, next);
      }
    }
  });
}

function handle_client_store(conn, params, id, wid, net, next) {
  var auth = params.authentication;
  var hres = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ns   = params.data.namespace;
  var cn   = params.data.collection;
  var key  = params.data.key;
  var json = params.data.json;
  var ks   = ZH.CompositeQueueKey(ns, cn, key);
  ZPio.SendClusterClientStore(net.plugin, net.collections,
                              ks, json, auth, hres, next);
}

function handle_cluster_client_fetch(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks     = params.data.ks;
  var rchans = json._channels;
  var duuid  = -1;
  ZAuth.HasReadPermissions(net, auth, ks, duuid, function(aerr, ok) {
    if (aerr) next(aerr, hres);
    else {
      if (!ok) next(new Error(ZS.Errors.WritePermsFail), hres);
      else {
        ZDirect.CentralHandleClusterClientFetch(net, ks, hres, next);
      }
    }
  });
}

function handle_client_fetch(conn, params, id, wid, net, next) {
  var auth = params.authentication;
  var hres = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ns   = params.data.namespace;
  var cn   = params.data.collection;
  var key  = params.data.key;
  var ks   = ZH.CompositeQueueKey(ns, cn, key);
  ZPio.SendClusterClientFetch(net.plugin, net.collections,
                              ks, auth, hres, next);
}

function handle_cluster_client_commit(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks     = params.data.ks;
  var crdt   = params.data.crdt;
  var oplog  = params.data.oplog;
  var rchans = crdt._meta.replication_channels;
  var duuid  = -1;
  ZAuth.HasWritePermissions(net, auth, ks, duuid, rchans, function(aerr, ok) {
    if (aerr) next(aerr, hres);
    else {
      if (!ok) next(new Error(ZS.Errors.WritePermsFail), hres);
      else {
        ZDirect.CentralHandleClusterClientCommit(net, ks, crdt, oplog,
                                                 hres, next);
      }
    }
  });
}

function handle_client_commit(conn, params, id, wid, net, next) {
  var auth  = params.authentication;
  var hres  = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ns    = params.data.namespace;
  var cn    = params.data.collection;
  var key   = params.data.key;
  var crdt  = params.data.crdt;
  var oplog = params.data.oplog;
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  ZPio.SendClusterClientCommit(net.plugin, net.collections,
                               ks, crdt, oplog, auth, hres, next);
}

function handle_cluster_client_remove(conn, params, id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks     = params.data.ks;
  var duuid  = -1;
  ZAuth.HasWritePermissionsOnKey(net, auth, ks, duuid, function(aerr, ok) {
    if (aerr) next(aerr, hres);
    else {
      if (!ok) next(new Error(ZS.Errors.WritePermsFail), hres);
      else {
        ZDirect.CentralHandleClusterClientRemove(net, ks, hres, next);
      }
    }
  });
}

function handle_client_remove(conn, params, id, wid, net, next) {
  var auth  = params.authentication;
  var hres  = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ns    = params.data.namespace;
  var cn    = params.data.collection;
  var key   = params.data.key;
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  ZPio.SendClusterClientRemove(net.plugin, net.collections,
                               ks, auth, hres, next);
}

function handle_cluster_client_stateless_commit(conn, params,
                                                id, wid, net, next) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ks     = params.data.ks;
  var rchans = params.data.replication_channels;
  var oplog  = params.data.oplog;
  var duuid  = -1;
  ZAuth.HasWritePermissions(net, auth, ks, duuid, rchans, function(aerr, ok) {
    if (aerr) next(aerr, hres);
    else {
      if (!ok) next(new Error(ZS.Errors.WritePermsFail), hres);
      else {
        ZDirect.CentralHandleClusterStatelessClientCommit(net, ks, rchans,
                                                          oplog, hres, next);
      }
    }
  });
}

function handle_client_stateless_commit(conn, params, id, wid, net, next) {
  var auth   = params.authentication;
  var hres   = ZCloud.CreateHres(conn, params, id, wid, auth);
  var ns     = params.data.namespace;
  var cn     = params.data.collection;
  var key    = params.data.key;
  var rchans = params.data.replication_channels;
  var oplog  = params.data.oplog;
  var ks     = ZH.CompositeQueueKey(ns, cn, key);
  ZPio.SendClusterClientStatelessCommit(net.plugin, net.collections,
                                        ks, rchans, oplog, auth, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SHUTDOWN ------------------------------------------------------------------

exports.DoShutdown = function() {
  ZH.Central.net.plugin.do_shutdown(ZH.Central.net.collections);
  ZH.e("EXITING");
  fs.unlinkSync(ZH.Central.PidFile);
  ZH.e('EXITING');
  ZH.CloseLogFile();
  process.exit();
}

function handle_shutdown(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  ZH.t('<-|(I): Shutdown');
  ZCLS.CentralHandleShutdown(net.plugin, net.collections);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// METHOD NAME TO FUNCTION MAP -----------------------------------------------

var Methods = {
// AGENT PING ----------------------------------------------------------------
                'AgentPing'                  :
                  {handler    : handle_agent_ping,
                   no_backoff : true,
                   fixlog     : true,
                   next : agent_ping_processed},

// AGENT METHODS (LEADER BASED) ----------------------------------------------
                'AgentOnline'                :
                  {handler    : handle_agent_online,
                   no_backoff : true,
                   fixlog     : true,
                   next : agent_online_processed},
                  'GeoBroadcastHttpsAgent' : 
                    {handler    : handle_geo_broadcast_https_agent,
                     fixlog     : true,
                     next : ZH.OnErrLog},
                    'BroadcastHttpsAgent' : 
                      {handler    : handle_broadcast_https_agent,
                       fixlog     : true,
                       next : ZH.OnErrLog},
                'AgentRecheck'                  :
                  {handler    : handle_agent_recheck,
                   device_check : true,
                   no_backoff : true,
                   fixlog     : true,
                   next : agent_recheck_processed},
                'AgentGetAgentKeys'          :
                  {handler    : handle_get_agent_keys,
                   device_check : true,
                   fixlog     : true,
                   next : get_agent_keys_processed},

// AGENT METHODS (KEY BASED) -------------------------------------------------
                'AgentDelta'                              :
                  {handler : handle_agent_delta,
                   device_check : true,
                   next : ZH.OnErrLog},
                  'ClusterDelta'                          :
                    {handler : handle_cluster_delta,
                     next : ZH.OnErrLog},
                    'ClusterSubscriberDelta'              :
                      {handler : handle_cluster_subscriber_delta},
                    'ClusterSubscriberAgentDeltaError'  :
                      {handler : handle_cluster_subscriber_agent_delta_error},
                'GeoDelta'                   : 
                  {handler : handle_geo_delta,
                   next : ZH.OnErrLog},
                  'ClusterGeoDelta'          : 
                    {handler : handle_geo_cluster_delta,
                     next : ZH.OnErrLog},
                    'AckGeoDelta'            : 
                      {handler : handle_ack_geo_delta,
                       next : ZH.OnErrLog},
                    'AckGeoDeltaError'       : 
                      {handler : handle_ack_geo_delta_error,
                       next : ZH.OnErrLog},

                'AgentDentries'                 :
                  {handler : handle_agent_dentries,
                   device_check : true,
                   next : ZH.OnErrLog},
                  'ClusterDentries'             :
                    {handler : handle_cluster_dentries,
                     next : ZH.OnErrLog},
                'GeoDentries'                   : 
                  {handler : handle_geo_dentries,
                   next : ZH.OnErrLog},
                  'ClusterGeoDentries'          : 
                    {handler : handle_cluster_geo_dentries,
                     next : ZH.OnErrLog},
                    'ClusterSubscriberDentries' :
                      {handler : handle_cluster_subscriber_dentries},

                'GeoCommitDelta'          :
                  {handler : handle_geo_commit_delta,
                   next : geo_commit_delta_processed},
                  'ClusterGeoCommitDelta' :
                    {handler : handle_cluster_geo_commit_delta,
                     next : ZH.OnErrLog},
                    'AckGeoCommitDelta'   :
                      {handler : handle_ack_geo_commit_delta,
                       next : ZH.OnErrLog},
                      'ClusterAckGeoCommitDelta'   :
                        {handler : handle_cluster_ack_geo_commit_delta,
                           next : ZH.OnErrLog},
                'GeoSubscriberCommitDelta'          :
                  {handler : handle_geo_subscriber_commit_delta},
                  'ClusterGeoSubscriberCommitDelta'          :
                    {handler : handle_cluster_geo_subscriber_commit_delta,
                     next : ZH.OnErrLog},
                    'ClusterSubscriberCommitDelta'      :
                      {handler : handle_cluster_subscriber_commit_delta},

// MERGE METHODS -------------------------------------------------------------
                'AgentNeedMerge'           :
                  {handler : handle_agent_need_merge,
                   device_check : true,
                   next : agent_need_merge_processed},
                  'ClusterSubscriberMerge' :
                    {handler : handle_cluster_subscriber_merge},
                'GeoNeedMerge'               : 
                  {handler : handle_geo_need_merge,
                   next : geo_need_merge_processed},

// CACHE/EVICT METHODS -------------------------------------------------------
                'AgentCache'                 : 
                  {handler   : handle_agent_cache,
                   device_check : true,
                   geo_ready : true,
                   next : agent_cache_processed},
                  'ClusterCache'             : 
                    {handler   : handle_cluster_cache,
                     geo_ready : true,
                     next : cluster_cache_processed},
                'AgentEvict'                 : 
                  {handler   : handle_agent_evict,
                   device_check : true,
                   geo_ready : true,
                   next : agent_evict_processed},
                  'ClusterEvict'             : 
                    {handler   : handle_cluster_evict,
                     geo_ready : true,
                     next : cluster_evict_processed},
                'AgentLocalEvict'                 : 
                  {handler   : handle_agent_local_evict,
                   device_check : true,
                   geo_ready : true,
                   next : agent_local_evict_processed},

                'GeoBroadcastCache'          : 
                  {handler   : handle_geo_cache,
                   geo_ready : true,
                   next : geo_cache_processed},
                  'ClusterGeoCache'          : 
                    {handler   : handle_geo_cluster_cache,
                     geo_ready : true,
                     next : cluster_geo_cache_processed},
                'GeoBroadcastEvict'          : 
                  {handler   : handle_geo_evict,
                   geo_ready : true,
                   next : geo_evict_processed},
                  'ClusterGeoEvict'          : 
                    {handler   : handle_geo_cluster_evict,
                     geo_ready : true,
                     next : cluster_geo_evict_processed},
                'GeoBroadcastLocalEvict'          : 
                  {handler   : handle_geo_local_evict,
                   geo_ready : true,
                   next : geo_local_evict_processed},
                  'ClusterGeoLocalEvict'          : 
                    {handler   : handle_geo_cluster_local_evict,
                     geo_ready : true,
                     next : cluster_geo_local_evict_processed},

// SUBSCRIBER-MAP METHODS ----------------------------------------------------
                'BroadcastUpdateSubscriberMap' :
                  {handler : handle_broadcast_update_subscriber_map},

// AGENT METHODS (CHANNEL BASED -> BROADCAST) --------------------------------
                'AgentHasSubscribePermissions' :
                  {handler : handle_agent_has_subscribe_permissions,
                   device_check : true,
                   next : agent_has_subscribe_permissions_processed},
                'AgentSubscribe'               :
                  {handler   : handle_agent_subscribe,
                   device_check : true,
                   geo_ready : true,
                   next : agent_subscribe_processed},
                  'GeoBroadcastSubscribe'      :
                    {handler   : handle_geo_subscribe,
                     geo_ready : true,
                     next : geo_subscribe_processed},
                    'BroadcastSubscribe'       :
                      {handler : handle_broadcast_subscribe,
                       storage : true,
                       no_sync : true,
                       next : broadcast_subscribe_processed},
                'AgentUnsubscribe'           : 
                  {handler   : handle_agent_unsubscribe,
                   device_check : true,
                   geo_ready : true,
                   next : agent_unsubscribe_processed},
                  'GeoBroadcastUnsubscribe'  : 
                    {handler   : handle_geo_unsubscribe,
                     geo_ready : true,
                     next : geo_unsubscribe_processed},
                    'BroadcastUnsubscribe'   : 
                      {handler : handle_broadcast_unsubscribe,
                       storage : true,
                       no_sync : true,
                       next : broadcast_unsubscribe_processed},

// AGENT METHODS (DEVICE BASED) ----------------------------------------------
                'AgentStationUser'            : 
                  {handler   : handle_agent_station_user,
                   device_check : true,
                   geo_ready : true,
                   next : agent_station_user_processed},
                  'GeoBroadcastStationUser'   : 
                    {handler   : handle_geo_station_user,
                     geo_ready : true,
                     next : geo_station_user_processed},
                'AgentDestationUser'          : 
                  {handler   : handle_agent_destation_user,
                   device_check : true,
                   geo_ready : true},
                  'GeoBroadcastDestationUser' : 
                    {handler   : handle_geo_destation_user,
                     geo_ready : true},

// AGENT METHODS (USER BASED) -----------------------------------------------
                'AgentAuthenticate'              :
                  {handler : handle_agent_authenticate,
                   device_check : true,
                   fixlog  : true,
                   next : agent_authenticate_processed},
                'AgentGetUserSubscriptions'      :
                  {handler : handle_agent_get_user_subscriptions,
                   device_check : true,
                   fixlog  : true,
                   next : agent_get_user_subscriptions_processed},
                'AgentGetUserInfo'               :
                  {handler : handle_agent_get_user_info,
                   device_check : true,
                   fixlog  : true,
                   next : agent_get_user_info_processed},
                'AgentGetUserChannelPermissions' :
                  {handler : handle_agent_get_user_channel_permissions,
                   device_check : true,
                   fixlog  : true,
                   next : agent_get_user_channel_permissions_processed},
                'Find'                           :
                  {handler : handle_external_find,
                   fixlog  : true,
                   next : external_find_processed},
                'AgentGetClusterInfo'        : 
                  {handler                   : handle_agent_get_cluster_info,
                   no_sync                   : true,
                   cluster_network_partition : true,
                   geo_network_partition     : true,
                   fixlog                    : true,
                   next : agent_get_cluster_info_processed},

// ADMIN METHODS (USER BASED) ------------------------------------------------
                'AdminAuthenticate'          : 
                  {handler : handle_admin_authenticate,
                   fixlog  : true,
                   next : admin_authenticate_processed},

// ADMIN METHODS (GLOBAL) ----------------------------------------------------
                'AdminAddUser'               : 
                  {handler   : handle_admin_add_user,
                   geo_ready : true,
                   next : admin_add_user_processed},
                  'GeoBroadcastAddUser'      : 
                    {handler   : handle_geo_add_user,
                     geo_ready : true,
                     next : geo_add_user_processed},

                'AdminRemoveUser'            : 
                  {handler   : handle_admin_remove_user,
                   geo_ready : true,
                   next : admin_remove_user_processed},
                  'GeoBroadcastRemoveUser'   : 
                    {handler   : handle_geo_remove_user,
                     geo_ready : true,
                     next : geo_remove_user_processed},
                    'BroadcastRemoveUser'    : 
                      {handler : handle_broadcast_remove_user,
                       storage : true,
                       no_sync : true,
                       next : broadcast_remove_user_processed},

                'AdminGrantUser'             : 
                  {handler   : handle_admin_grant_user,
                   geo_ready : true,
                   next : admin_grant_user_processed},
                  'GeoBroadcastGrantUser'    : 
                    {handler   : handle_geo_grant_user,
                     geo_ready : true,
                     next : geo_grant_user_processed},
                    'BroadcastGrantUser'     : 
                      {handler : handle_broadcast_grant_user,
                       storage : true,
                       no_sync : true,
                       next : broadcast_grant_user_processed},

                'AdminRemoveDataCenter'      : 
                  {handler   : handle_admin_remove_dc,
                   geo_ready : true,
                   next : admin_remove_dc_processed},
                  'GeoRemoveDataCenter'      :
                    {handler   : handle_geo_remove_dc,
                     geo_ready : true,
                     next : ZH.OnErrLog},

// CLUSTER VOTE METHODS ------------------------------------------------------
                'AnnounceNewCluster'                :
                  {handler               : handle_announce_new_cluster,
                   no_sync               : true,
                   geo_network_partition : true,
                   fixlog                : true,
                   next : ZH.OnErrLog},

// DATACENTER ONLINE METHODS -------------------------------------------------
                'GeoDataCenterOnline'        : 
                  {handler : handle_geo_datacenter_online,
                   fixlog  : true,
                   next : geo_datacenter_online_processed},
                'GeoBroadcastDeviceKeys'        : 
                  {handler : handle_geo_broadcast_device_keys,
                   fixlog  : true,
                   next : geo_broadcast_device_keys_processed},
                'BroadcastClusterStatus'     : 
                  {handler               : handle_cluster_status,
                   storage               : true,
                   no_sync               : true,
                   geo_network_partition : true,
                   fixlog                : true,
                   next : cluster_status_processed},

// GEO PING METHODS ----------------------------------------------------------
                'GeoLeaderPing'              : 
                  {handler               : handle_geo_leader_ping,
                   no_sync               : true,
                   geo_network_partition : true,
                   fixlog                : true},
                'GeoDataPing'                : 
                  {handler               : handle_geo_data_ping,
                   no_sync               : true,
                   geo_network_partition : true,
                   fixlog                : true},

// GEO VOTE METHODS ----------------------------------------------------------
                'GeoStateChangeVote'             :
                  {handler               : handle_geo_state_change_vote,
                   geo_network_partition : true,
                   no_sync               : true,
                   fixlog                : true},
                'GeoStateChangeCommit'           :
                  {handler               : handle_geo_state_change_commit,
                   no_sync               : true,
                   geo_network_partition : true,
                   fixlog                : true},
                'BroadcastAnnounceNewGeoCluster' : 
                  {handler               : handle_announce_new_geo_cluster,
                   storage               : true,
                   no_sync               : true,
                   geo_network_partition : true,
                   fixlog                : true,
                   next : ZH.OnErrLog},

// DISCOVERY -----------------------------------------------------------------
                'DataCenterDiscovery'        : 
                  {handler : handle_datacenter_discovery,
                   fixlog  : true,
                   next : datacenter_discovery_processed},

// APP-SERVER-CLUSTER --------------------------------------------------------
                'AnnounceNewAppServerCluster' :
                  {handler : handle_announce_new_app_server_cluster,
                   device_check : true,
                   no_sync : true,
                   fixlog  : true,
                   next : announce_new_app_server_cluster_processed},
                'AgentMessage'                :
                  {handler : handle_agent_message,
                   device_check : true,
                   fixlog  : true,
                   next : agent_message_processed},
                  'GeoMessage'                  :
                    {handler : handle_geo_message,
                     fixlog  : true,
                     next : ZH.OnErrLog},
                    'ClusterSubscriberMessage'  :
                      {handler : handle_cluster_subscriber_message,
                       fixlog  : true,
                       next : ZH.OnErrLog},
                    'ClusterUserMessage'        :
                      {handler : handle_cluster_user_message,
                       fixlog  : true,
                       next : ZH.OnErrLog},
                'AgentRequest'                :
                  {handler : handle_agent_request,
                   device_check : true,
                   fixlog  : true,
                   next : agent_request_processed},

// APP-SERVER/MEMCACHE-CLUSTER ------------------------------------------------
                'AnnounceNewMemcacheCluster'          :
                  {handler : handle_announce_new_memcache_cluster,
                   device_check : true,
                   no_sync : true,
                   fixlog  : true,
                   next : announce_new_memcache_cluster_processed},
                  'BroadcastAnnounceNewMemcacheCluster' :
                    {handler : handle_broadcast_announce_new_memcache_cluster,
                     no_sync : true,
                     fixlog  : true,
                     next : ZH.OnErrLog},

// CLIENT (DIRECT) METHODS ----------------------------------------------------
                'ClientStore'                             :
                  {handler : handle_client_store,
                   next : client_store_processed},
                  'ClusterClientStore'                     :
                    {handler : handle_cluster_client_store,
                     next : cluster_client_store_processed},
                'ClientFetch'                             :
                  {handler : handle_client_fetch,
                   next : client_fetch_processed},
                  'ClusterClientFetch'                     :
                    {handler : handle_cluster_client_fetch,
                     next : cluster_client_fetch_processed},
                'ClientCommit'                            :
                  {handler : handle_client_commit,
                   next : client_commit_processed},
                  'ClusterClientCommit'                    :
                    {handler : handle_cluster_client_commit,
                     next : cluster_client_commit_processed},
                'ClientRemove'                            :
                  {handler : handle_client_remove,
                   next : client_remove_processed},
                  'ClusterClientRemove'                    :
                    {handler : handle_cluster_client_remove,
                     next : cluster_client_remove_processed},
                'ClientStatelessCommit'                    :
                  {handler : handle_client_stateless_commit,
                   next : client_stateless_commit_processed},
                  'ClusterClientStatelessCommit'           :
                    {handler : handle_cluster_client_stateless_commit,
                     next : cluster_client_stateless_commit_processed},

// SHUTDOWN ------------------------------------------------------------------
                'Shutdown'                   :
                  {handler : handle_shutdown,
                   no_backoff                : true,
                   no_sync                   : true,
                   cluster_network_partition : true,
                   geo_network_partition     : true,
                   fixlog                    : true},

              };

// NOTE: these are ALL ROUTER methods
for (var mname in Methods) {
  Methods[mname].router = true;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HTTPS SERVER --------------------------------------------------------------

function process_https_request(request, response) {
  var conn = {hsconn : response};
  if (request.method !== 'POST') {
    var path = './static' + url.parse(request.url).pathname;
    if (ZH.AmRouter && ZH.IsDefined(WhitelistedFileTypes[path])) {
      ZH.t('HTML SERVING PAGE: ' + path);
      var text = fs.readFileSync(path, 'utf8');
      var type = WhitelistedFileTypes[path].type;
      return ZCloud.HttpsRespondFile(conn.hsconn, text, type);
    } else {
      ZH.e('IGNORED: HTTP REQUEST FOR: ' + url.parse(request.url).pathname);
      return ZCloud.RespondError(conn, -32000, ZS.Errors.MethodMustBePost,
                                 null, null, null, ZH.NoOp);
    }
  }

  var pdata = '';
  request.addListener('data', function(chunk) { pdata += chunk; });
  request.addListener('end', function() { 
    ZCloud.HandleFrontendRequest(Central.wss, null, response, null, pdata);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZATION ------------------------------------------------------------

function init_cluster(net) {
  ZCLS.Initialize(net, MyClusterNode, ZCloud.HandleClusterMethod, ZH.OnErrLog);
  if (ZH.AmBoth) {
    ZCLS.InitStorageConnection(net, ZH.OnErrLog);
    ZCLS.InitRouterConnection (net, ZH.OnErrLog);
  } else if (ZH.AmRouter) {
    ZCLS.InitStorageConnection(net, ZH.OnErrLog);
  } else {
    ZCLS.InitRouterConnection(net, ZH.OnErrLog);
  }
  if (!ZH.AmBoth) {
    ZDQ.InitDataQueueConnection(exports.DataQueue, ZH.OnErrLog);
  }
}

function init_complete(plugin, collections, next) {
  next(null, null);
  // NOTE: BELOW is ASYNC
  if (ZH.AmStorage) {
    ZDConn.StartCentralToSyncKeysDaemon();
    ZGDD.StartCentralDirtyDeltasDaemon();
    ZGCReap.StartCentralGCPurgeDaemon();
    ZED.StartCentralExpireDaemon();
  }
  ZASC.InitializeCentralAppServerClusterQueueConnection();
}

function init_agent_isolated_map(plugin, collections, next) {
  plugin.do_get(collections.global_coll, ZS.AllIsolatedDevices,
  function (gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length !== 0) {
        var auuids = gres[0];
        delete(auuids._id);
        for (var auuid in auuids) {
          auuid = Number(auuid);
          ZH.CentralIsolatedAgents[auuid] = true;
        }
      }
      next(null, null);
    }
  });
}

function init_central(plugin, collections, next) {
  var now  = ZH.GetMsTime();
  var ckey = ZS.ServiceStart;
  plugin.do_set_field(collections.global_coll, ckey, "timestamp", now,
  function (serr, sres) {
    if (serr) next(serr, null);
    else {
      init_agent_isolated_map(plugin, collections, function(ierr, ires) {
        if (ierr) next(ierr, null);
        else {
          ZFix.Init(function(ierr, ires) {
            if (ierr) next(ierr, null);
            else      init_complete(plugin, collections, next);
          });
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WSS SERVER ----------------------------------------------------------------

//TODO ZH.CentralWsconnMap[] needs reverse mapping
function handle_wsconn_close(wsconn) {
  for (var suuid in ZH.CentralWsconnMap) {
    if (ZH.CentralWsconnMap[suuid] === wsconn) {
      ZH.t('deleting ZH.CentralWsconnMap[' + suuid + ']');
      delete(ZH.CentralWsconnMap[suuid]);
    }
  }
}

//TODO ZH.CentralSocketMap[] needs reverse mapping
function handle_socket_close(sock) {
  for (var suuid in ZH.CentralSocketMap) {
    if (ZH.CentralSocketMap[suuid] === sock) {
      ZH.t('deleting ZH.CentralSocketMap[' + suuid + ']');
      delete(ZH.CentralSocketMap[suuid]);
    }
  }
}

function init_central_wss_server(next) { // WSS server for Agents
  ZH.l('init_central_wss_server');
  Central.hserver.listen(Central.port, Central.ip); // CALLBACK below
  ZH.l('ZCentral listening on https://' + Central.ip + 
                                    ':' + Central.port + '/');
  Central.wss = new WebSocketServer({server: Central.hserver});
  Central.wss.on('listening', function() {
    ZH.l('CENTRAL: WSS: listening');
    Central.ready = true;
    next(null, null);
  });
  Central.wss.on('connection', function(wsconn) {
    ZH.t('CENTRAL: WSS: connection');
    wsconn.on('message', function (message) {
      ZH.t('CENTRAL: WSS Recieved message'); //ZH.t(message);
      ZCloud.HandleFrontendRequest(Central.wss, wsconn, null, null, message);
    });
    wsconn.on('error', function (err) {
      ZH.t('CENTRAL: wsconn error'); ZH.t(err);
      handle_wsconn_close(wsconn);
    });
    wsconn.on('close', function () {
      ZH.t('CENTRAL: wsconn close');
      handle_wsconn_close(wsconn);
    });
  });
}

function process_socket_read(sbuf) {
  if (sbuf.contents.length === 0) return null;
  if (sbuf.need === 0) {
    var need      = sbuf.contents.readUInt32LE(0).toString();
    sbuf.need     = Number(need);
    var body      = sbuf.contents.slice(4);
    sbuf.contents = body;
  }
  if (sbuf.contents.length < sbuf.need) return null;
  else {
    var subb      = sbuf.contents.slice(0, sbuf.need);
    var rest      = sbuf.contents.slice(sbuf.need);
    sbuf.contents = rest;
    sbuf.need     = 0;
    return subb.toString();
  }
}

function init_central_socket_server(next) {
  if (!Central.socket_port) return next(null, null);
  Central.tserver = Net.createServer(function(sock) {
    socket.setKeepAlive(true, 60000); // 60000ms -> 1 minute
    var sbuf = {need : 0, contents : Buffer(0)};
    sock.on('data', function(data) {
      sbuf.contents = Buffer.concat([sbuf.contents, data]);
      while (true) {
        var pdata = process_socket_read(sbuf);
        if (!pdata) break;
        else {
          ZH.t('CENTRAL: SOCKET Recieved request'); ZH.t(pdata);
          ZCloud.HandleFrontendRequest(Central.wss, null, null, sock, pdata);
        }
      }
    });
    sock.on('error', function(err) {
      ZH.e('CENTRAL: socket error'); ZH.e(err);
      handle_socket_close(sock);
    });
    sock.on('close', function() {
      ZH.e('CENTRAL: socket close');
      handle_socket_close(sock);
    });
  })
  Central.tserver.listen(Central.socket_port, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      ZH.e('ZCentral listening on SOCKET://' + Central.socket_port);
      next(null, null);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZE ADMIN USER ON STARTUP ------------------------------------------

function init_admin_user(net) {
  if (InitializeAdminUserOnStartup) {
    var admin = InitializeAdminUserOnStartup;
    ZH.e('InitializeAdminUserOnStartup: UN: ' + admin.username);
    ZH.HashPassword(admin.password, function(perr, phash) {
      if (perr) next(perr, hres);
      else {
        ZUM.DoAddUser(net.plugin, net.collections,
                      admin.username, "ADMIN", phash, {}, ZH.OnErrLog);
      }
    });
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CONFIG --------------------------------------------------------------------

var InitNamespace       = 'datastore';


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MAIN() --------------------------------------------------------------------

Central.ready           = false;
ZH.CentralSynced        = this.datacenter_synced;
ZH.l('STARTUP: ZH.CentralSynced: ' + ZH.CentralSynced);

Central.net             = {};   // Grab-bag for [plugin, collections]
Central.net.collections = {};   // Grab-bag for [admin_coll, c_delta_coll]
Central.net.plugin      = null;
ZDBP.SetDeviceUUID(Central.device_uuid);

ZCloud.Initialize(Central);
ZCloud.InitializeMethods(Methods,      ZH.OnErrLog);
ZCloud.AddMethods       (ZMCV.Methods, ZH.OnErrLog);

var https_conf          = ZH.CentralConfig;
var Server_opts         = {key  : fs.readFileSync(https_conf.ssl_key),
                           cert : fs.readFileSync(https_conf.ssl_cert)};
Central.hserver         = https.createServer(Server_opts,
                                             process_https_request);

ZDBP.SetDataCenter(this.datacenter);
ZDBP.PluginConnect(InitNamespace, function(cerr, zhndl) {
  if (cerr) throw(cerr);
  else {
    Central.net.zhndl       = zhndl;
    Central.net.plugin      = zhndl.plugin;
    Central.net.db_handle   = zhndl.db_handle;
    Central.net.collections = zhndl.collections;
    ZDBP.AdminConnect(true, function(aerr) {
      if (aerr) throw(aerr);
      else {
        Central.net.plugin.do_populate_data(Central.net, Central.device_uuid);
        var plugin      = Central.net.plugin;
        var collections = Central.net.collections;
        init_central(plugin, collections, function(ierr, ires) {
          if (ierr) throw(ierr);
          else {
            if (ZH.AmRouter) {
              init_central_wss_server(ZH.OnErrLog);
              init_central_socket_server(ZH.OnErrLog);
            }
            init_cluster(Central.net);
            init_admin_user(Central.net);
          }
        });
      }
    });
  }
});


