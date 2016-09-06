"use strict";

require('./setImmediate');

var ZPio       = require('./zpio');
var ZGack      = require('./zgack');
var ZDConn     = require('./zdconn');
var ZVote      = require('./zvote');
var ZTLS       = require('./ztls');
var ZPart      = require('./zpartition');
var ZDQ        = require('./zdata_queue');
var ZQ         = require('./zqueue');
var ZS         = require('./zshared');
var ZH         = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var GeoReconnectTimeout     = 4000;
var ClusterReconnectTimeout = 8000;

var RetrySendDiscoverySleep = 5000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function cmp_device_uuid(acnode, bcnode) {
  var aduuid = acnode.device_uuid;
  var bduuid = bcnode.device_uuid;
  return (aduuid === bduuid) ? 0 : (aduuid > bduuid) ? 1 : -1;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLUSTER ACCESSORS ---------------------------------------------------------

exports.ForceVote           = true;

exports.ClusterNodes        = [];
exports.MyNode              = null;

exports.ForceGeoVote        = true;
exports.GeoNodes            = [];
exports.KnownGeoNodes       = []; // Used for LoadBalancer ip:port

exports.LastGeoCreated      = {};

var RedirectTimeout         = 50; // Cluster/GeoMessages redirects

exports.HandleMethod        = null;

exports.GetClusterNodes = function() {
  return ZH.clone(exports.ClusterNodes);
}

exports.GetMyClusterNode = function() {
  return exports.GetClusterNodeByUUID(ZH.MyUUID);
}

exports.GetGeoNodes = function() {
  return ZH.clone(exports.GeoNodes);
}

exports.GetOtherGeoNodes = function() {
  var gnodes = exports.GetGeoNodes();
  for (var i = 0; i < gnodes.length; i++) {
    var gnode = gnodes[i];
    var guuid = gnode.device_uuid;
    if (guuid === ZH.MyDataCenter) {
      gnodes.splice(i, 1);
      break;
    }
  }
  return gnodes;
}

exports.GetPrimaryDataCenter = function() {
  for (var i = 0; i < exports.GeoNodes.length; i++) {
    var gnode = exports.GeoNodes[i];
    if (gnode.primary) return gnode;
  }
  return null;
}

exports.GetClusterNodeByUUID = function(uuid) {
  var cuuid = Number(uuid);
  for (var i = 0; i < exports.ClusterNodes.length; i++) {
    var cnode = exports.ClusterNodes[i];
    if (cnode.device_uuid === cuuid) return cnode;
  }
  return null;
}

exports.GetClusterLeaderNode = function() {
  for (var i = 0; i < exports.ClusterNodes.length; i++) {
    var cnode = exports.ClusterNodes[i];
    if (cnode.leader) return cnode;
  }
  return null;
}

exports.GetGeoNodeByUUID = function(guuid) {
  for (var i = 0; i < exports.GeoNodes.length; i++) {
    var gnode = exports.GeoNodes[i];
    if (gnode.device_uuid === guuid) return gnode;
  }
  return null;
}

exports.RemoveGeoNodeByUUID = function(guuid) {
  for (var i = 0; i < exports.GeoNodes.length; i++) {
    var gnode = exports.GeoNodes[i];
    if (gnode.device_uuid === guuid) return exports.GeoNodes.splice(i, 1);
  }
  return null;
}

// NOTE: RUN this only if you are SURE you have ONE EXTERNAL Geo-Connection
var NextRandomGeoNode = 0;
exports.GetRandomGeoNode = function() {
  if (exports.GeoNodes.length === 0) return null;
  var safeguard = exports.GeoNodes.length + 1;
  while(true) {
    NextRandomGeoNode += 1;
    if (NextRandomGeoNode >= exports.GeoNodes.length) NextRandomGeoNode = 0;
    var gnode = exports.GeoNodes[NextRandomGeoNode];
    var guuid = gnode.device_uuid;
    if (guuid !== ZH.MyDataCenter) {
      if (!ZH.AmRouter) return gnode; // STORAGE NOT connected to gnodes[]
      else {
        var mclient = GeoDataClients[guuid];
        if (mclient) {
          //ZH.t('GetRandomGeoNode: ' + i + ' connected: ' + mclient.connected);
          if (mclient.connected) return gnode;
        }
      }
    }
    safeguard -= 1;
    if (safeguard === 0) return null;
  }
}

exports.AmPrimaryDataCenter = function() {
  if (exports.GeoNodes.length === 0) return false;
  var pdc = exports.GetPrimaryDataCenter();
  if (!pdc) return false;
  return (pdc.device_uuid === ZH.MyDataCenter);
}

exports.GetKnownGeoNodeByUUID = function(guuid) {
  for (var i = 0; i < exports.KnownGeoNodes.length; i++) {
    var gnode = exports.KnownGeoNodes[i];
    if (gnode.device_uuid === guuid) return gnode;
  }
  return null;
}

exports.RemoveKnownGeoNodeByUUID = function(guuid) {
  for (var i = 0; i < exports.KnownGeoNodes.length; i++) {
    var gnode = exports.KnownGeoNodes[i];
    if (gnode.device_uuid === guuid) return exports.KnownGeoNodes.splice(i, 1);
  }
  return null;
}

exports.GetKnownDataGeoNodes = function() {
  var gnodes = ZH.clone(exports.KnownGeoNodes);
  for (var i = 0; i < gnodes.length; i++) {
    var gnode = gnodes[i];
    if (gnode.device_uuid === exports.DiscoveryUUID) { // Discover != DATA
      gnodes.splice(i, 1);
      return gnodes;
    }
  }
  return gnodes;
}

// NOTE: RUN this only if you are SURE you have ONE EXTERNAL KnownGeoNode
var NextRandomKnownGeoNode = 0;
exports.GetRandomKnownGeoNode = function() {
  if (exports.KnownGeoNodes.length === 0) return null;
  var safeguard = exports.KnownGeoNodes.length + 1;
  while(true) {
    NextRandomKnownGeoNode += 1;
    if (NextRandomKnownGeoNode >= exports.KnownGeoNodes.length) {
      NextRandomKnownGeoNode = 0;
    }
    var gnode = exports.KnownGeoNodes[NextRandomKnownGeoNode];
    var guuid = gnode.device_uuid;
    if (guuid !== exports.DiscoveryUUID && guuid !== ZH.MyDataCenter) {
      return gnode;
    }
    safeguard -= 1;
    if (safeguard === 0) return null;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// (GEO)CLUSTER MESH SETUP ---------------------------------------------------

var ClusterClients       = {};
var ClusterClientLocks   = {};

var GeoLeaderClients     = {};
var GeoLeaderClientLocks = {};
var GeoDataClients       = {};
var GeoDataClientLocks   = {};

exports.SelfGeoReady = function() {
  if (!ZH.AmRouter) return true;
  var guuid = ZH.MyDataCenter;
  if (ZH.AmClusterLeader) {
    var rdy = (GeoLeaderClients[guuid] && GeoDataClients[guuid]) ? true : false;
    ZH.t('ZCLS.SelfGeoReady: LEADER: ' + rdy);
    return rdy;
  } else {
    var rdy = (GeoDataClients[guuid])                            ? true : false;
    ZH.t('ZCLS.SelfGeoReady: NODE: ' + rdy);
    return rdy;
  }
}

function create_Cclient(cnode, next) {
  ZH.l('ZCLS: Opening CLUSTER CLIENT: U: ' + cnode.device_uuid +
       ' H: ' + cnode.backend.server.hostname +
       ' P: ' + cnode.backend.server.port);
  ZTLS.CreateTlsClient(cnode, false, false, false,
                       exports.GetClusterNodeByUUID, ClusterReconnectTimeout,
                       next);
}

function kill_previous_cclient(cnode, next) {
  var cuuid   = cnode.device_uuid;
  var cclient = ClusterClients[cuuid];
  if (!cclient) next(null, null);
  else {
    delete(ClusterClients[cuuid]);
    cclient.kill(next);
  }
}

function recreate_cluster_client(cuuid, cnode, next) {
  ZH.e('recreate_cluster_client: U: ' + cuuid);
  var c_lock = ClusterClientLocks;
  if (c_lock[cuuid]) {
    ZH.e('recreate_cluster_client: ERROR:(1): ' + ZS.Errors.ErrorClientLocked);
    next(new Error(ZS.Errors.ErrorClientLocked), null);
  } else {
    c_lock[cuuid] = true;
    kill_previous_cclient(cnode, function(kerr, kres) {
      if (kerr) {
        ZH.e('recreate_cluster_client: ERROR:(2): ' + kerr.message);
        c_lock[cuuid] = false;
        next(kerr, null);
      } else {
        create_Cclient(cnode, function(cerr, cclient) {
          if (cerr) {
            ZH.e('recreate_cluster_client: ERROR:(3): ' + cerr.message);
            c_lock[cuuid] = false;
            next(cerr, null);
          } else {
            ZH.e('recreate_cluster_client: OK: U: ' + cuuid);
            ClusterClients[cuuid] = cclient;
            c_lock[cuuid]         = false;
            next(null, cclient);
          }
        });
      }
    });
  }
}

function prune_cclients(tor, next) {
  ZH.t('prune_cclients: REMAINING: #R: ' + tor.length);
  if (tor.length === 0) next(null, null);
  else {
    var cuuid = tor.shift();
    var cclient = ClusterClients[cuuid];
    if (!cclient) {
      setImmediate(prune_cclients, tor, next);
    } else {
      delete(ClusterClients[cuuid]);
      cclient.kill(function(serr, sres) {
        if (serr) next(serr, null);
        else      setImmediate(prune_cclients, tor, next);
      });
    }
  }
}

function prune_ClusterClients(next) {
  var tor = [];
  for (var cuuid in ClusterClients) {
    var cnode = exports.GetClusterNodeByUUID(cuuid);
    if (!cnode) {
      tor.push(cuuid);
    }
  }
  if (tor.length) { ZH.t('do(prune_cclients)'); ZH.t(tor); }
  prune_cclients(tor, next);
}

function create_client_to_cluster_node(cnode, next) {
  var cuuid   = cnode.device_uuid;
  var cclient = ClusterClients[cuuid];
  ZH.l('create_client_to_cluster_node: U: ' + cuuid + ' CLI: ' + cclient);
  if (cclient) next(null, cclient);
  else         recreate_cluster_client(cuuid, cnode, next);
}

var InitClusterMeshInProgress = false;

function init_cluster_mesh_nodes(cnodes, next) {
  ZH.t('init_cluster_mesh_nodes: REMAINING: #CN: ' + cnodes.length);
  if (cnodes.length === 0) {
    InitClusterMeshInProgress = false;
    next(null, null);
  } else {
    var cnode = cnodes.shift();
    create_client_to_cluster_node(cnode, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(init_cluster_mesh_nodes, cnodes, next);
    });
  }
}

function init_cluster_mesh(next) {
  ZH.t('init_cluster_mesh: InProgress: ' + InitClusterMeshInProgress);
  if (InitClusterMeshInProgress) next(null, null);
  else{
    if (exports.ClusterNodes.length === 0) next(null, null);
    else {
      var cnodes                = ZH.clone(exports.ClusterNodes);
      InitClusterMeshInProgress = true;
      ZH.t('do(init_cluster_mesh_nodes): #CN: ' + cnodes.length);
      init_cluster_mesh_nodes(cnodes, next);
    }
  }
}

function get_Gclient(guuid, leader) {
  return leader ? GeoLeaderClients[guuid] : GeoDataClients[guuid];
}

function create_Gclient(gnode, leader, next) {
  var guuid = gnode.device_uuid;
  ZH.l('ZCLS: Opening ' + (leader ? 'LEADER-' : '') +
       'GEO-CLIENT: GU: ' + guuid + ' H: ' + gnode.backend.server.hostname +
        ' P: ' + gnode.backend.server.port);
  ZTLS.CreateTlsClient(gnode, true, leader, false,
                       exports.GetKnownGeoNodeByUUID, GeoReconnectTimeout,
                       next);
}

function kill_previous_gclient(gnode, leader, next) {
  var guuid    = gnode.device_uuid;
  ZH.t('kill_previous_gclient: GU: ' + guuid + ' leader: ' + leader);
  var gclients = leader ? GeoLeaderClients : GeoDataClients;
  var gclient  = gclients[guuid];
  if (!gclient) next(null, null); 
  else {
    delete(gclients[guuid]);
    gclient.kill(next);
  }
}

function get_geo_client_lock(leader, guuid) {
  if (leader) return GeoLeaderClientLocks;
  else        return GeoDataClientLocks;
}

function recreate_geo_client(guuid, gnode, leader, next) {
  ZH.e('recreate_geo_client: RU: ' + guuid + ' L: ' + leader);
  var glock = get_geo_client_lock(leader);
  if (glock[guuid]) {
    ZH.e('recreate_geo_client: ERROR:(1): ' + ZS.Errors.ErrorGeoClientLocked);
    next(new Error(ZS.Errors.ErrorGeoClientLocked), null);
  } else {
    glock[guuid] = true;
    kill_previous_gclient(gnode, leader, function(kerr, kres) {
      if (kerr) {
        ZH.e('recreate_geo_client: ERROR:(2): ' + kerr.message);
        glock[guuid] = false;
        next(kerr, null);
      } else {
        create_Gclient(gnode, leader, function(cerr, gclient) {
          if (cerr) {
            ZH.e('recreate_geo_client: ERROR:(3): ' + cerr.message);
            glock[guuid] = false;
            next(cerr, null);
          } else {
            ZH.e('recreate_geo_client: OK: RU: ' + guuid + ' L: ' + leader);
            if (leader) GeoLeaderClients[guuid] = gclient;
            else        GeoDataClients  [guuid] = gclient;
            glock[guuid] = false;
            next(null, gclient);
          }
        });
      }
    });
  }
}

function recreate_geo_client_from_cnode(guuid, cnode, leader, next) {
  var rgnode         = ZH.clone(cnode);
  rgnode.device_uuid = guuid;
  ZH.t('recreate_geo_client_from_cnode'); ZH.t(rgnode);
  recreate_geo_client(guuid, rgnode, leader, next);
}

function create_client_to_geo_node(gnode, leader, next) {
  var guuid   = gnode.device_uuid;
  ZH.t('create_client_to_geo_node: RU: ' + guuid);
  var clients = leader ? GeoLeaderClients : GeoDataClients;
  var gclient = clients[guuid];
  if (gclient) next(null, gclient);
  else         recreate_geo_client(guuid, gnode, leader, next);
}

function init_geo_nodes(next) {
  var need = exports.GeoNodes.length;
  if (need === 0) next(null, null);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var gnode = exports.GeoNodes[i];
      create_client_to_geo_node(gnode, false, function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, null);
        } else {
          done += 1;
          if (done === need) next(null, null);
        }
      });
    }
  }
}

function init_geo_leader_nodes(next) {
  var need = exports.KnownGeoNodes.length;
  if (need === 0) next(null, null);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var gnode = exports.KnownGeoNodes[i];
      if (gnode.discovery) {
        done += 1;
        if (done === need) next(null, null);
      } else {
        create_client_to_geo_node(gnode, true, function(serr, sres) {
          if      (nerr) return;
          else if (serr) {
            nerr = true;
            next(serr, null);
          } else {
            done += 1;
            if (done === need) next(null, null);
          }
        });
      }
    }
  }
}

function kill_geo_leader_nodes(next) {
  for (var guuid in GeoLeaderClients) {
    var gclient = GeoLeaderClients[guuid];
    ZH.t('init_geo_cluster_mesh: NOT LEADER: KILL: GU: ' + guuid);
    gclient.kill(ZH.OnErrLog);
  }
  GeoLeaderClients = {};
  next(null, null);
}

// NOTE: init_geo_cluster_mest() may NEVER return -> next() may NEVER be called
function init_geo_cluster_mesh(next) {
  ZH.t('init_geo_cluster_mesh: #GN: ' + exports.GeoNodes.length +
       ' #KGN: ' + exports.KnownGeoNodes.length);
  init_geo_nodes(function(ierr, ires) {
    if (ierr) next(ierr, null);
    else {
      if (ZH.AmClusterLeader) init_geo_leader_nodes(next);
      else                    kill_geo_leader_nodes(next);
    }
  });
}

function init_local_server(mynode, next) {
  ZH.t('init_local_server');
  ZTLS.CreateTlsServer(mynode, handle_cluster_node_server_request,
  function(cerr, cres) {
    if (cerr) next(cerr, null);
    else {
      var mcuuid = mynode.device_uuid;
      recreate_cluster_client(mcuuid, mynode, next);
    }
  });
}

// NOTE: init_cluster_node_server() is ASYNC -> ClusterNodes may be DOWN
function init_cluster_node_server(plugin, collections, mynode, next) {
  ZH.t('init_cluster_node_server: BPORT: ' + mynode.backend.server.port);
  init_local_server(mynode, function(ierr, ires) {
    if (ierr) next(ierr, null);
    else {
      init_cluster_mesh(ZH.OnErrLog);
      ZVote.StartNodeHeartbeat(plugin, collections);
      ZVote.StartClusterCheck(plugin, collections);
      next(null, null);
    }
  });
}

// NOTE: init_geo_cluster_node_server() is ASYNC -> GeoNodes may be DOWN
function init_geo_cluster_node_server(next) {
  ZH.t('init_geo_cluster_node_server');
  init_geo_cluster_mesh(ZH.OnErrLog);
  ZVote.StartGeoLeaderHeartbeat();
  ZVote.StartGeoDataHeartbeat();
  ZVote.StartGeoCheck();
  next(null, null);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZE CLUSTER STATE FROM DATABASE -------------------------------------

function initialize_cluster_node_table(plugin, collections, next) {
  ZVote.FetchClusterStatus(plugin, collections, function(gerr, cstat) {
    if (gerr) next(gerr, null);
    else {
      exports.ClusterNodes = [];
      for (var cuuid in cstat) {
        var cts   = cstat[cuuid].ts * 1000;
        var stale = (ZVote.NodeStart * 1000) - cts;
        if (stale < ZVote.ClusterNodeMaximumAllowedStaleness) {
          exports.ClusterNodes.push(cstat[cuuid].node);
        }
      }
      exports.ClusterNodes.sort(cmp_device_uuid);
      // NOTE ClusterNodes[] computed from ZS.ClusterNodeAliveStatus
      plugin.do_get(collections.global_coll, ZS.ClusterState,
      function(gerr, gres) {
        if (gerr) next(gerr, null);
        else {
          if (gres.length !== 0) {
            var state   = gres[0];
            ZH.ClusterNetworkPartitionMode = state.cluster_network_partition;
            ZH.l('initialize_cluster_node_table: ClusterNetworkPartition: ' +
                 ZH.ClusterNetworkPartitionMode);
            var lcnodes = state.cluster_nodes;
            var am_in_cluster = false;
            for (var i = 0; i < lcnodes.length; i++) {
              if (lcnodes[i].device_uuid === ZH.MyUUID) {
                am_in_cluster = true;
                break;
              }
            }
            var votes_still_alive = true;
            for (var i = 0; i < lcnodes.length; i++) {
              var cuuid = lcnodes[i].device_uuid;
              if (!cstat[cuuid]) {
                ZH.l('IN VOTE, BUT DEAD: DU: ' + cuuid);
                votes_still_alive = false;
                break;
              }
            }
            ZH.YetToClusterVote = !am_in_cluster || !votes_still_alive;
          }
          var vkey = ZS.LastVoteTermNumber;
          plugin.do_get_field(collections.global_coll, vkey, ZH.MyUUID,
          function(gerr, cterm) {
            if (gerr) next(gerr, hres);
            else {
              if (!cterm) cterm = 0;
              ZVote.TermNumber  = cterm + 1; // ADD ONE to avoid repeat votes
              ZH.l('initialize_cluster_node_table:' + 
                   ' TermNumber: ' + ZVote.TermNumber);
              // NOTE: ALWAYS CLUSTER-VOTE ON RESTART
              ZH.l('CLUSTER: #C: ' + exports.ClusterNodes.length);
              ZVote.ClusterVoteCompleted = false;
              exports.ForceVote          = true;
              next(null, null);
            }
          });
        }
      });
    }
  });
}

function init_geo_node_table(plugin, collections, next) {
  ZH.t('init_geo_node_table');
  ZVote.FetchGeoClusterStatus(plugin, collections, function(gerr, gstat) {
    if (gerr) next(gerr, null);
    else {
      exports.GeoNodes = [];
      for (var guuid in gstat) {
        var gts   = gstat[guuid].ts * 1000;
        var stale = (ZVote.NodeStart * 1000) - gts;
        if (stale < ZVote.GeoNodeMaximumAllowedStaleness) {
          exports.GeoNodes.push(gstat[guuid]);
        }
      }
      exports.GeoNodes.sort(cmp_device_uuid);
      plugin.do_get(collections.global_coll, ZS.LastGeoState,
      function(cerr, gres) {
        if (cerr) next(cerr, null);
        else {
          var new_vote_needed = true;
          if (gres.length !== 0) {
            var state                  = gres[0];
            ZH.GeoNetworkPartitionMode = state.geo_network_partition;
            ZH.GeoMajority             = state.geo_majority;
            ZH.l('init_geo_node_table: ZH.GeoNetworkPartitionMode: ' +
                 ZH.GeoNetworkPartitionMode + ' ZH.GeoMajority: ' +
                 ZH.GeoMajority);
            var lgnodes                = state.geo_nodes;
            var am_in_cluster          = false;
            for (var i = 0; i < lgnodes.length; i++) {
              if (lgnodes[i].device_uuid === ZH.MyDataCenter) {
                am_in_cluster = true;
                break;
              }
            }
            var votes_still_alive = true;
            for (var i = 0; i < lgnodes.length; i++) {
              var guuid = lgnodes[i].device_uuid;
              if (!gstat[guuid]) {
                //ZH.t('IN VOTE, BUT DEAD: GU: ' + guuid);
                votes_still_alive = false;
                break;
              }
            }
            ZH.YetToGeoVote = !am_in_cluster || !votes_still_alive; 
            if (ZH.YetToGeoVote) new_vote_needed = true;
            else { // ZS.GeoNodeAliveStatus & ZS.LastGeoState MUST match
              var miss = false;
              for (var i = 0; i < lgnodes.length; i++) {
                var lguuid = lgnodes[i].device_uuid;
                if (!exports.GetGeoNodeByUUID(lguuid)) {
                  miss = true;
                  break;
                }
              }
              new_vote_needed = miss; // Must be 100% match
            }
          }
          ZVote.FetchKnownGeoNodes(plugin, collections, function(ferr, dcs) {
            if (ferr) next(ferr, null);
            else {
              exports.KnownGeoNodes = ZH.clone(dcs);
              if (!exports.GetKnownGeoNodeByUUID(ZH.MyDataCenter)) {
                exports.KnownGeoNodes.push(ZVote.GetMyGeoNode(false));
              }
              var nkdc   = exports.KnownGeoNodes.length;
              ZH.l('ZCLS.init_geo_node_table: #KDC: ' + nkdc);
              var vkey   = ZS.LastVoteGeoTermNumber;
              var mguuid = ZH.MyDataCenter;
              plugin.do_get_field(collections.global_coll, vkey, mguuid,
              function(terr, gterm) {
                if (terr) next(terr, hres);
                else {
                  if (!gterm) gterm = 0;
                  ZVote.GeoTermNumber = gterm + 1;// ADD ONE, avoid repeat votes
                  if (new_vote_needed || exports.GeoNodes.length === 0) {
                    ZVote.GeoVoteCompleted = false;
                    exports.ForceGeoVote   = true;
                    ZH.l('GEO: new_vote: YetToGeoVote: ' + ZH.YetToGeoVote +
                         ' #C: ' + exports.GeoNodes.length +
                         ' ForceGeoVote: ' + exports.ForceGeoVote);
                  } else {
                    ZVote.GeoVoteCompleted = true;
                    exports.ForceGeoVote   = false;
                    ZH.l('GEO: init_geo_node_table: T: ' + ZVote.GeoTermNumber +
                         ' ForceGeoVote: ' + exports.ForceGeoVote);
                    ZH.l(exports.GeoNodes);
                  }
                  next(null, null);
                }
              });
            }
          });
        }
      });
    }
  });
}

function initialize_cluster_and_geo_partition_tables(net, next) {
  initialize_cluster_node_table(net.plugin, net.collections,
  function(ierr, ires) {
    if (ierr) next(ierr, null);
    else {
      ZPart.CreatePartitionTable(net, function(serr, sres) {
        if (serr) next(serr, null);
        else {
          if (!ZH.AmRouter) next(null, null);
          else {
            init_geo_node_table(net.plugin, net.collections, next);
          }
        }
      });
    }
  });
}

function initialize_geo(net, next) {
  if (!ZH.AmCentral) throw(new Error("initialize_geo LOGIC ERROR"));
  if (!ZH.AmRouter) next(null, null);
  else {
    load_last_GeoCreated(net.plugin, net.collections, function(lerr, lres) {
      if (lerr) next(lerr, null);
      else      init_geo_cluster_node_server(next);
    });
  }
}

exports.Initialize = function(net, mynode, mhandler, next) {
  ZH.t('ZCLS.Initialize: BP: ' + mynode.backend.server.port);
  exports.MyNode       = ZH.clone(mynode);
  exports.HandleMethod = mhandler;
  initialize_cluster_and_geo_partition_tables(net, function(nerr, nres) {
    if (nerr) next(nerr, null);
    else {
      init_cluster_node_server(net.plugin, net.collections, mynode,
      function(cerr, cres) {
        if (cerr) next(cerr, null);
        else {
          if (!ZH.AmCentral) next(null, null);
          else               initialize_geo(net, next);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLUSTER MESSAGING ---------------------------------------------------------

function handle_cluster_node_server_request(replier, rdata) {
  //ZH.t('CLUSTER SERVER RECEIVED'); //ZH.t(rdata);
  var conn   = {replier : replier};
  var method = rdata.method;
  var params = rdata.params;
  var id     = rdata.id;
  var wid    = rdata.worker_id;
  exports.HandleMethod(method, conn, params, id, wid, rdata);
}

function updateClusterNode(ncnode) {
  for (var i = 0; i < exports.ClusterNodes.length; i++) {
    if (exports.ClusterNodes[i].device_uuid === ncnode.device_uuid) {
      exports.ClusterNodes[i] = ncnode;
      return;
    }
  }
}

function get_cluster_message_client(cnode, next) {
  var cuuid   = cnode.device_uuid;
  var mclient = ClusterClients[cuuid];
  ZH.t('get_cluster_message_client: U: ' + cuuid + ' mclient: ' + mclient);
  if (mclient) next(null, mclient);
  else {
    recreate_cluster_client(cuuid, cnode, function(rerr, rres) {
      if (rerr) next(rerr, null);
      else      get_cluster_message_client(cnode, next);
    });
  }
}

exports.InternalMessageResponse = function(data, hres, next) {
  for (var k in data.result) {
    hres[k] = data.result[k];
  }
  next(null, hres);
}

exports.ClusterMessage = function(cnode, jrpc_body, hres, next) {
  if (jrpc_body.params) { // NOTE: tags as ClusterMethod
    jrpc_body.params.cluster_method_key = ZH.ClusterMethodKey;
  }
  get_cluster_message_client(cnode, function(gerr, mclient) {
    if (gerr) next(gerr, hres);
    else {
      var cuuid = cnode.device_uuid;
      ZH.t('CLUSTER SEND: U: ' + cuuid); //ZH.t(jrpc_body);
      mclient.request(jrpc_body, function(data) {
        //ZH.t('ZCLS.ClusterMessage RESPONSE');
        if (data.error) {
          hres.error_details = data.error.details;
          next(data.error, hres);
        } else {
          if (data.result.status === 'REDIRECT') {
            var rcnode = data.result.device.cluster_node;
            ZH.t('CLUSTER-REDIRECT U: ' + cuuid + ' ID: ' + jrpc_body.id);
            ZH.t(rcnode);
            updateClusterNode(rcnode);
            recreate_cluster_client(cuuid, rcnode, function(rerr, rres) {
              if (rerr) next(rerr, hres);
              else      exports.ClusterMessage(rcnode, jrpc_body, hres, next);
            });
          } else {
            exports.InternalMessageResponse(data, hres, next);
          }
        }
      });
    }
  });
}

function cmp_cluster_leader(kcl, gcl) {
  if (!kcl) return gcl ? 1 : 0;
  if (!gcl) return 1;
  if (kcl.backend.server.hostname !== gcl.backend.server.hostname) return 1;
  if (kcl.backend.server.port     !== gcl.backend.server.port) return 1;
  return 0;
}

function resetGeoNodeLeader(guuid, cnode, leader, next) {
  if (!leader) next(null, null); // NO-OP
  else {
    if (guuid === exports.DiscoveryUUID) next(null, null); // NO-OP
    else {
      ZH.t('resetGeoNodeLeader: RU: ' + guuid);
      ZH.t('resetGeoNodeLeader: CNODE: ' + JSON.stringify(cnode));
      for (var i = 0; i < exports.KnownGeoNodes.length; i++) {
        var knode = exports.KnownGeoNodes[i];
        if (guuid === knode.device_uuid) {
          var kcl = knode.cluster_leader;
          var gcl = cnode;
          if (cmp_cluster_leader(kcl, gcl)) {
            knode.cluster_leader = ZH.clone(cnode);
            var net              = ZH.CreateNetPerRequest(ZH.Central);
            ZVote.PersistKnownDataCenters(net.plugin, net.collections,
                                          [], next);
            return;
          }
          break;
        }
      }
      next(null, null);
    }
  }
}

function get_geo_message_client(gnode, leader, next) {
  var guuid   = gnode.device_uuid;
  var mclient = get_Gclient(guuid, leader);
  ZH.t('get_geo_message_client: RU: ' + guuid + ' mclient: ' + mclient);
  if (mclient) next(null, mclient);
  else {
    var gnode = leader ? exports.GetKnownGeoNodeByUUID(guuid) :
                         exports.GetGeoNodeByUUID(guuid);
    if (!gnode) {
      ZH.t('ERROR: MISS: get_geo_message_client: GU: ' + guuid);
      next(new Error(ZS.Errors.GeoNodeUnknown), null);
    } else {
      recreate_geo_client(guuid, gnode, leader, function(rerr, rres) {
        if (rerr) next(rerr, null);
        else      get_geo_message_client(gnode, leader, next);
      });
    }
  }
}

function geo_message(gnode, jrpc_body, leader, hres, next) {
  if (jrpc_body.params) { // NOTE: tags as ClusterMethod
    jrpc_body.params.cluster_method_key = ZH.ClusterMethodKey;
  }
  get_geo_message_client(gnode, leader, function(gerr, mclient) {
    if (gerr) next(gerr, hres);
    else {
      var guuid = gnode.device_uuid;
      //ZH.t('GEO SEND: GU: ' + guuid); //ZH.t(jrpc_body);
      mclient.request(jrpc_body, function(data) {
        //ZH.t('ZCLS.GeoMessage RESPONSE');
        if (data.error) { //ZH.t('ZCLS.GeoMessage RESPONSE ERROR');
          next(data.error, hres);
        } else {
          if (data.result.status === 'REDIRECT') {
            var cnode = data.result.device.cluster_node;
            resetGeoNodeLeader(guuid, cnode, leader, function(nerr, nres) {
              if (nerr) next(nerr, hres);
              else {
                ZH.t(leader ? 'GEO-LEADER-REDIRECT' : 'GEO-REDIRECT');
                recreate_geo_client_from_cnode(guuid, cnode, leader,
                function(rerr, rres) {
                  if (rerr) next(rerr, hres);
                  else      geo_message(gnode, jrpc_body, leader, hres, next);
                });
              }
            });
          } else {
            exports.InternalMessageResponse(data, hres, next);
          }
        }
      });
    }
  });
}

exports.GeoMessage = function(gnode, jrpc_body, hres, next) {
  geo_message(gnode, jrpc_body, false, hres, next);
}

exports.GeoLeaderMessage = function(gnode, jrpc_body, hres, next) {
  geo_message(gnode, jrpc_body, true, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO PING HANDLERS ---------------------------------------------------------

function updateGeoNode(ngnode, next) {
  var nguuid = ngnode.device_uuid;
  ZH.t('updateGeoNode: RU: ' + nguuid);
  for (var i = 0; i < exports.GeoNodes.length; i++) {
    var guuid = exports.GeoNodes[i].device_uuid;
    if (guuid === nguuid) {
      exports.GeoNodes[i] = ngnode;
      break;
    }
  }
  var hit = -1;
  for (var i = 0; i < exports.KnownGeoNodes.length; i++) {
    var guuid = exports.KnownGeoNodes[i].device_uuid;
    if (guuid === nguuid) {
      if (guuid !== exports.DiscoveryUUID) {
        hit = i;
      }
      break;
    }
  }
  if (hit === -1) next(null, null);
  else {
    var knode = exports.KnownGeoNodes[hit];
    var kcl   = knode.cluster_leader;
    var gcl   = ngnode.cluster_leader;
    if (cmp_cluster_leader(kcl, gcl)) {
      ZH.t('updateGeoNode: NEW CLUSTER LEADER: RU: ' + nguuid);
      var net = ZH.CreateNetPerRequest(ZH.Central);
      ZVote.ReplaceKnownGeoNode(net.plugin, net.collections, ngnode, next);
    } else {
      exports.KnownGeoNodes[hit] = ngnode;
      next(null, null);
    }
  }
}
  
function update_geo_node_alive_status(plugin, collections, gnode, next) {
  gnode.ts  = ZH.GetMsTime();
  var dkey  = ZS.GeoNodeAliveStatus;
  var guuid = gnode.device_uuid;
  plugin.do_set_field(collections.global_coll, dkey, guuid, gnode, next);
}

exports.HandleGeoLeaderPing = function(plugin, collections, gnode, hres, next) {
  if (ZH.ChaosMode === 3) {
    if (gnode.device_uuid === ZH.MyDataCenter) {
      ZH.e('ALLOWING SELF PING -> CHAOS-MODE: ' + ZH.ChaosDescriptions[3]);
    } else {
      ZH.e('HandleGeoLeaderPing: CHAOS-MODE: ' + ZH.ChaosDescriptions[3]);
      return next(new Error(ZH.ChaosDescriptions[3]), hres);
    }
  }
  ZH.t('ZCLS.HandleGeoLeaderPing: GNODE: ' + JSON.stringify(gnode));
  update_geo_node_alive_status(plugin, collections, gnode,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      var guuid  = gnode.device_uuid;
      var kgnode = exports.GetKnownGeoNodeByUUID(guuid);
      if (kgnode) {
        var ognode = exports.GetGeoNodeByUUID(guuid);
        if (ognode) ognode.ts = gnode.ts;
        updateGeoNode(gnode, function(nerr, nres) {
          next(nerr, hres);
        });
      } else {
        var ngnode = ZH.clone(gnode);
        delete(ngnode.ts);
        var nguuid = ngnode.device_uuid;
        ZH.t('HandleGeoLeaderPing: recreate_geo_client: RU: ' + nguuid);
        recreate_geo_client(nguuid, ngnode, true, function(rerr, rres) {
          if (rerr) next(rerr, hres);
          else {
            ZVote.ReplaceKnownGeoNode(plugin, collections, ngnode, next);
          }
        });
      }
    }
  });
}

exports.HandleGeoDataPing = function(plugin, collections, gnode, hres, next) {
  if (ZH.ChaosMode === 3) {
    ZH.e('HandleGeoDataPing: CHAOS-MODE: ' + ZH.ChaosDescriptions[3]);
    return next(new Error(ZH.ChaosDescriptions[3]), hres);
  }
  //ZH.t('ZCLS.HandleGeoDataPing: FROM: GU: ' + gnode.device_uuid);
  update_geo_node_alive_status(plugin, collections, gnode,
  function(serr, sres) {
    next(serr, hres);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO CLUSTER FORMATION------------------------------------------------------

exports.SaveSingleLastGeoCreated = function(plugin, collections,
                                            guuid, created, next) {
  ZH.t('ZCLS.SaveSingleLastGeoCreated: GU: ' + guuid + ' C: ' + created);
  var ccreated = exports.LastGeoCreated[guuid];
  if (ccreated && ccreated >= created) next(null, null);
  else {
    var dkey = ZS.DataCenterLastCreated;
    plugin.do_set_field(collections.global_coll, dkey, guuid, created,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        exports.LastGeoCreated[guuid] = created;
        next(null, null);
      }
    });
  }
}

exports.SaveLastGeoCreateds = function(plugin, collections, createds, next) {
  var dkey = ZS.DataCenterLastCreated;
  plugin.do_set(collections.global_coll, dkey, createds, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      delete(createds._id);
      exports.LastGeoCreated = createds;
      next(null, null);
    }
  });
}

function load_last_GeoCreated(plugin, collections, next) {
  ZH.t('load_last_GeoCreated');
  var dkey = ZS.DataCenterLastCreated;
  plugin.do_get(collections.global_coll, dkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length !== 0) {
        var createds = gres[0];
        delete(createds._id);
        for (var guuid in createds) {
          var created = createds[guuid];
          exports.LastGeoCreated[guuid] = created;
        }
        ZH.t('load_last_GeoCreated'); ZH.t(exports.LastGeoCreated);
      }
      next(null, null);
    }
  });
}

function debug_handle_geo_state_change(duuid, gnodes) {
  ZH.t('ZCLS.AnnounceNewGeoCluster: U: ' + duuid                      +
       ' #DC: '                          + gnodes.length              +
       ' CentralSynced: '                + ZH.CentralSynced           + 
       ' GeoNetworkPartitionMode: '      + ZH.GeoNetworkPartitionMode +
       ' GeoMajority: '                  + ZH.GeoMajority             +
       ' GeoVoteCompleted: '             + ZVote.GeoVoteCompleted);
}

function leader_push_storage_new_geo_cluster(net, duuid, gterm, gnodes,
                                             gnetpart, gmaj, csynced,
                                             send_dco, next) {
  if (!ZH.AmClusterLeader) next(null, null);
  else {
    ZDQ.PushStorageAnnounceNewGeoCluster(net.plugin, net.collections,
                                         gterm, gnodes, gnetpart,
                                         gmaj, csynced, send_dco, next);
  }
}

function get_geo_node_summary(gnode) {
  return {device_uuid : gnode.device_uuid,
          wss         : gnode.wss,
          socket      : gnode.socket};
}

function get_geo_nodes_summary(gnodes) {
  var sgnodes = [];
  for (var i = 0; i < gnodes.length; i++) {
    var gnode = gnodes[i];
    sgnodes.push(get_geo_node_summary(gnode));
  }
  return sgnodes;
}

function send_all_subscribers_geo_state_change(net, gnodes) {
  for (var suuid in ZH.CentralWsconnMap) {
    var sub = ZH.CreateSub(suuid);
    ZPio.SendSubscriberGeoStateChange(net.plugin, net.collections, sub, gnodes);
  }
  for (var suuid in ZH.CentralSocketMap) {
    var sub = ZH.CreateSub(suuid);
    ZPio.SendSubscriberGeoStateChange(net.plugin, net.collections, sub, gnodes);
  }
  for (var suuid in ZH.CentralHttpsMap) {
    var sub = ZH.CreateSub(suuid);
    ZPio.SendSubscriberGeoStateChange(net.plugin, net.collections, sub, gnodes);
  }
}

function router_handle_announce_new_geo_cluster(net, duuid, gterm, gnodes,
                                                gnetpart, gmaj, csynced,
                                                send_dco, next) {
  leader_push_storage_new_geo_cluster(net, duuid, gterm, gnodes,
                                      gnetpart, gmaj, csynced, send_dco,
  function(perr, pres) {
    if (perr) next(perr, null);
    else {
      ZVote.UnionKnownGeoNodes(net.plugin, net.collections, gnodes,
       function(uerr, ures) {
        if (uerr) next(uerr);
        else {
          var sgnodes = get_geo_nodes_summary(gnodes);
          send_all_subscribers_geo_state_change(net, sgnodes);
          if (duuid !== ZH.MyUUID) {
            init_geo_cluster_mesh(ZH.OnErrLog);
          }
          next(null, null);
        }
      });
    }
  });
}

function both_handle_announce_new_geo_cluster(net, duuid, gterm, gnodes,
                                              gnetpart, gmaj, csynced,
                                              send_dco, next) {
  router_handle_announce_new_geo_cluster(net, duuid, gterm, gnodes, gnetpart,
                                         gmaj, csynced, send_dco,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      storage_handle_announce_new_geo_cluster(net, gnodes, send_dco, next);
    }
  });
}

exports.HandleAnnounceNewGeoCluster = function(net, duuid, gterm, gnodes,
                                               gnetpart, gmaj, csynced,
                                               send_dco, next) {
  ZVote.GeoVoteCompleted     = true;
  ZVote.GeoTermNumber        = gterm;
  exports.GeoNodes           = ZH.clone(gnodes);
  ZH.GeoNetworkPartitionMode = gnetpart;
  ZH.GeoMajority             = gmaj;
  ZH.t('ZCLS.HandleAnnounceNewGeoCluster: NEW: GeoNodes');
  ZH.t(exports.GeoNodes);
  debug_handle_geo_state_change(duuid, gnodes)
  if (ZH.AmBoth) {
    both_handle_announce_new_geo_cluster(net, duuid, gterm, gnodes,
                                         gnetpart, gmaj, csynced,
                                         send_dco, next);
  } else if (ZH.AmRouter) {
    router_handle_announce_new_geo_cluster(net, duuid, gterm, gnodes, gnetpart,
                                           gmaj, csynced, send_dco, next);
  } else {
    storage_handle_announce_new_geo_cluster(net, gnodes, send_dco, next);
  }
}

function storage_handle_announce_new_geo_cluster(net, gnodes, send_dco, next) {
  ZVote.UnionKnownGeoNodes(net.plugin, net.collections, gnodes,
  function(uerr, ures) {
    if (uerr) next(uerr, null);
    else      ZGack.StoragePostGeoVoteCleanupDirtyDeltas(net, next);
  });
}

exports.StorageHandleAnnounceNewGeoCluster = function(net, duuid, gterm, gnodes,
                                                      gnetpart, gmaj, csynced,
                                                      send_dco, next) {
  ZVote.GeoVoteCompleted     = true;
  ZVote.GeoTermNumber        = gterm;
  exports.GeoNodes           = ZH.clone(gnodes);
  ZH.GeoNetworkPartitionMode = gnetpart;
  ZH.GeoMajority             = gmaj;
  ZH.t('ZCLS.StorageHandleAnnounceNewGeoCluster: NEW: GeoNodes');
  ZH.t(exports.GeoNodes);
  debug_handle_geo_state_change(duuid, gnodes)
  storage_handle_announce_new_geo_cluster(net, gnodes, send_dco, next);
}

exports.HandleClusterStatus = function(plugin, collections,
                                       csynced, hres, next) {
  if (csynced) {
    ZH.CentralSynced = true;
    ZH.e('ZCLS.HandleClusterStatus: ZH.CentralSynced: ' + ZH.CentralSynced);
  }
  hres.cluster_nodes  = exports.ClusterNodes; // Used in response
  hres.cluster_synced = ZH.CentralSynced;     // Used in response
  next(null, hres);
}

// NOTE: RemoteDataCenter Leader may have changed (Local Leader must switch)
exports.HandleAnnounceNewCluster = function(plugin, collections,
                                            guuid, cnodes, next) {
  if (!ZH.AmRouter || !ZH.AmClusterLeader) return next(null, null);;
  ZH.t('HandleAnnounceNewCluster: GU: ' + guuid + ' #CN: ' + cnodes.length);
  var lhit = -1;
  for (var i = 0; i < cnodes.length; i++) {
    var cnode = cnodes[i];
    if (cnode.leader) {
      lhit = i;
      break;
    }
  }
  if (lhit === -1) next(null, null);
  else {
    var cnode = cnodes[lhit];
    resetGeoNodeLeader(guuid, cnode, true, function(nerr, nres) {
      if (nerr) next(nerr);
      else {
        ZH.t('HandleAnnounceNewCluster: recreate_geo_client: GU: ' + guuid);
        recreate_geo_client_from_cnode(guuid, cnode, true, next);
      }
    });
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLUSTER SUMMARY ------------------------------------------------------------

exports.SummarizeGeoNodesForAgent = function() {
  return get_geo_nodes_summary(exports.GeoNodes);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATACENTER DISCOVERY ------------------------------------------------------

var RetrySendDiscoveryTimer = null;

exports.DiscoveryUUID       = "ZyncDiscovery";
var SendDiscoveryPending    = false;

function create_discovery_gnode() {
  return {device_uuid : exports.DiscoveryUUID,
          backend : {
            server : {
              hostname : ZH.Discovery.hostname,
              port     : ZH.Discovery.port,
            },
            load_balancer : {
              hostname : ZH.Discovery.hostname,
              port     : ZH.Discovery.port,
            }
          },
          discovery   : true };
}

function handle_discovery_response(err, hres) {
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('handle_discovery_response: ERROR: ' + etxt);
  } else {
    var cuuid = hres.device.uuid;
    ZH.t('handle_discovery_response: CU: ' + cuuid);
    if (RetrySendDiscoveryTimer) {
      clearTimeout(RetrySendDiscoveryTimer);
      RetrySendDiscoveryTimer = null;
    }
    if (cuuid === ZH.MyUUID) {
      ZH.t('handle_discovery_response: FROM SELF: -> NO-OP');
    } else {
      var net    = ZH.CreateNetPerRequest(ZH.Central);
      var gnodes = hres.geo_nodes;
      ZVote.UnionKnownGeoNodes(net.plugin, net.collections,
                               gnodes, ZH.OnErrLog);
    }
  }
}

function send_discovery(next) {
  if (!ZH.AmRouter               ||
      !ZH.AmClusterLeader        ||
      ZH.CentralDisableDiscovery ||
      SendDiscoveryPending)         return next(null, null);
  ZH.t('SendDiscoveryPending: ' + SendDiscoveryPending);
  //ZH.t('DISCOVERY'); ZH.t(ZH.Discovery);
  SendDiscoveryPending = true;
  var dgnode           = create_discovery_gnode();
  if (!exports.GetKnownGeoNodeByUUID(dgnode.device_uuid)) {
    exports.KnownGeoNodes.push(dgnode);
    exports.KnownGeoNodes.sort(cmp_device_uuid);
  }
  recreate_geo_client(dgnode.device_uuid, dgnode, false, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      var net    = ZH.CreateNetPerRequest(ZH.Central);
      var mgnode = ZVote.GetMyGeoNode(false);
      ZPio.SendDiscovery(net.plugin, net.collections, dgnode, mgnode,
                         handle_discovery_response);
      SendDiscoveryPending = false;
      next(null, null);
      //NOTE: BELOW is ASYNC
      if (RetrySendDiscoveryTimer) return;
      ZH.t('DataCenterDiscovery: RESEND: SLEEP: ' + RetrySendDiscoverySleep);
      RetrySendDiscoveryTimer = setTimeout(function() {
        ZH.t('DISCOVERY TIMEOUT -> RESEND');
        RetrySendDiscoveryTimer = null;
        send_discovery(ZH.OnErrLog);
      }, RetrySendDiscoverySleep);
    }
  });
}

exports.HandleDataCenterDiscovery = function(plugin, collections,
                                             guuid, dgnode, hres, next) {
  ZH.t('ZCLS.HandleDataCenterDiscovery: GU: ' + guuid); ZH.t(dgnode);
  if (guuid === ZH.MyDataCenter) {
    if (ZH.CentralSynced === true) {
      ZH.t('DataCenterDiscovery on SYNCED DATACENTER -> NO-OP');
      hres.geo_nodes = exports.GeoNodes; // Used in response
      next(null, hres);
    } else {
      next(new Error(ZS.Errors.CentralNotSynced), hres);
      if (RetrySendDiscoveryTimer) return;
      //NOTE: BELOW is ASYNC
      ZH.t('SELF-Discovery: RESEND: SLEEP: ' + RetrySendDiscoveryTimer);
      RetrySendDiscoveryTimer = setTimeout(function() {
        ZH.t('SELF-DISCOVERY TIMEOUT -> RESEND');
        RetrySendDiscoveryTimer = null;
        send_discovery(ZH.OnErrLog);
      }, RetrySendDiscoverySleep);
    }
  } else {
    if (exports.GeoNodes.length === 0) {
      next(new Error(ZS.Errors.InGeoSync), hres);
    } else {
      hres.geo_nodes = exports.GeoNodes; // Used in response
      var dkey       = ZS.KnownDataCenters;
      plugin.do_set_field(collections.global_coll, dkey, guuid, dgnode,
      function(serr, sres) {
        next(serr, hres);
      });
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REMOVE DATACENTER----------------------------------------------------------

exports.CentralHandleShutdown = function(plugin, collections) {
  ZH.e('Received SHUTDOWN -> EXITING');
  process.exit();
}

function final_remove_datacenter(net, gnode, next) {
  kill_previous_gclient(gnode, false, function(kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      if (exports.AmPrimaryDataCenter()) {
        exports.ForceGeoVote = true;
      }
      ZH.t('POST: RemoveDataCenterKnownGeoNodes');
      ZH.t(exports.KnownGeoNodes);
      next(null, null);
    }
  });
}

function do_central_geo_remove_datacenter(net, dcuuid, am_router, next) {
  var dkey = ZS.GeoNodeAliveStatus;
  net.plugin.do_unset_field(net.collections.global_coll, dkey, dcuuid,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var dkey = ZS.KnownDataCenters;
      net.plugin.do_unset_field(net.collections.global_coll, dkey, dcuuid,
      function(uerr, ures) {
        if (uerr) next(uerr, null);
        else {
          var gnode = ZH.clone(exports.GetKnownGeoNodeByUUID(dcuuid));
          exports.RemoveGeoNodeByUUID     (dcuuid);
          exports.RemoveKnownGeoNodeByUUID(dcuuid);
          if (!ZH.AmClusterLeader) {
            final_remove_datacenter(net, gnode, next);
          } else {
            if (!am_router) next(null, null);
            else {
              kill_previous_gclient(gnode, true, function(kerr, kres) {
                if (kerr) next(kerr, null);
                else      final_remove_datacenter(net, gnode, next);
              });
            }
          }
        }
      });
    }
  });
}

exports.HandleStorageGeoRemoveDataCenter = function(net, dcuuid, next) {
  ZH.e('ZCLS.HandleStorageGeoRemoveDataCenter: DC: ' + dcuuid);
  do_central_geo_remove_datacenter(net, dcuuid, false, next);
}

exports.CentralHandleGeoRemoveDataCenter = function(net, dcuuid, next) {
  ZH.e('ZCLS.CentralHandleGeoRemoveDataCenter: DC: ' + dcuuid);
  if (dcuuid === ZH.MyDataCenter) {
    ZH.e('GeoRemoveDataCenter: SELF -> BroadcastShutdown');
    ZPio.BroadcastShutdown(net.plugin, net.collections);
  } else {
    var gnode = exports.GetKnownGeoNodeByUUID(dcuuid);
    if (!gnode) {
      ZH.e('ERROR: RemoveDataCenter: UNKNOWN DC: ' + dcuuid);
      ZH.e(exports.KnownGeoNodes);
      return;
    }
    ZDQ.PushStorageGeoRemoveDataCenter(net.plugin, net.collections, dcuuid,
    function(perr, pres) {
      if (perr) next(perr, null);
      else      do_central_geo_remove_datacenter(net, dcuuid, true, next);
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RPC-ID STORGE & ROUTER NODES ----------------------------------------------

// NOTE StorageNode & RouterNode are ONLY used for UUIDs in RPC-IDs
exports.StorageNode = null;
exports.StorageUUID = "ZyncStorage";
exports.RouterNode  = null;
exports.RouterUUID  = "ZyncRouter";

function create_storage_cnode() {
  return {device_uuid : exports.StorageUUID,
          hostname    : "STORAGE",
          backend     : {},
          storage     : true};
}

function create_router_cnode() {
  return {device_uuid : exports.RouterUUID,
          hostname    : "ROUTER",
          backend     : {},
          router      : true};
}

exports.InitStorageConnection = function(net, next) {
  if (!ZH.AmRouter) return next(null, null);
  ZH.e('ZCLS.InitStorageConnection');
  var cnode           = create_storage_cnode();
  exports.StorageNode = cnode;
}

exports.InitRouterConnection = function(net, next) {
  if (!ZH.AmStorage) return next(null, null);
  ZH.e('ZCLS.InitRouterConnection');
  var cnode           = create_router_cnode();
  exports.RouterNode  = cnode;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET-CLUSTER-INFO ----------------------------------------------------------

exports.HandleRouterStorageOnline = function(plugin, collections,
                                             csynced, hres, next) {
  ZH.CentralSynced = csynced;
  ZH.e('ZCLS.HandleRouterStorageOnline: ZH.CentralSynced: ' +
        ZH.CentralSynced);
  hres.router_data = {}; // NOTE: Used in response
  hres.router_data.geo_term_number       = ZVote.GeoTermNumber;
  hres.router_data.geo_nodes             = exports.GeoNodes;
  hres.router_data.cluster_synced        = ZH.CentralSynced;
  hres.router_data.geo_network_partition = ZH.GeoNetworkPartitionMode;
  hres.router_data.geo_majority          = ZH.GeoMajority;
  next(null, hres);
  // NOTE: ZPio.BroadcastClusterStatus() is ASYNC
  ZPio.BroadcastClusterStatus(plugin, collections,
                              ZH.CentralSynced, ZH.OnErrLog);

}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET-CLUSTER-INFO ----------------------------------------------------------

exports.CentralHandleGetClusterInfo = function(plugin, collections,
                                               hres, next) {
  ZH.t('ZCLS.CentralHandleGetClusterInfo');
  hres.cluster_synced      = ZH.CentralSynced;     // Used in response
  hres.geo_term_number     = ZVote.GeoTermNumber;
  hres.geo_nodes           = exports.GeoNodes;
  hres.cluster_term_number = ZVote.TermNumber;
  hres.cluster_nodes       = exports.ClusterNodes;
  next(null, hres);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

//NOTE: Used by ZVote.start_new_cluster/geo_vote() &
//              ZPart.simple_compute_partition_table()
exports.CmpDeviceUuid = function(a, b) {
  return cmp_device_uuid(a, b);
}

//NOTE: Used by ZVote.HandleClusterStateChangeCommit(),
//              ZVote.start_new_cluster_vote(),
exports.InitClusterMesh = function(next) {
  init_cluster_mesh(next);
}

//NOTE: Used by ZVote.HandleGeoStateChangeCommit()
exports.InitGeoClusterMesh = function(next) {
  init_geo_cluster_mesh(next);
}


// NOTE: Used by ZVote.HandleClusterStateChangeCommit()
exports.PruneClusterClients = function(next) {
  prune_ClusterClients(next);
}

//NOTE: Used by ZVote.persist_KnownDataCenters()
exports.RecreateGeoClient = function(guuid, gnode, leader, next) {
  ZH.t('ZCLS.RecreateGeoClient: RU: ' + guuid);
  recreate_geo_client(guuid, gnode, leader, next);
}

//NOTE: Used by ZVote.geo_broadcast_cluster_state_change()
exports.SendDiscovery = function(next) {
  send_discovery(next);
}

// NOTE: Used by ZPio.geo_leader_broadcast()
exports.GetGeoClient = function(guuid, leader) {
  return get_Gclient(guuid, leader);
}

//NOTE: Used by zcentral.redirect_discovery()
exports.GetDiscoveryNode = function() {
  return create_discovery_gnode();
}


