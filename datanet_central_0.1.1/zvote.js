"use strict";

require('./setImmediate');

var ZCloud = require('./zcloud_server');
var ZRAD   = require('./zremote_apply_delta');
var ZNM    = require('./zneedmerge');
var ZPio   = require('./zpio');
var ZDConn = require('./zdconn');
var ZCLS   = require('./zcluster');
var ZPart  = require('./zpartition');
var ZAio   = require('./zaio');
var ZFix   = require('./zfixlog');
var ZASC   = require('./zapp_server_cluster');
var ZMCC   = require('./zmemcache_server_cluster');
var ZMDC   = require('./zmemory_data_cache');
var ZDQ    = require('./zdata_queue');
var ZS     = require('./zshared');
var ZH     = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

// NOTE: on EVERY ClusterStateChangeCommit -> close ALL connections
//       this forces an AgentOnline request, which insures up-to-dateness
function close_all_agent_wsconn(net) {
  ZH.t('ZVote.close_all_agent_wsconn');
  for (var suuid in ZH.CentralWsconnMap) {
    var wsconn = ZH.CentralWsconnMap[suuid];
    if (wsconn) wsconn.close();
  }
  for (var suuid in ZH.CentralSocketMap) {
    var sock = ZH.CentralSocketMap[suuid];
    if (sock) sock.end();
  }
  for (var suuid in ZH.CentralHttpsMap) {
    var sub = {UUID : suuid};
    // NOTE: ZPio.SendSubscriberReconnect() is ASYNC
    ZPio.SendSubscriberReconnect(net.plugin, net.collections, sub);
    var hso = ZH.CentralHttpsMap[suuid];
    if (hso) ZCloud.RemoveHttpsMap(suuid, null);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLUSTER VOTING ------------------------------------------------------------

// NOTE: cluster-nodes are deemed down if their timeofday() is too old
//       there are tons of clock-skew issues with this SIMPLE solution

exports.NumberFailedNodesForClusterNetworkPartition = 2;
exports.NumberFailedNodesForGeoNetworkPartition     = 2;

exports.NodeStart                            = ZH.GetMsTime();

exports.TermNumber                           = 1;
exports.ClusterNodeMaximumAllowedStaleness   = 10000;

exports.GeoTermNumber                        = 1;
exports.GeoNodeMaximumAllowedStaleness       = 25000;

var NewNodeMinimumHeartBeats = 3;

var MinNodeHeartbeatInterval =   600;
var MaxNodeHeartbeatInterval =   900;
// NOTE: 3X+ MaxNodeHeartbeatInterval
var ClusterWarmupPeriod      =  3000;

var MinClusterCheckInterval  =  2500;
var MaxClusterCheckInterval  =  5000;
var ClusterCheckInProgress   = false;

exports.ClusterVoteCompleted = false;
exports.ClusterBorn          = null;


var MinGeoLeaderHeartbeatInterval  = 1000;
var MaxGeoLeaderHeartbeatInterval  = 1500;
// NOTE: 3X+ MinGeoLeaderHeartbeatInterval
var GeoLeaderWarmupPeriod          = 4000;

var MinGeoCheckInterval            = 3500;
var MaxGeoCheckInterval            = 6000;

var GeoCheckInProgress             = false;
exports.GeoVoteCompleted           = false;

var MinGeoDataHeartbeatInterval  =  2100;
var MaxGeoDataHeartbeatInterval  =  2600;

//TODO this is a BAD FLOW -> get rid of RollbackClusterNodes
var RollbackClusterNodes     = null;

var AlreadyVoted             = [];
var NumVotes                 = {};
var NumCommits               = {};
var LastClusterCommitId      = null;

var ClusterCheckTimer        = null;

var ElectionCount = 0;
function generate_vote_id() {
  ElectionCount += 1;
  return ZH.MyUUID + '_' + ElectionCount;
}

function should_send_datacenter_online(gnodes) {
  if (ZH.GeoMajority     === false) return false;
  if (ZH.AmClusterLeader === false) return false;
  if (gnodes.length      === 1)     return false;
  else                              return ZH.CentralSynced ? false : true;
}

function count_cluster_commit(plugin, collections, err, hres) {
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('count_cluster_commit: ERROR: ' + etxt);
  } else {
    rerun_cluster_check(plugin, collections);
    var vid          = hres.vote_id;    
    NumCommits[vid] += 1;
    ZH.l('count_cluster_commit: V: ' + vid + ' #C: ' + NumCommits[vid]);
    if (NumCommits[vid] === ZCLS.ClusterNodes.length) {
      ZH.l('COMMIT SUCCEEDED ON ALL CLUSTER-NODES: #CN: ' +
           ZCLS.ClusterNodes.length);
      LastClusterCommitId = null;
    }
  }
}

function clone_cluster_leader() {
  var cleader = ZH.clone(ZH.MyClusterLeader);
  if (cleader) {
    delete(cleader.ip);
    delete(cleader.leader);
  }
  return cleader;
}

function get_my_geo_node(force) {
  var kgnode = force ? null : ZCLS.GetKnownGeoNodeByUUID(ZH.MyDataCenter);
  if (kgnode) return kgnode;
  else {
    var mgnode            = ZH.clone(ZH.MyGeoNode);
    mgnode.cluster_leader = clone_cluster_leader();
    if (!mgnode.cluster_leader) {
      mgnode.cluster_leader = ZH.clone(ZCLS.MyNode);
    }
    if (ZCLS.AmPrimaryDataCenter()) mgnode.primary = true;
    return mgnode;
  }
}

function assign_my_cluster_leader() {
  ZH.t('assign_my_cluster_leader');
  for (var i = 0; i < ZCLS.ClusterNodes.length; i++) {
    var cnode = ZCLS.ClusterNodes[i];
    if (cnode.leader) {
      ZH.MyClusterLeader = ZH.clone(cnode);
      ZH.l('assign_my_cluster_leader: CNODE: ' + JSON.stringify(cnode));
      ZH.AmClusterLeader = (cnode.device_uuid === ZH.MyUUID);
      return;
    }
  }
  //throw(new Error(ZS.Errors.MeNotInCluster));
}

function handle_cluster_status_reply(err, hres) {
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('handle_cluster_status_reply: ERROR: ' + etxt);
  } else {
    var csynced = hres.cluster_synced;
    if (csynced) ZH.CentralSynced = true;
    ZH.l('handle_cluster_status_reply: ZH.CentralSynced: ' + ZH.CentralSynced);
  }
}

function async_storage_online_processed(net, send_dco) {
  ZPio.BroadcastClusterStatus(net.plugin, net.collections,
                              ZH.CentralSynced, ZH.OnErrLog);
  ZNM.StoragePrimaryHandleAnnounceNewGeoCluster(net, ZH.OnErrLog);
  if (send_dco) {
    ZDQ.PushRouterSendDataCenterOnline(net.plugin, net.collections,
                                       ZH.OnErrLog);
  }
}
function storage_online_processed(err, hres) {
  var rdata = hres.router_data;
  if (!rdata) {
    ZH.e('ERROR: storage_online_processed -> NO ROUTER_DATA'); ZH.e(hres);
  } else {
    var net      = ZH.CreateNetPerRequest(ZH.Central);
    var next     = ZH.OnErrLog;
    var gterm    = rdata.geo_term_number;
    var gnodes   = rdata.geo_nodes;
    var gnetpart = rdata.geo_network_partition;
    var gmaj     = rdata.geo_majority;
    var csynced  = rdata.cluster_synced;
    var send_dco = should_send_datacenter_online(gnodes);
    ZH.e('storage_online_processed: ZH.CentralSynced: ' + ZH.CentralSynced +
         ' send_dco: ' + send_dco);
    ZCLS.StorageHandleAnnounceNewGeoCluster(net, -1, gterm, gnodes, gnetpart,
                                            gmaj, csynced, send_dco,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        // NOTE: async_storage_online_processed() is ASYNC
        async_storage_online_processed(net, send_dco);
        next(null, null);
      }
    });
  }
}

function async_router_leader_post_cluster_state_commit(net) {
  var cnodes = ZCLS.ClusterNodes;
  var gnodes = ZCLS.GetKnownDataGeoNodes();
  ZPio.GeoBroadcastAnnounceNewCluster(net.plugin, net.collections,
                                      gnodes, cnodes);
  if (ZCLS.KnownGeoNodes.length <= 1) {
    ZCLS.SendDiscovery(ZH.OnErrLog);
  }
}

function leader_central_post_cluster_state_commit(net, next) {
  if (!ZH.AmClusterLeader) next(null, null);
  else {
    if (ZH.AmBoth) {
      next(null, null);
    } else if (ZH.AmRouter) {
      // NOTE: async_router_leader_post_cluster_state_commit() is ASYNC
      async_router_leader_post_cluster_state_commit(net);
      next(null, null);
    } else {
      // ZDQ.RouterQueueRequestStorageOnline() is ASYNC
      ZDQ.RouterQueueRequestStorageOnline(net.plugin, net.collections,
                                          storage_online_processed);
      next(null, null);
    }
  }
}

function reinitialize_data_queue_connection(next) {
  if (ZH.AmBoth) next(null, null);
  else           ZDQ.ReinitializeDataQueueConnection(next);
}

function central_post_cluster_state_commit(net, next) {
  ZH.t('central_post_cluster_state_commit: L: ' + ZH.AmClusterLeader);
  reinitialize_data_queue_connection(function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var mgnode = get_my_geo_node(true); // has up-to-date leader
      ZH.l('central_post_cluster_state_commit: MGNODE' +
           JSON.stringify(mgnode));
      replace_KnownGeoNode(net.plugin, net.collections, mgnode,
      function(rerr, rres) {
        if (rerr) next(rerr, null);
        else {
          ZCLS.PruneClusterClients(function(perr, pres) {
            if (perr) next(perr, null);
            else {
              // NOTE: ZDConn.SignalCentralToSyncKeysDaemon() is ASYNC
              ZDConn.SignalCentralToSyncKeysDaemon();
              leader_central_post_cluster_state_commit(net, next);
            }
          });
        }
      });
    }
  });
}

function agent_post_cluster_state_commit(net, next) {
  ZCLS.PruneClusterClients(function(perr, pres) {
    if (perr) next(perr, null);
    else {
      if (ZH.Agent.AppServerClusterConfig) {
        ZASC.ReinitializeAppServerCluster(net, function(serr, sres) {
          if (serr) next(serr, null);
          else {
            if (!ZH.AmClusterLeader) next(null, null);
            else {
              ZASC.AgentAnnounceNewAppServerCluster(net, next);
            }
          }
        });
      } else if (ZH.Agent.MemcacheClusterConfig) {
        ZMCC.ReinitializeMemcacheCluster(net, function(serr, sres) {
          if (serr) next(serr, null);
          else {
            if (!ZH.AmClusterLeader) next(null, null);
            else {
              ZMCC.AgentAnnounceNewMemcacheCluster(net, next);
            }
          }
        });
      }
    }
  });
}

function set_cluster_network_partition_status(net, cnetpart, cnodes, csynced,
                                              next) {
  if (ZH.ClusterNetworkPartitionMode === false) {
    if (cnetpart === true) { // First time set to PARTITIONEd
      ZH.ClusterNetworkPartitionMode = true;
      ZH.CentralSynced               = false;
      var rbnodes     = RollbackClusterNodes;
      var full_ncnode = rbnodes ? rbnodes.length : ZCLS.ClusterNodes.length;
      next(null, full_ncnode);
    } else {
      ZH.ClusterNetworkPartitionMode = false;
      ZH.CentralSynced               = csynced;
      next(null, cnodes.length);
    }
  } else { // Currently in ClusterNetworkPartition
    var ckey = ZS.ClusterState;
    net.plugin.do_get_field(net.collections.global_coll,
                            ckey, "full_cluster_node_count",
    function(gerr, full_ncnode) {
      if (gerr) next(gerr, null);
      else {
        if (cnetpart === true) {
          ZH.ClusterNetworkPartitionMode = true;
          ZH.CentralSynced               = false;
          next(null, full_ncnode);
        } else {
          var num_cnode = cnodes.length;
          ZH.t('num_cnode: ' + num_cnode + ' FULL #CN: ' + full_ncnode);
          if (num_cnode >= full_ncnode) {
            ZH.ClusterNetworkPartitionMode = false;
            ZH.CentralSynced               = csynced;
          } else {
            ZH.ClusterNetworkPartitionMode = true;
            ZH.CentralSynced               = false;
          }
          next(null, full_ncnode);
        }
      }
    });
  }
}

function cluster_leader_finalize_cluster_commit(net, term, vid, full_ncnode,
                                                next) {
  if (!ZH.AmClusterLeader) next(null, null);
  else {
    var state = {term_number               : term,
                 vote_id                   : vid,
                 cluster_nodes             : ZCLS.ClusterNodes,
                 cluster_network_partition : ZH.ClusterNetworkPartitionMode,
                 full_cluster_node_count   : full_ncnode,
                 cluster_born              : exports.ClusterBorn};
    ZH.t('cluster_leader_finalize_cluster_commit'); ZH.t(state);
    net.plugin.do_set(net.collections.global_coll,
                      ZS.ClusterState, state, next);
  }
}

function finalize_cluster_state_change_commit(net, term, vid, full_ncnode,
                                              next) {
  cluster_leader_finalize_cluster_commit(net, term, vid, full_ncnode, 
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      ZFix.Init(function(ierr, ires) {
        if (ierr) next(ierr, null)
        else {
          if (!ZH.AmCentral) agent_post_cluster_state_commit  (net, next);
          else               central_post_cluster_state_commit(net, next);
        }
      });
    }
  });
}

function debug_cluster_state_change_commit(full_ncnode) {
  ZH.l('HandleClusterStateChangeCommit: RESULTS;');
  ZH.l('ClusterNodes');              ZH.l(ZCLS.ClusterNodes);
  ZH.l('PartitionTable');            ZH.l(ZPart.PartitionTable);
  ZH.l('TermNumber: '              + exports.TermNumber);
  ZH.l('ZH.CentralSynced: '        + ZH.CentralSynced);
  ZH.l('ClusterNetworkPartition: ' + ZH.ClusterNetworkPartitionMode);
  ZH.l('full_ncnode: '             + full_ncnode);
}

exports.HandleClusterStateChangeCommit = function(net, term, vid, cnodes,
                                                  csynced, cnetpart, ptbl,
                                                  hres, next) {
  rerun_cluster_check(net.plugin, net.collections);
  ZH.e('HandleClusterStateChangeCommit: TN: ' + term + ' V: ' + vid);
  set_cluster_network_partition_status(net, cnetpart, cnodes, csynced,
  function(cerr, full_ncnode) {
    if (cerr) next(cerr, hres);
    else {
      ZPart.SavePartitionTable(net, cnodes, ptbl, function(serr, sres) {
        if (serr) next(serr, hres);
        else {
          ZH.FixLogActive              = true; // TRUE until set false
          exports.TermNumber           = term; // NOTE: NEW TermNumber
          ZCLS.ClusterNodes            = cnodes;
          RollbackClusterNodes         = null;
          ZCLS.ForceVote               = false;
          ZH.YetToClusterVote          = false;
          ZH.ClusterVoteInProgress     = false;
          exports.ClusterVoteCompleted = true;
          exports.ClusterBorn          = ZH.GetMsTime();
          debug_cluster_state_change_commit(full_ncnode);
          ZMDC.DropCache();
          close_all_agent_wsconn(net);
          assign_my_cluster_leader();
          ZCLS.InitClusterMesh(function(ierr, ires) {
            if (ierr) next(ierr, hres);
            else {
              hres.term_number = term;
              hres.vote_id     = vid;
              finalize_cluster_state_change_commit(net, term, vid, full_ncnode,
              function(ferr, fres) {
                next(ferr, hres);
              });
            }
          });
        }
      });
    }
  });
}

var AnyNodeInClusterSynced = false;

function count_cluster_vote(plugin, collections, err, hres, next) {
  if (err) {
    rerun_cluster_check(plugin, collections);
    if (err.message === ZS.Errors.OldTermNumber ||
        err.message === ZS.Errors.AlreadyVoted) {
      ZH.l('count_cluster_vote: ERROR: ' + err.message);
      ZH.l('count_cluster_vote: TermNumber: ' + exports.TermNumber +
           ' details.term_number: ' + err.details.term_number);
      if (err.details.term_number > exports.TermNumber) {
        exports.TermNumber = err.details.term_number + 1;
        ZH.l('count_cluster_vote: UPDATING TermNumber: ' + exports.TermNumber);
      }
    } else {
      ZH.l('count_cluster_vote: ERROR: ' + err.message);
    }
    next(null, null);
  } else {
    var term       = hres.term_number;
    var vid        = hres.vote_id;
    var cnodes     = hres.cluster_nodes;
    var csynced    = hres.cluster_synced;
    if (csynced) AnyNodeInClusterSynced = true; // MONOTONIC
    var cnetpart   = hres.cluster_network_partition;
    NumVotes[vid] += 1;
    ZH.l('count_cluster_vote: V: ' + vid + ' #V: ' + NumVotes[vid]);
    if (NumVotes[vid] === ZCLS.ClusterNodes.length) {
      ZH.l('ALL CLUSTER VOTES RECEIVED');
      NumCommits[vid]     = 0;
      LastClusterCommitId = vid;
      ZPart.ComputeNewFromOldPartitionTable(plugin, collections, cnodes,
      function(serr, ptbl) {
        if (serr) next(serr, null);
        else {
          var acsynced = AnyNodeInClusterSynced;
          // NOTE: ZPio.BroadcastClusterStateChangeCommit() is ASYNC
          ZPio.BroadcastClusterStateChangeCommit(plugin, collections,
                                                 term, vid, cnodes, acsynced,
                                                 cnetpart, ptbl,
          function(err, hres) { // NOTE: CLOSURE as 'next' argument
            count_cluster_commit(plugin, collections, err, hres);
          });
          AnyNodeInClusterSynced = false;
          next(null, null);
        }
      });
    }
  }
}

exports.HandleClusterStateChangeVote = function(net, term, vid, cnodes,
                                                csynced, cnetpart, hres, next) {
  ZH.e('HandleClusterStateChangeVote: T: ' + term + ' V: ' + vid);
  if        (AlreadyVoted[term]) {
    hres.error_details             = {};
    hres.error_details.term_number = exports.TermNumber;
    ZH.e('HandleClusterStateChangeVote: AlreadyVoted:' + 
         ' TermNumber: ' + exports.TermNumber);
    next(new Error(ZS.Errors.AlreadyVoted), hres);
  } else if (term < exports.TermNumber) {
    hres.error_details             = {};
    hres.error_details.term_number = exports.TermNumber;
    ZH.e('HandleClusterStateChangeVote: OldTermNumber:' + 
         ' TermNumber: ' + exports.TermNumber);
    next(new Error(ZS.Errors.OldTermNumber), hres);
  } else {
    rerun_cluster_check(net.plugin, net.collections);
    ZH.ClusterVoteInProgress = true;
    AlreadyVoted[term]       = true;
    var vkey                 = ZS.LastVoteTermNumber;
    net.plugin.do_set_field(net.collections.global_coll, vkey, ZH.MyUUID, term,
    function(serr, sres) {
      if (serr) next(serr, hres);
      else {
        hres.term_number               = term;
        hres.vote_id                   = vid;
        hres.cluster_nodes             = cnodes;
        hres.cluster_synced            = csynced || ZH.CentralSynced;
        hres.cluster_network_partition = cnetpart;
        next(null, hres);
      }
    });
  }
}

function start_new_cluster_vote(plugin, collections, cnodes) {
  cnodes.sort(ZCLS.CmpDeviceUuid);
  LastClusterCommitId      = null; // Commit comes after ALL Votes received
  exports.TermNumber      += 1;    // NOTE: NEW TermNumber
  var vid                  = generate_vote_id();
  var term                 = exports.TermNumber;
  ZH.e('start_new_cluster_vote: TermNumber: ' + term + ' V: ' + vid);
  var diff                 = ZCLS.ClusterNodes.length - cnodes.length;
  var cnetpart             = false
  if (ZH.CentralDisableCentralNetworkPartitions === false) {
    if (diff >= exports.NumberFailedNodesForClusterNetworkPartition) {
      cnetpart = true;
    }
  }
  NumVotes[vid]          = 0;
  AnyNodeInClusterSynced = false;
  RollbackClusterNodes   = ZH.clone(ZCLS.ClusterNodes);
  ZCLS.ClusterNodes      = cnodes;
  var mcnode             = ZCLS.GetMyClusterNode();
  if (!mcnode) mcnode    = ZH.clone(ZCLS.MyNode);
  for (var i = 0; i < ZCLS.ClusterNodes.length; i++) {
    ZCLS.ClusterNodes[i].leader = false;
  }
  mcnode.leader          = true;
  ZCLS.InitClusterMesh(function(ierr, ires) {
    if (ierr) ZH.e(ierr);
    else {
      var csynced = ZH.CentralSynced;
      var bfunc   = ZPio.BroadcastClusterStateChangeVote;
      bfunc(plugin, collections, term, vid, cnodes,
            csynced, cnetpart,
      function(err, hres) { // NOTE: CLOSURE as 'next' argument
        count_cluster_vote(plugin, collections, err, hres, ZH.OnErrLog);
      });
    }
  });
}

function post_analyze_cluster_status_central_not_synced(plugin, collections) {
  if (!ZH.AmCentral) throw(new Error("post_analyze_cluster LOGIC ERROR"));
  ZPio.BroadcastClusterStatus(plugin, collections, 
                              false, handle_cluster_status_reply);
  if (ZCLS.KnownGeoNodes.length <= 1) ZCLS.SendDiscovery(ZH.OnErrLog);
}

var NewNodeStats = {};
function analyze_cluster_status(plugin, collections, cstat, next) {
  var now     = ZH.GetMsTime();
  var changed = false;
  var ncnodes = [];
  for (var cuuid in cstat) {
    var cts   = cstat[cuuid].ts;
    var stale = now - cts;
    if (ZCLS.GetClusterNodeByUUID(cuuid)) {
      if (stale < exports.ClusterNodeMaximumAllowedStaleness) {
        ncnodes.push(cstat[cuuid].node); // Normal Healthy node
      } else {
        changed = true;
        ZH.l('CD: ' + cuuid + ' is DOWN: stale: ' + stale);
      }
    } else {
      if (stale < exports.ClusterNodeMaximumAllowedStaleness) {
        var nnode = NewNodeStats[cuuid];
        if (!nnode) {
          nnode                = ZH.clone(cstat[cuuid]);
          nnode.num_heartbeats = 0;
          NewNodeStats[cuuid]  = nnode;
        }
        nnode.num_heartbeats += 1;
        if (nnode.num_heartbeats > NewNodeMinimumHeartBeats) {
          ZH.l('ADD NEW NODE: CU: ' + cuuid); ZH.l(nnode);
          changed = true;
          ncnodes.push(cstat[cuuid].node);
        }
      } else {
        NewNodeStats[cuuid] = null;
      }
    }
  }
  next(null, null);
  // NOTE: BELOW is ASYNC
  if (changed) {
    start_new_cluster_vote(plugin, collections, ncnodes);
  } else {
    if (ZH.AmCentral) { // NOT FOR APP-SERVER-CLUSTER
      if (ZH.CentralSynced === false) {
        post_analyze_cluster_status_central_not_synced(plugin, collections);
      }
    }
  }
}

function fetch_cluster_status(plugin, collections, next) {
  var akey = ZS.ClusterNodeAliveStatus;
  plugin.do_get(collections.global_coll, akey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var cstat = {};
      if (gres.length !== 0) {
        cstat = gres[0];
        delete(cstat._id);
      }
      next(null, cstat);
    }
  });
}

function fetch_live_cluster_nodes(plugin, collections, next) {
  fetch_cluster_status(plugin, collections, function(ferr, cstat) {
    if (ferr) next(ferr, null);
    else {
      var now     = ZH.GetMsTime();
      var lcnodes = [];
      for (var cuuid in cstat) {
        var cts   = cstat[cuuid].ts;
        var stale = now - cts;
        if (stale <= exports.ClusterNodeMaximumAllowedStaleness) {
          lcnodes.push(cstat[cuuid].node);
        }
      }
      next(null, lcnodes);
    }
  });
}

function force_vote(plugin, collections, next) {
  ZH.t('FORCE CLUSTER_VOTE');
  fetch_live_cluster_nodes(plugin, collections, function(ferr, lcnodes) {
    if (ferr) next(ferr, null);
    else {
      var hit = false;
      for (var i = 0; i < lcnodes.length; i++) {
        var cnode = lcnodes[i];
        if (cnode.device_uuid === ZH.MyUUID) {
          cnode.leader = true;
          hit          = true;
          break;
        }
      }
      if (!hit) {
        var mcnode    = ZH.clone(ZCLS.MyNode);
        mcnode.leader = true;
        lcnodes.push(mcnode);
      }
      next(null, null);
      // NOTE: BELOW is ASYNC
      start_new_cluster_vote(plugin, collections, lcnodes);
    }
  });
}

function evaluate_last_cluster_commit() {
  if (LastClusterCommitId) {
    if (NumCommits[LastClusterCommitId] !== ZCLS.ClusterNodes.length) {
      ZH.l('FORCE CLUSTER_VOTE: #CMT: ' + NumCommits[LastClusterCommitId] +
                              ' #CN: '  + ZCLS.ClusterNodes.length);
      ZCLS.ForceVote = true;
    }
  }
  if (ZH.ClusterVoteInProgress) {
    ZH.l('FORCE CLUSTER_VOTE: LAST CLUSTER_VOTE NEVER ENDED');
    ZCLS.ForceVote = true;
  }
  ZH.ClusterVoteInProgress = false;
}

function reset_election() {
  if (RollbackClusterNodes) {
    ZH.l('->RollbackClusterNodes');
    ZCLS.ClusterNodes    = ZH.clone(RollbackClusterNodes);
    RollbackClusterNodes = null;
  }
}

function __run_cluster_check(plugin, collections, next) {
  evaluate_last_cluster_commit();
  if (ZCLS.ForceVote) {
    reset_election();
    force_vote(plugin, collections, next); 
  } else {
    var ts   = ZH.GetMsTime();
    var diff = ts - exports.NodeStart;
    if (diff < ClusterWarmupPeriod) next(null, null);
    else {
      reset_election();
      fetch_cluster_status(plugin, collections, function(gerr, cstat) {
        if (gerr) next(gerr, null);
        else {
          analyze_cluster_status(plugin, collections, cstat, next);
        }
      });
    }
  }
}

function run_cluster_check(plugin, collections) {
  ClusterCheckInProgress = true;
  __run_cluster_check(plugin, collections, function(cerr, cres) {
    if (cerr) ZH.e('run_cluster_check: ERROR: ' + cerr.message);
    rerun_cluster_check(plugin, collections);
  });
}

function next_run_cluster_check(plugin, collections) {
  if (ClusterCheckInProgress || ClusterCheckTimer) return;
  var min = MinClusterCheckInterval;
  var max = MaxClusterCheckInterval;
  var to  = min + ((max - min) * Math.random());
  //ZH.t('next_run_cluster_check: to: ' + to);
  ClusterCheckTimer = setTimeout(function() {
    run_cluster_check(plugin, collections);
  }, to);
}

function rerun_cluster_check(plugin, collections) {
  if (ClusterCheckTimer) {
    clearTimeout(ClusterCheckTimer);
    ClusterCheckTimer = null;
  }
  ClusterCheckInProgress = false;
  next_run_cluster_check(plugin, collections);
}

function __node_heartbeat(plugin, collections, next) {
  //ZH.t('node_heartbeat');
  var ts     = ZH.GetMsTime();
  var akey   = ZS.ClusterNodeAliveStatus;
  var mcnode = ZCLS.GetMyClusterNode();
  if (!mcnode) mcnode = ZH.clone(ZCLS.MyNode);
  var aentry = {ts : ts, node : mcnode};
  plugin.do_set_field(collections.global_coll, akey, ZH.MyUUID, aentry, next);
}

var NodeHeartbeatTimer = null;
function node_heartbeat(plugin, collections) {
  __node_heartbeat(plugin, collections, function(nerr, nres) {
    if (nerr) ZH.e('node_heartbeat: ERROR: ' + nerr.message);
    var min = MinNodeHeartbeatInterval;
    var max = MaxNodeHeartbeatInterval;
    var to  = min + ((max - min) * Math.random());
    //ZH.t('node_heartbeat: to: ' + to);
    NodeHeartbeatTimer = setTimeout(function() {
      node_heartbeat(plugin, collections);
    }, to);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO VOTING ----------------------------------------------------------------


var AlreadyGeoVoted        = [];
var NumGeoVotes            = {};
var NumGeoCommits          = {};
var LastGeoCommitId        = null;
var LastGeoCommitNodes     = null;

var GeoCheckTimer          = null;

var GeoElectionCount = 0;
function generate_geo_vote_id() {
  GeoElectionCount += 1;
  return ZH.MyDataCenter + '_' + GeoElectionCount;
}

function count_geo_commit(err, hres) {
  ZH.t('count_geo_commit');
  if (err) {
    var etxt = err.message ? err.message : err;
    ZH.e('count_geo_commit: ERROR: ' + etxt);
  } else {
    rerun_geo_check();
    var vid             = hres.vote_id;    
    NumGeoCommits[vid] += 1;
    ZH.l('>>>>>>>>: count_geo_commit: V: ' + vid + ' #: ' + NumGeoCommits[vid]);
    if (NumGeoCommits[vid] === LastGeoCommitNodes.length) {
      ZH.l('COMMIT SUCCEEDED ON ALL GEO-NODES: #DC: ' +
           LastGeoCommitNodes.length);
      LastGeoCommitId    = null;
      LastGeoCommitNodes = null;
    }
  }
}

function check_vote_covers_live_GeoNodes(plugin, collections, gnodes, next) {
  fetch_live_geo_nodes(plugin, collections, function(ferr, lgnodes) {
    if (ferr) next(ferr, null);
    else {
      // Remove intersecting geo-nodes
      for (var i = 0; i < gnodes.length; i++) {
        var gnode = gnodes[i];
        for (var j = 0; j < lgnodes.length; j++) {
          var lgnode = lgnodes[j];
          if (lgnode.device_uuid === gnode.device_uuid) {
            lgnodes.splice(j, 1);
            break;
          }
        }
      }
      next(null, lgnodes);
    }
  });
}

function recreate_geo_clients(newbies, next) {
  if (newbies.length === 0) next(null, null);
  else {
    var ngnode = newbies.shift();
    var nguuid = ngnode.device_uuid;
    ZH.l('zvote.recreate_geo_clients: GU: ' + nguuid);
    ZCLS.RecreateGeoClient(nguuid, ngnode, true, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(recreate_geo_clients, newbies, next);
    });
  }
}

function persist_KnownDataCenters(plugin, collections, newbies, next) {
  ZH.t('persist_KnownDataCenters: NEWBIES: ' + JSON.stringify(newbies));
  var kdcs = {};
  for (var i = 0; i < ZCLS.KnownGeoNodes.length; i++) {
    var gnode   = ZCLS.KnownGeoNodes[i];
    var guuid   = gnode.device_uuid;
    kdcs[guuid] = gnode;
  }
  plugin.do_set(collections.global_coll, ZS.KnownDataCenters, kdcs,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      if (!ZH.AmRouter) next(null, null);
      else              recreate_geo_clients(newbies, next);
    }
  });
}

function do_replace_KnownGeoNode(gnode) {
  for (var i = 0; i < ZCLS.KnownGeoNodes.length; i++) {
    var kgnode = ZCLS.KnownGeoNodes[i];
    if (gnode.device_uuid === kgnode.device_uuid) {
      ZCLS.KnownGeoNodes[i] = gnode;
      return true;
    }
  }
  return false;
}

function union_KnownGeoNodes(plugin, collections, gnodes, next) {
  ZH.t('union_KnownGeoNodes');
  var newbies = [];
  for (var i = 0; i < gnodes.length; i++) {
    var gnode = gnodes[i];
    var hit   = do_replace_KnownGeoNode(gnode);
    if (!hit) {
      newbies.push(gnode);
      var ngnode = ZH.clone(gnode);
      delete(ngnode.ts);
      ZCLS.KnownGeoNodes.push(ngnode);
      ZH.l('ADDED to KNOWN GEO-NODES'); ZH.l(ngnode);
    }
  }
  ZCLS.KnownGeoNodes.sort(ZCLS.CmpDeviceUuid);
  if (newbies.length) {
    ZH.t('union_KnownGeoNodes: ZCLS.KnownGeoNodes'); ZH.t(ZCLS.KnownGeoNodes);
  }
  persist_KnownDataCenters(plugin, collections, newbies, next);
}

function replace_KnownGeoNode(plugin, collections, gnode, next) {
  ZH.t('replace_KnownGeoNode');
  var hit = do_replace_KnownGeoNode(gnode);
  if (!hit) {
    ZCLS.KnownGeoNodes.push(gnode);
    ZCLS.KnownGeoNodes.sort(ZCLS.CmpDeviceUuid);
    ZH.t('REPLACE: ZCLS.KnownGeoNodes'); ZH.t(ZCLS.KnownGeoNodes);
  }
  var newbies = [gnode];
  persist_KnownDataCenters(plugin, collections, newbies, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE GEO STATE CHANGE COMMIT---------------------------------------------

function set_geo_network_partition_status(net, gnetpart, gmaj, gnodes, next) {
  if (ZH.GeoNetworkPartitionMode === false) {
    if (gnetpart === true) { // First time set to PARTITIONED
      ZH.GeoNetworkPartitionMode  = true;
      ZH.GeoMajority              = gmaj;
      if (ZH.GeoMajority === false) ZH.CentralSynced = false;
      var full_num_gnode          = ZCLS.GeoNodes.length;
      next(null, full_num_gnode);
    } else {
      ZH.GeoNetworkPartitionMode  = false;
      ZH.GeoMajority              = true;
      next(null, gnodes.length);
    }
  } else { // Currently in GeoNetworkPartition
    var gkey = ZS.LastGeoState;
    net.plugin.do_get_field(net.collections.global_coll,
                            gkey, "full_geo_node_count",
    function(gerr, full_num_gnode) {
      if (gerr) next(gerr, null);
      else {
        if (gnetpart === true) {
          ZH.GeoNetworkPartitionMode  = true;
          ZH.GeoMajority              = false; // Partition of partition
          ZH.CentralSynced            = false;
          next(null, full_num_gnode);
        } else {
          var ghalf = Math.floor(full_num_gnode / 2) + 1;
          ZH.t('#GN: ' + gnodes.length + ' FULL #GN: ' + full_num_gnode +
               ' ghalf: ' + ghalf);
          if (gnodes.length >= ghalf) {
            ZH.GeoMajority = true;
          }
          if (gnodes.length >= full_num_gnode) {
            ZH.GeoNetworkPartitionMode  = false;
          }
          next(null, full_num_gnode);
        }
      }
    });
  }
}

function post_geo_state_change_commit(net, hres, next) {
  if (ZH.AmBoth) next(null, hres);
  else {
    var gterm    = exports.GeoTermNumber;
    var gnodes   = ZCLS.GeoNodes;
    var gnetpart = ZH.GeoNetworkPartitionMode;
    var gmaj     = ZH.GeoMajority;
    var csynced  = ZH.CentralSynced;
    var send_dco = should_send_datacenter_online(gnodes);
    // NOTE: ZPio.BroadcastAnnounceNewGeoCluster() is ASYNC
    ZPio.BroadcastAnnounceNewGeoCluster(net.plugin, net.collections,
                                        gterm, gnodes, gnetpart,
                                        gmaj, csynced);
    // NOTE: ZCLS.InitGeoClusterMesh() is ASYNC
    ZCLS.InitGeoClusterMesh(ZH.OnErrLog);
    ZDQ.PushStorageAnnounceNewGeoCluster(net.plugin, net.collections,
                                         gterm, gnodes, gnetpart, gmaj,
                                         csynced, send_dco,
    function(aerr, ares) {
      next(aerr, hres);
    });
  }
}

exports.HandleGeoStateChangeCommit = function(net, term, vid, gnodes,
                                              gnetpart, gmaj, hres, next) {
  rerun_geo_check();
  ZH.e('HandleGeoStateChangeCommit: T: ' + term + ' V: ' + vid);
  set_geo_network_partition_status(net, gnetpart, gmaj, gnodes,
  function(cerr, full_num_gnode) {
    if (cerr) next(cerr, hres);
    else {
      exports.GeoTermNumber    = term;          // NOTE: NEW TermNumber
      ZCLS.GeoNodes            = gnodes;
      ZCLS.ForceGeoVote        = false;
      ZH.YetToGeoVote          = false;
      ZH.GeoVoteInProgress     = false;
      exports.GeoVoteCompleted = true;

      ZH.l('NEW: GeoTermNumber: '         + exports.GeoTermNumber);
      ZH.l('NEW: GeoNodes');                ZH.l(ZCLS.GeoNodes);
      ZH.l('full_num_gnode: '             + full_num_gnode);
      ZH.l('ZH.GeoNetworkPartitionMode: ' + ZH.GeoNetworkPartitionMode);
      ZH.l('ZH.GeoMajority: '             + ZH.GeoMajority);
      ZH.e('ZH.CentralSynced: '           + ZH.CentralSynced);

      hres.term_number = term;
      hres.vote_id     = vid;
      var state = {term_number           : term,
                   vote_id               : vid,
                   geo_nodes             : gnodes,
                   geo_network_partition : ZH.GeoNetworkPartitionMode,
                   geo_majority          : ZH.GeoMajority,
                   full_geo_node_count   : full_num_gnode};
      net.plugin.do_set(net.collections.global_coll, ZS.LastGeoState, state,
      function(serr, sres) {
        if (serr) next(serr, hres);
        else {
          union_KnownGeoNodes(net.plugin, net.collections, ZCLS.GeoNodes,
          function(uerr, ures) {
            if (uerr) next(uerr, hres);
            else      post_geo_state_change_commit(net, hres, next);
          });
        }
      });
    }
  });
}

var RemoteDataCenterSynced = {};
function elect_primary_geo_node(gnodes) {
  for (var i = 0; i < gnodes.length; i++) {
    var gnode   = gnodes[i];
    var guuid   = gnode.device_uuid;
    var csynced = RemoteDataCenterSynced[guuid];
    if (csynced) {
      gnodes[i].primary = true;
      ZH.e('elect_primary_geo_node: RU: ' + guuid);
      return;
    }
  }
  if (gnodes.length) gnodes[0].primary = true; // FAIL-SAFE
}

function count_geo_vote(plugin, collections, err, hres) {
  ZH.t('>>>>>>>>: count_geo_vote');
  if (err) {
    rerun_geo_check();
    if (err.message === ZS.Errors.OldGeoTermNumber ||
        err.message === ZS.Errors.AlreadyGeoVoted) {
      if (err.details.term_number > exports.GeoTermNumber) {
        exports.GeoTermNumber = err.details.term_number;
        ZH.l('count_geo_vote: UPDATE GeoTermNumber: ' + exports.GeoTermNumber);
      }
      var rgnodes = err.details.geo_nodes;
      // ALWAYS add ANY NODE in ANY VOTE to KNOWN-DATACENTERS
      union_KnownGeoNodes(plugin, collections, rgnodes, ZH.NoOp);
    } else {
      ZH.l('count_geo_vote: ERROR: ' + err.message);
      if (err.details && err.details.missing_geo_nodes) {
        var mgnodes = err.details.missing_geo_nodes;
        // ALWAYS add ANY NODE in ANY VOTE to KNOWN-DATACENTERS
        union_KnownGeoNodes(plugin, collections, mgnodes, ZH.NoOp);
      }
    }
  } else {
    var term          = hres.term_number;
    var vid           = hres.vote_id;
    var gnodes        = hres.geo_nodes;
    var gnetpart      = hres.geo_network_partition;
    var gmaj          = hres.geo_majority;
    var csynced       = hres.central_synced;
    var guuid         = hres.datacenter;
    NumGeoVotes[vid] += 1;
    var ngvotes       = NumGeoVotes[vid];
    var gnlen         = gnodes.length;
    RemoteDataCenterSynced[guuid] = csynced;
    ZH.l('count_geo_vote: V: ' + vid + ' #: ' + ngvotes + ' NG: ' + gnlen);
    if (ngvotes === gnlen) {
      ZH.l('ALL GEO VOTES RECEIVED');
      elect_primary_geo_node(gnodes);
      NumGeoCommits[vid] = 0;
      LastGeoCommitId    = vid;
      LastGeoCommitNodes = ZH.clone(gnodes);
      ZPio.GeoBroadcastGeoStateChangeCommit(plugin, collections,
                                            gnodes, term, vid, gnetpart, gmaj,
                                            count_geo_commit);
    }
  }
}

function create_geo_state_change_vote_error_details() {
  var details         = {};
  details.term_number = exports.GeoTermNumber;
  details.geo_nodes   = ZCLS.GeoNodes;
  return details;
}

exports.HandleGeoStateChangeVote = function(plugin, collections,
                                            term, vid, gnodes, gnetpart, gmaj,
                                            hres, next) {
  ZH.e('ZVote.HandleGeoStateChangeVote: T: ' + term + ' V: ' + vid);
  // ALWAYS add ANY NODE in ANY VOTE to KNOWN-DATACENTERS
  union_KnownGeoNodes(plugin, collections, gnodes, function(uerr, ures) {
    if (uerr) next(uerr, hres);
    else {
      if (ZH.ClusterVoteInProgress) {
        hres.error_details = create_geo_state_change_vote_error_details();
        next(new Error(ZS.Errors.ClusterVoteInProgress), hres);
      } else if (AlreadyGeoVoted[term]) {
        hres.error_details = create_geo_state_change_vote_error_details();
        next(new Error(ZS.Errors.AlreadyGeoVoted), hres);
      } else if (term < exports.GeoTermNumber) {
        hres.error_details = create_geo_state_change_vote_error_details();
        next(new Error(ZS.Errors.OldGeoTermNumber), hres);
      } else {
        check_vote_covers_live_GeoNodes(plugin, collections, gnodes,
        function(lerr, lgnodes) {
          if (lerr) next(lerr, hres);
          else {
            if (lgnodes.length !== 0) {
              hres.error_details                   = {};
              hres.error_details.missing_geo_nodes = lgnodes;
              next(new Error(ZS.Errors.BackwardsProgressGeoVote), hres);
            } else {
              rerun_geo_check();
              ZH.GeoVoteInProgress  = true;
              AlreadyGeoVoted[term] = true;
              var vkey              = ZS.LastVoteGeoTermNumber;
              var guuid             = ZH.MyDataCenter;
              plugin.do_set_field(collections.global_coll, vkey, guuid, term,
              function(serr, sres) {
                if (serr) next(serr, hres);
                else {
                  hres.term_number           = term;
                  hres.vote_id               = vid;
                  hres.geo_nodes             = gnodes;
                  hres.geo_network_partition = gnetpart,
                  hres.geo_majority          = gmaj,
                  hres.central_synced        = ZH.CentralSynced;
                  next(null, hres);
                }
              });
            }
          }
        });
      }
    }
  });
}

function start_new_geo_vote(plugin, collections, gnodes) {
  gnodes.sort(ZCLS.CmpDeviceUuid);
  RemoteDataCenterSynced = {};
  exports.GeoTermNumber += 1;    // NOTE: NEW TermNumber
  LastGeoCommitId        = null; // Commit comes after ALL Votes received
  LastGeoCommitNodes     = null;
  var vid                = generate_geo_vote_id();
  var term               = exports.GeoTermNumber;
  ZH.e('start_new_geo_vote: T: ' + term + ' V: ' + vid); ZH.l(gnodes);
  var diff               = ZCLS.GeoNodes.length - gnodes.length;
  var gnetpart           = false
  var gmaj               = true;
  if (ZH.CentralDisableGeoNetworkPartitions === false) {
    if (diff >= exports.NumberFailedNodesForGeoNetworkPartition) {
      gnetpart  = true;
      var ghalf = Math.floor(ZCLS.GeoNodes.length / 2) + 1;
      gmaj      = (gnodes.length >= ghalf) ?  true : false;
    }
  }
  NumGeoVotes[vid]       = 0;
  var bfunc = ZPio.GeoBroadcastGeoStateChangeVote;
  bfunc(plugin, collections, gnodes, term, vid, gnetpart, gmaj,
  function(err, hres) { // NOTE: CLOSURE as 'next' argument
    count_geo_vote(plugin, collections, err, hres);
  });
}

function analyze_geo_status(plugin, collections, gstat, next) {
  ZH.t('analyze_geo_status'); //ZH.t(gstat);
  var now     = ZH.GetMsTime();
  var changed = false;
  var ngnodes = [];
  for (var guuid in gstat) {
    var gts   = gstat[guuid].ts;
    var stale = now - gts;
    var gnode = ZCLS.GetGeoNodeByUUID(guuid);
    if (gnode) {
      if (stale > exports.GeoNodeMaximumAllowedStaleness) {
        ZH.l('STALE DC'); ZH.l(gstat[guuid]);
        changed = true;
      } else {
        ngnodes.push(gstat[guuid]);
      }
    } else {
      if (stale < exports.GeoNodeMaximumAllowedStaleness) {
        ZH.l('NEW DC'); ZH.l(gstat[guuid]);
        changed = true;
        ngnodes.push(gstat[guuid]);
      }
    }
  }
  next(null, null);
  // NOTE: BELOW is ASYNC
  if (changed) start_new_geo_vote(plugin, collections, ngnodes);
  else {
    var send_dco = should_send_datacenter_online(ZCLS.GeoNodes);
    if (send_dco) ZDConn.SendDataCenterOnline();
  }
}

function fetch_live_geo_nodes(plugin, collections, next) {
  fetch_geo_cluster_status(plugin, collections, function(ferr, gstat) {
    if (ferr) next(ferr, null);
    else {
      var now     = ZH.GetMsTime();
      var lgnodes = [];
      for (var guuid in gstat) {
        var gts   = gstat[guuid].ts;
        var stale = now - gts;
        if (stale <= exports.GeoNodeMaximumAllowedStaleness) {
          lgnodes.push(gstat[guuid]);
        }
      }
      next(null, lgnodes);
    }
  });
}

function force_geo_vote(plugin, collections, next) {
  ZH.t('FORCE GEO_VOTE');
  fetch_live_geo_nodes(plugin, collections, function(ferr, lgnodes) {
    if (ferr) next(ferr, null);
    else {
      var hit = false;
      for (var i = 0; i < lgnodes.length; i++) {
        var gnode = lgnodes[i];
        if (gnode.device_uuid === ZH.MyDataCenter) {
          gnode.leader = true;
          hit          = true;
          break;
        }
      }
      if (!hit) {
        var mgnode    = get_my_geo_node(false);
        mgnode.leader = true;
        lgnodes.push(mgnode);
      }
      next(null, null);
      // NOTE: BELOW is ASYNC
      start_new_geo_vote(plugin, collections, lgnodes);
    }
  });
}

function fetch_geo_cluster_status(plugin, collections, next) {
  plugin.do_get(collections.global_coll, ZS.GeoNodeAliveStatus,
  function (gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var gstat = {};
      if (gres.length !== 0) {
        gstat = gres[0];
        delete(gstat._id);
      }
      next(null, gstat);
    }
  });
}

function evaluate_last_geo_commit() {
  if (LastGeoCommitId) {
    if (NumGeoCommits[LastGeoCommitId] !== LastGeoCommitNodes.length) {
      ZH.l('FORCE GEO_VOTE: #CMT: ' + NumGeoCommits[LastGeoCommitId] +
                          ' #DC: '  + LastGeoCommitNodes.length);
      ZCLS.ForceGeoVote = true;
    }
  }
  if (ZH.GeoVoteInProgress) {
    ZH.l('FORCE GEO_VOTE: LAST GEO_VOTE NEVER ENDED');
    ZCLS.ForceGeoVote = true;
  }
  ZH.GeoVoteInProgress = false;
}

function __run_geo_check(next) {
  if (exports.ClusterVoteCompleted === false ||
      ZH.AmClusterLeader           === false) {
    return next(null, null);
  }
  ZH.t('run_geo_check');
  var ts   = ZH.GetMsTime();
  var diff = ts - exports.NodeStart;
  if (diff < GeoLeaderWarmupPeriod) next(null, null);
  else {
    var net = ZH.CreateNetPerRequest(ZH.Central);
    evaluate_last_geo_commit();
    if (ZCLS.ForceGeoVote) {
      force_geo_vote(net.plugin, net.collections, next);
    } else {
      fetch_geo_cluster_status(net.plugin, net.collections,
      function(gerr, gstat) {
        if (gerr) next(gerr, null);
        else {
          analyze_geo_status(net.plugin, net.collections, gstat, next);
        }
      });
    }
  }
}

function run_geo_check() {
  GeoCheckInProgress = true;
  __run_geo_check(function(cerr, cres) {
    if (cerr) ZH.e('geo_check: ERROR: ' + cerr);
    rerun_geo_check();
  });
}

function next_run_geo_check() {
  if (GeoCheckInProgress || GeoCheckTimer) return;
  var min = MinGeoCheckInterval;
  var max = MaxGeoCheckInterval;
  var to  = min + ((max - min) * Math.random());
  //ZH.t('next_run_geo_check: to: ' + to);
  GeoCheckTimer = setTimeout(run_geo_check, to);
}

// NOTE: rerun_geo_check() cancels current Timeout and forces new run
function rerun_geo_check() {
  if (GeoCheckTimer) {
    clearTimeout(GeoCheckTimer);
    GeoCheckTimer = null;
  }
  GeoCheckInProgress   = false;
  ZH.GeoVoteInProgress = false;
  next_run_geo_check();
}

function fetch_known_geo_nodes(plugin, collections, next) {
  plugin.do_get(collections.global_coll, ZS.KnownDataCenters,
  function (gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var dcs = [];
      if (gres.length !== 0) {
        var gs  = gres[0];
        delete(gs._id);
        for (var guuid in gs) {
          if (guuid !== ZCLS.DiscoveryUUID) {
            dcs.push(gs[guuid]);
          }
        }
      }
      next(null, dcs);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO LEADER HEARTBEAT ------------------------------------------------------

function chaos_geo_leader_heartbeat(plugin, collections, next) {
  var mgnode = get_my_geo_node(false);
  var gnodes = [mgnode]; // ONLY SEND TO SELF
  ZPio.GeoBroadcastLeaderPing(plugin, collections, gnodes, mgnode);
  return next(new Error(ZH.ChaosDescriptions[3]), null);
}

function __geo_leader_heartbeat(next) {
  if (ZH.AmClusterLeader             === false) return next(null, null);
  if (ZH.ClusterNetworkPartitionMode === true)  return next(null, null);
  var net = ZH.CreateNetPerRequest(ZH.Central);
  if (ZH.ChaosMode === 3) {
    return chaos_geo_leader_heartbeat(net.plugin, net.collections, next);
  }
  ZH.t('geo_leader_heartbeat');
  fetch_known_geo_nodes(net.plugin, net.collections, function(ferr, dcs) {
    if (ferr) next(ferr, null);
    else {
      // NOTE: initializes ZCLS.KnownGeoNodes[] on first run
      union_KnownGeoNodes(net.plugin, net.collections, dcs,
      function(uerr, ures) {
        if (uerr) next(uerr, null);
        else {
          var mgnode = get_my_geo_node(false);
          var gnodes = ZCLS.GetKnownDataGeoNodes();
          ZPio.GeoBroadcastLeaderPing(net.plugin, net.collections,
                                      gnodes, mgnode);
          next(null, null);
        }
      });
    }
  });
}

function geo_leader_heartbeat() {
  __geo_leader_heartbeat(function(nerr, nres) {
    if (nerr) ZH.e('geo_leader_heartbeat: ERROR: ' + nerr.message);
    next_run_geo_leader_heartbeat();
  });
}

var GeoLeaderHeartbeatTimer = null;
function next_run_geo_leader_heartbeat() {
  var min = MinGeoLeaderHeartbeatInterval;
  var max = MaxGeoLeaderHeartbeatInterval;
  var to  = min + ((max - min) * Math.random());
  //ZH.t('next_run_geo_leader_heartbeat: to: ' + to);
  GeoLeaderHeartbeatTimer = setTimeout(geo_leader_heartbeat, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO DATA HEARTBEAT --------------------------------------------------------

function __geo_data_heartbeat(next) {
  if (ZH.ClusterNetworkPartitionMode === true)  return next(null, null);
  if (ZH.ChaosMode === 3) {
    return next(new Error(ZH.ChaosDescriptions[3]), null);
  }
  ZH.t('geo_data_heartbeat');
  var net    = ZH.CreateNetPerRequest(ZH.Central);
  var mgnode = get_my_geo_node(false);
  ZPio.GeoBroadcastDataPing(net.plugin, net.collections, mgnode);
  next(null, null);
}

function geo_data_heartbeat() {
  __geo_data_heartbeat(function(nerr, nres) {
    if (nerr) ZH.e('geo_data_heartbeat: ERROR: ' + nerr.message);
    next_run_geo_data_heartbeat();
  });
}

var GeoDataHeartbeatTimer = null;
function next_run_geo_data_heartbeat() {
  var min = MinGeoDataHeartbeatInterval;
  var max = MaxGeoDataHeartbeatInterval;
  var to  = min + ((max - min) * Math.random());
  //ZH.t('next_run_geo_data_heartbeat: to: ' + to);
  GeoDataHeartbeatTimer = setTimeout(geo_data_heartbeat, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPER -------------------------------------------------------------------

exports.FetchClusterStatus = function(plugin, collections, next) {
  fetch_cluster_status(plugin, collections, next);
}

exports.StartNodeHeartbeat = function(plugin, collections) {
  ZH.t('ZVote.StartNodeHeartbeat');
  node_heartbeat(plugin, collections);
}

exports.StartClusterCheck = function(plugin, collections) {
  ZH.t('ZVote.StartClusterCheck');
  next_run_cluster_check(plugin, collections);
}

exports.FetchGeoClusterStatus = function(plugin, collections, next) {
  fetch_geo_cluster_status(plugin, collections, next);
}

exports.FetchKnownGeoNodes = function(plugin, collections, next) {
  fetch_known_geo_nodes(plugin, collections, next);
}

exports.StartGeoLeaderHeartbeat = function() {
  ZH.t('ZVote.StartGeoLeaderHeartbeat');
  next_run_geo_leader_heartbeat();
}

exports.StartGeoDataHeartbeat = function() {
  ZH.t('ZVote.StartGeoDataHeartbeat');
  next_run_geo_data_heartbeat();
}

exports.StartGeoCheck = function() {
  ZH.t('Zvote.StartGeoCheck');
  next_run_geo_check();
}

exports.PersistKnownDataCenters = function(plugin, collections, newbies, next) {
  ZH.t('ZVote.PersistKnownDataCenters');
  persist_KnownDataCenters(plugin, collections, newbies, next);
}

exports.UnionKnownGeoNodes = function(plugin, collections, gnodes, next) {
  ZH.t('ZVote.UnionKnownGeoNodes');
  union_KnownGeoNodes(plugin, collections, gnodes, next);
}

exports.ReplaceKnownGeoNode = function(plugin, collections, gnode, next) {
  ZH.t('ZVote.ReplaceKnownGeoNode');
  replace_KnownGeoNode(plugin, collections, gnode, next);
}

exports.GetMyGeoNode = function(force) {
  ZH.t('ZVote.GetMyGeoNode');
  return get_my_geo_node(force);
}

