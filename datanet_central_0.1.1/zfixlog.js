"use strict";

var ZPub, ZRAD, ZGack, ZNM, ZPart;
var ZWss, ZSD, ZSM, ZAS, ZDelt, ZRollback, ZDack, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZPub      = require('./zpublisher');
  ZRAD      = require('./zremote_apply_delta');
  ZGack     = require('./zgack');
  ZNM       = require('./zneedmerge');
  ZPart     = require('./zpartition');
  ZWss      = require('./zwss');
  ZSD       = require('./zsubscriber_delta');
  ZSM       = require('./zsubscriber_merge');
  ZAS       = require('./zactivesync');
  ZDelt     = require('./zdeltas');
  ZRollback = require('./zrollback');
  ZDack     = require('./zdack');
  ZS        = require('./zshared');
  ZH        = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var MinNextFixLogReplayInterval =  5000;
var MaxNextFixLogReplayInterval = 10000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIX-LOG -------------------------------------------------------------------

var FixLogReplayerTimer         = null;
var FixLogReplayerInProgress    = false;

function replay_data(plugin, collections, ulogs, next) {
  if (ulogs.length === 0) next(null, null);
  else {
    var d  = ulogs.shift();
    var pc = d.data;
    var rfunc;
    if        (pc.op === "AgentDelta") { // ROLLBACK
      ZH.e('replay_data: T: AgentDelta K: ' + pc.ks.kqk);
      rfunc = ZRollback.RollbackAdelta;
    } else if (pc.op === 'RemoveAgentDelta') {
      ZH.e('replay_data: T: RemoveAgentDelta K: ' + pc.ks.kqk);
      rfunc = ZDack.RetryRemoveAgentDelta;
    } else if (pc.op === "SubscriberDelta") {
      ZH.e('replay_data: T: SubscriberDelta K: ' + pc.ks.kqk);
      rfunc = ZSD.RetrySubscriberDelta;
    } else if (pc.op === 'SubscriberCommitDelta') {
      ZH.e('replay_data: T: SubscriberCommitDelta K: ' + pc.ks.kqk);
      rfunc = ZAS.RetrySubscriberCommitDelta;
    } else if (pc.op === "SubscriberMerge") {
      ZH.e('replay_data: T: SubscriberMerge K: ' + pc.ks.kqk);
      rfunc = ZSM.RetrySubscriberMerge;
    // ABOVE THIS LINE AGENT   METHODS
    // BELOW THIS LINE CENTRAL METHODS
    } else if (pc.op === "ClusterDelta") {
      ZH.e('replay_data: T: ClusterDelta K: ' + pc.ks.kqk);
      rfunc = ZRAD.RetryClusterDelta;
    } else if (pc.op === 'AckGeoDelta') {
      ZH.e('replay_data: T: AckGeoDelta K: ' + pc.ks.kqk);
      rfunc = ZGack.RetryGeoAckDelta;
    } else if (pc.op === 'AckGeoDentries') {
      ZH.e('replay_data: T: AckGeoDentries K: ' + pc.ks.kqk);
      rfunc = ZGack.RetryGeoAckDentries;

    } else if (pc.op === 'GeoCommitDelta') {
      ZH.e('replay_data: T: GeoCommitDelta K: ' + pc.ks.kqk);
      rfunc = ZGack.RetryGeoCommitDelta;
    } else if (pc.op === "AckGeoNeedMerge") {
      ZH.e('replay_data: T: AckGeoNeedMerge K: ' + pc.ks.kqk);
      rfunc = ZNM.RetryAckGeoNeedMerge;
    } else {
      throw(new Error("LOGIC ERROR in replay_data()"));
    }
    rfunc(plugin, collections, pc, null, function(rerr, rres) {
      if (rerr) next(rerr, null);
      else {
        var ukey  = ZS.FixLog;
        plugin.do_unset_field(collections.global_coll, ukey, d.id,
        function(uerr, ures) {
          if (uerr) next(uerr, null);
          else {
            setImmediate(replay_data, plugin, collections, ulogs, next);
          }
        });
      }
    });

  }
}

// PURPOSE: Each ClusterNode only REPLAYs KeyMaster portion of FIXLOG
function filter_key_master(ulogs) {
  var flogs = [];
  for (var i = 0; i < ulogs.length; i++) {
    var d   = ulogs[i];
    var pc  = d.data;
    var ks  = pc.ks;
    var cnode = ZPart.GetKeyNode(ks);
    if (cnode && cnode.device_uuid === ZH.MyUUID) flogs.push(d);
  }
  return flogs;
}

function replay_fixlog(plugin, collections, next) {
  ZH.e('replay_fixlog');
  var ukey = ZS.FixLog;
  plugin.do_get(collections.global_coll, ukey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var umap  = gres[0];
        delete(umap._id);
        var ulogs = [];
        for (var id in umap) {
          ulogs.push({id : id, data : umap[id]});
        }
        //TODO CMP functions not needed
        if (ZH.AmCentral) ulogs.sort(cmp_central_fixlog);
        else              ulogs.sort(cmp_agent_fixlog);
        ZH.e('replay_fixlog: RAW #ulogs: ' + ulogs.length);
        if (ZH.AmCentral) {
          ulogs = filter_key_master(ulogs);
        }
        ZH.e('replay_fixlog: POST FILTER #ulogs: ' + ulogs.length);
        replay_data(plugin, collections, ulogs, next);
      }
    }
  });
}

function fixlog_replayer() {
  if (ZH.ClusterVoteInProgress) {
    ZH.e('fixlog_replayer: DURING: CLUSTER-VOTE -> DEFER');
    return rerun_fixlog_replayer();
  }
  ZH.e('fixlog_replayer: IN PROGRESS');
  FixLogReplayerInProgress = true;
  var net  = ZH.AmCentral ? ZH.CreateNetPerRequest(ZH.Central) :
                            ZH.CreateNetPerRequest(ZH.Agent);
  var ckey = ZS.ServiceStart;
  net.plugin.do_get(net.collections.global_coll, ckey, function(gerr, gres) {
    if (gerr) {
      ZH.e('fixlog_replayer: DB STILL DOWN: ERROR: ' + gerr);
      rerun_fixlog_replayer();
    } else {
      replay_fixlog(net.plugin, net.collections, function(perr, pres) {
        if (perr) {
          ZH.e('fixlog_replayer: ERROR: ' + perr);
          rerun_fixlog_replayer();
        } else {
          var now  = ZH.GetMsTime();
          ZH.e('fixlog_replayer: DB UP: @: ' + now);
          var ukey = ZS.FixLog;
          net.plugin.do_remove(net.collections.global_coll, ukey,
          function(rerr, rres) {
            if (rerr) {
              ZH.e('fixlog_replayer: ERROR: ' + rerr);
              rerun_fixlog_replayer();
            } else {
              FixLogReplayerTimer      = null;
              FixLogReplayerInProgress = false;
              ZH.FixLogActive          = false;
              ZH.e('FIX-LOG REPLAYED: ZH.FixLogActive: ' + ZH.FixLogActive);
              if (!ZH.AmCentral && !ZH.Agent.Synced) {
                ZWss.OpenAgentWss(ZH.OnErrThrow);
              }
            }
          });
        }
      });
    }
  });
}

function immediate_run_fixlog_replayer() {
  if (FixLogReplayerInProgress || FixLogReplayerTimer) return;
  ZH.l('immediate_run_fixlog_replayer');
  fixlog_replayer();
}

function next_run_fixlog_replayer() {
  if (FixLogReplayerInProgress || FixLogReplayerTimer) return;
  var min = MinNextFixLogReplayInterval;
  var max = MaxNextFixLogReplayInterval;
  var to  = min + ((max - min) * Math.random());
  ZH.l('next_run_fixlog_replayer: to: ' + to);
  FixLogReplayerTimer = setTimeout(fixlog_replayer, to);
}

function rerun_fixlog_replayer() {
  if (FixLogReplayerTimer) {
    clearTimeout(FixLogReplayerTimer);
    FixLogReplayerTimer = null;
  }
  FixLogReplayerInProgress = false;
  next_run_fixlog_replayer();
}

exports.ActivateFixLog = function(rerr, next) {
  ZH.FixLogActive = true;
  next(rerr, null);
  // NOTE: BELOW is ASYNC
  next_run_fixlog_replayer();
}

//TODO CMP functions not needed
function cmp_pc_id(pca, pcb) {
  return (pca.id === pcb.id) ? 0 : ((pca.id > pcb.id) ? 1 : -1);
}

//TODO CMP functions not needed
function cmp_central_fixlog(pca, pcb) {
  if        (pca.data.op === 'ClusterDelta') {
    if (pcb.data.op !== 'ClusterDelta') return 1;
    else {
      return cmp_pc_id(pca, pcb);
    }
  } else if (pca.data.op === 'AckGeoDentries') {
    if      (pcb.data.op === 'ClusterDelta')   return -1;
    else if (pcb.data.op !== 'AckGeoDentries') return  1;
    else {
      return cmp_pc_id(pca, pcb);
    }
  } else if (pca.data.op === 'AckGeoDelta') {
    if      (pcb.data.op === 'ClusterDelta')   return -1;
    else if (pcb.data.op === 'AckGeoDentries') return -1;
    else if (pcb.data.op !== 'AckGeoDelta')    return  1;
    else {
      return ZGack.CmpGeoAckID(pca, pcb);
    }
  } else if (pca.data.op === 'GeoCommitDelta') {
    if      (pcb.data.op === 'ClusterDelta')             return -1;
    else if (pcb.data.op === 'AckGeoDentries')           return -1;
    else if (pcb.data.op === 'AckGeoDelta')              return -1;
    else if (pcb.data.op !== 'GeoCommitDelta') return  1;
    else {
      return ZAS.CmpSubscriberCommitID(pca, pcb);
    }
  } else if (pca.data.op === 'AckGeoNeedMerge') {
    if (pcb.data.op !== 'AckGeoNeedMerge') return -1;
    else {
      return cmp_pc_id(pca, pcb);
    }
  } else {
    return -1; // Array of size one
  }
}

//TODO CMP functions not needed
function cmp_agent_fixlog(pca, pcb) {
  if        (pca.data.op === 'AgentDelta') {
    if (pcb.data.op !== 'AgentDelta') return 1;
    else {
      return ZDelt.CmpAgentDeltaID(pca, pcb);
    }
  } else if (pca.data.op === 'RemoveAgentDelta') {
    if      (pcb.data.op === 'AgentDelta')       return -1;
    else if (pcb.data.op !== 'RemoveAgentDelta') return  1;
    else {
      return ZDelt.CmpAgentDeltaID(pca, pcb);
    }
  } else if (pca.data.op === 'SubscriberDelta') {
    if      (pcb.data.op === 'AgentDelta')       return -1;
    else if (pcb.data.op === 'RemoveAgentDelta') return -1;
    else if (pcb.data.op !== 'SubscriberDelta')  return  1;
    else {
      return ZSD.CmpSubscriberDeltaID(pca, pcb);
    }
  } else if (pca.data.op === 'SubscriberCommitDelta') {
    if      (pcb.data.op === 'AgentDelta')            return -1;
    else if (pcb.data.op === 'RemoveAgentDelta')      return -1;
    else if (pcb.data.op === 'SubscriberDelta')       return -1;
    else if (pcb.data.op !== 'SubscriberCommitDelta') return  1;
    else {
      return ZAS.CmpSubscriberCommitID(pca, pcb);
    }
  } else if (pca.data.op === 'SubscriberMerge') {
    if (pcb.data.op !== 'SubscriberMerge') return -1;
    else {
      return cmp_pc_id(pca, pcb);
    }
  } else {
    ZH.l('pca'); ZH.p(pca);
    ZH.l('pcb'); ZH.p(pcb);
    throw(new Error("PROGRAM ERROR(cmp_agent_fixlog)"));
  }
}

exports.RemoveFromFixLog = function(plugin, collections, pc, next) {
  var udid;
  if        (pc.op === 'AgentDelta') {
    udid = ZDelt.GetAgentDeltaID(pc);
  } else if (pc.op === 'RemoveAgentDelta') {
    udid = ZDack.GetRemoveAgentDeltaID(pc);
  } else if (pc.op === 'SubscriberDelta') {
    udid = ZSD.GetSubscriberDeltaID(pc);
  } else if (pc.op === 'GeoCommitDelta') {
    udid = ZGack.GetGeoCommitID(pc);
  } else if (pc.op === 'SubscriberMerge') {
    udid = pc.ks.kqk;
  // ABOVE THIS LINE AGENT   METHODS
  // BELOW THIS LINE CENTRAL METHODS
  } else if (pc.op === 'ClusterDelta') {
    udid = ZRAD.GetClusterDeltaID(pc);
  } else if (pc.op === 'AckGeoDentries') {
    udid = pc.ks.kqk;
  } else if (pc.op === 'AckGeoDelta') {
    udid = ZGack.GetGeoAckDeltaID(pc);
  } else if (pc.op === 'SubscriberCommitDelta') {
    udid = ZAS.GetSubscriberCommitID(pc);
  } else if (pc.op === 'AckGeoNeedMerge') {
    udid = pc.ks.kqk;
  } else {
    throw(new Error("PROGRAM ERROR(ZFix.RemoveFromFixLog)"));
  }
  ZH.l('ZFix.RemoveFromFixLog: K: ' + pc.ks.kqk + ' UD: ' + udid);
  var ukey  = ZS.FixLog;
  plugin.do_unset_field(collections.global_coll, ukey, udid, next);
}

exports.Init = function(next) {
  ZH.e('ZFix.Init()');
  var me          = ZH.AmCentral ? ZH.Central : ZH.Agent;
  var plugin      = me.net.plugin;
  var collections = me.net.collections;
  var ukey        = ZS.FixLog;
  plugin.do_get(collections.global_coll, ukey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      ZH.FixLogActive = false;
      if (gres.length !== 0) {
        var fixlog = gres[0];
        delete(fixlog._id);
        var nfix = Object.keys(fixlog).length;
        ZH.FixLogActive = (nfix === 0) ? false : true;
      }
      ZH.e('ZH.FixLogActive: ' + ZH.FixLogActive);
      next(null, null);
      // NOTE: BELOW is ASYNC
      if (ZH.FixLogActive) immediate_run_fixlog_replayer();
    }
  });
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZFix']={} : exports);

