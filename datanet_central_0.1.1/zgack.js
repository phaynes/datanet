
var T = require('./ztrace'); // TRACE (before strict)
"use strict";

require('./setImmediate');

var ZPub   = require('./zpublisher');
var ZRAD   = require('./zremote_apply_delta');
var ZDConn = require('./zdconn');
var ZGDD   = require('./zgeo_dirty_deltas');
var ZPio   = require('./zpio');
var ZOOR   = require('./zooo_replay');
var ZDack  = require('./zdack');
var ZGC    = require('./zgc');
var ZDS    = require('./zdatastore');
var ZFix   = require('./zfixlog');
var ZCLS   = require('./zcluster');
var ZVote  = require('./zvote');
var ZPart  = require('./zpartition');
var ZMDC   = require('./zmemory_data_cache');
var ZDQ    = require('./zdata_queue');
var ZQ     = require('./zqueue');
var ZS     = require('./zshared');
var ZH     = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function cmp_central_dirty_delta_dval(adval, bdval) {
  var ares    = adval.split('-');
  var a_kqk   = ares[0];
  var a_auuid = Number(ares[1]);
  var a_avrsn = ares[2];
  var a_avnum = ZH.GetAvnum(a_avrsn);
  var bres    = bdval.split('-');
  var b_kqk   = bres[0];
  var b_auuid = Number(bres[1]);
  var b_avrsn = bres[2];
  var b_avnum = ZH.GetAvnum(b_avrsn);
  if      (a_kqk   !== b_kqk)   return (a_kqk   > b_kqk)   ? 1 : -1;
  else if (a_auuid !== b_auuid) return (a_auuid > b_auuid) ? 1 : -1;
  else if (a_avnum !== b_avnum) return (a_avnum > b_avnum) ? 1 : -1;
  else                          return 0;
}

// NOTE: Used by ZNM.get_ether_delta() -> 
//        called by ZNM.HandleClusterStorageNeedMerge() &
//                  ZNM.HandleClusterStorageGeoNeedMerge()
exports.GetNumGacks = function(plugin, collections, ks, author, next) {
  var dkey = ZS.GetCentralGeoAckDeltaMap(ks, author);
  plugin.do_get(collections.key_coll, dkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, 0);
      else {
        var gmap   = gres[0];
        delete(gmap._id);
        var ngacks = Object.keys(gmap).length;
        next(null, ngacks);
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ACK HELPERS ---------------------------------------------------------------

function debug_check_geo_ack_final(ks, avrsn, gmap, ngacks, ndcs, is_commit) {
  var prfx = is_commit ? "check_geo_ack_commit_final" :
                         "check_geo_ack_delta_final";
  ZH.l(prfx + ': K: ' + ks.kqk + ' AV: ' + avrsn + ' ngacks: ' + ngacks +
       ' #DC: ' + ndcs + ' GMAP: ' + JSON.stringify(gmap));
}

function check_geo_ack_final(ks, author, gmap, is_commit) {
  delete(gmap._id);
  var ngacks    = Object.keys(gmap).length;
  var ndcs      = ZCLS.GeoNodes.length;
  var avrsn     = author.agent_version;
  debug_check_geo_ack_final(ks, avrsn, gmap, ngacks, ndcs, is_commit);
  var gdc_ok    = ZVote.GeoVoteCompleted && ndcs > 0;
  // NOTE: ngacks GT ndcs on GEO CLUSTER resize
  var all_gacks = (ngacks >= ndcs);
  return (gdc_ok && all_gacks);
}

// NOTE: returns 'next(null, gmap);'
function set_geo_ack_map(net, ks, author, rguuid, is_commit, next) {
  var dkey = is_commit ? ZS.GetCentralGeoAckCommitMap(ks, author) :
                         ZS.GetCentralGeoAckDeltaMap (ks, author);
  net.plugin.do_set_field(net.collections.key_coll, dkey, rguuid, true,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      net.plugin.do_get(net.collections.key_coll, dkey, function(gerr, gres) {
        if (gerr) next(gerr, null);
        else {
          if (gres.length == 0) next(null, null);
          else                  next(null, gres[0]);
        }
      });
    }
  });
}
 

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO CLUSTER SUBSCRIBER-DELTA COMMIT ---------------------------------------

function primary_do_reap_delta(net, ks, dentry, next) {
  if (!ZCLS.AmPrimaryDataCenter()) next(null, null);
  else {
    if (!dentry || !dentry.delta._meta.DO_REAP) next(null, null);
    else {
      var rgcv = dentry.delta._meta.reap_gc_version;;
      ZH.l('PRIMARY: COMMIT: primary_do_reap_delta: K: ' + ks.kqk +
           ' (R)GCV: ' + rgcv);
      ZGC.RemoveCentralGCVSummary(net, ks, rgcv, next);
    }
  }
}

function remove_geo_ack_maps(net, ks, author, next) {
  var avrsn = author.agent_version;
  ZH.l('remove_geo_ack_maps: K: ' + ks.kqk + ' AV: ' + avrsn);
  var dkey = ZS.GetCentralGeoAckDeltaMap(ks, author);
  net.plugin.do_remove(net.collections.key_coll, dkey, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      var cdkey = ZS.GetCentralGeoAckCommitMap(ks, author);
      net.plugin.do_remove(net.collections.key_coll, cdkey, next);
    }
  });
}

function remove_storage_delta(net, ks, author, next) {
  var avrsn = author.agent_version;
  ZH.l('remove_storage_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  var pkey  = ZS.GetPersistedStorageDelta(ks, author);
  net.plugin.do_remove(net.collections.delta_coll, pkey, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var gkey = ZS.GetCentralKeyAgentVersions(ks.kqk);
      net.plugin.do_pull(net.collections.delta_coll, gkey, "aversions", avrsn,
      function (xerr, xres) {
        if (xerr && xerr.message !== ZS.Errors.ElementNotFound &&
                    xerr.message !== ZS.Errors.KeyNotFound) {
          next(xerr, null);
        } else {
          ZOOR.RemoveStorageDelta(net.plugin, net.collections, ks, author,
          function(rerr, rres) {
            if (rerr) next(rerr, null);
            else {
              var dkey = ZS.CentralDirtyDeltas;
              var dval = ZS.CentralDirtyDeltaKey(ks.kqk, author);
              net.plugin.do_unset_field(net.collections.global_coll, dkey, dval,
              function(uerr, ures) {
                if (uerr) next(uerr, null);
                else      remove_geo_ack_maps(net, ks, author, next);
              });
            }
          });
        }
      });
    }
  });
}

exports.DoStorageCommitDelta = function(net, ks, author, next) {
  var avrsn = author.agent_version;
  ZH.l('ZGack.DoStorageCommitDelta: K: ' + ks.kqk + ' AV: ' + avrsn);
  ZRAD.FetchStorageDelta(net.plugin, net.collections, ks, author,
  function(ferr, odentry) {
    if (ferr) next(ferr, null);
    else {
      remove_storage_delta(net, ks, author, function(rerr, rres) {
        if (rerr) next(rerr, null);
        else      primary_do_reap_delta(net, ks, odentry, next);
      });
    }
  });
}

exports.HandleStorageGeoSubscriberCommitDelta = function(net, ks, author,
                                                         rchans, next) {
  ZH.l('ZGack.HandleStorageGeoSubscriberCommitDelta: K: ' + ks.kqk);
  exports.DoStorageCommitDelta(net, ks, author, next);
}

exports.HandleStorageAckGeoCommitDelta = function(net, ks, author, rchans,
                                                  rguuid, next) {
  var is_commit = true;
  var avrsn     = author.agent_version;
  ZH.l('AckGeoCommit: K: ' + ks.kqk + ' RU: ' + rguuid + ' AV: ' + avrsn);
  set_geo_ack_map(net, ks, author, rguuid, is_commit, function (serr, gmap) {
    if (serr) next(serr, null);
    else {
      if (!gmap) next(null, null);
      else {
        var gacked = check_geo_ack_final(ks, author, gmap, is_commit);
        if (!gacked) next(null, null);
        else {
          ZDQ.PushRouterGeoBroadcastSubscriberCommitDelta(net, ks, author,
                                                          rchans, next);
        }
      }
    }
  });
}

exports.PostApplyDeltaConditionalCommit = function(net, ks, dentry, next) {
  var author = dentry.delta._meta.author;
  var ckey   = ZS.GetDeltaCommitted(ks, author);
  net.plugin.do_get_field(net.collections.delta_coll, ckey, "value",
  function(gerr, committed) {
    if (gerr) next(gerr, null);
    else {
      if (!committed) next(null, null);
      else {
        var avrsn = author.agent_version;
        ZH.l('ZGack.PostApplyDeltaConditionalCommit: REMOVE COMMITTED DELTA:' +
             ' K: ' + ks.kqk + ' AV: ' + avrsn);
        exports.DoStorageCommitDelta(net, ks, author, next);
      }
    }
  });
}

function check_storage_geo_commit_delta_persisted(net, ks, author, rchans,
                                                  rguuid, next) {
  var pkey = ZS.GetPersistedStorageDelta(ks, author);
  net.plugin.do_get_field(net.collections.delta_coll, pkey, "value",
  function(gerr, p) {
    if (gerr) next(gerr, null);
    else {
      if (!p) {
        var avrsn = author.agent_version;
        ZH.e('CD-COMMIT-DELTA ON MISSING-SD: K: ' + ks.kqk + ' AV: ' + avrsn);
        next(null, null);
      } else {
        ZDQ.PushRouterAckGeoCommitDelta(net, ks, author, rchans, rguuid, next);
      }
    }
  });
}

function do_storage_geo_commit_delta(net, ks, author, rchans, rguuid, next) {
  var avrsn  = author.agent_version;
  var auuid  = author.agent_uuid;
  var davnum = ZH.GetAvnum(avrsn);
  ZRAD.GetCentralDependencies(net, ks, function(ferr, mdeps) {
    if (ferr) next(ferr, null);
    else {
      var mavrsn = mdeps[auuid];
      var mavnum = ZH.GetAvnum(mavrsn);
      ZH.l('do_storage_geo_commit_delta: K: ' + ks.kqk + ' DAVNUM: ' + davnum +
           ' MAVNUM: ' + mavnum);
      if (davnum < mavnum) { // IN DEPENDECNIES[]: ALREADY PROCESSED
        ZDQ.PushRouterAckGeoCommitDelta(net, ks, author, rchans, rguuid, next);
      } else {
        check_storage_geo_commit_delta_persisted(net, ks, author, rchans,
                                                 rguuid, next);
      }
    }
  });
}

function storage_geo_commit_delta(net, pc, next) {
  var ks     = pc.ks;
  var author = pc.extra_data.author;
  var rchans = pc.extra_data.rchans;
  var rguuid = pc.extra_data.rguuid;
  var avrsn  = author.agent_version;
  ZH.l('storage_geo_commit_delta: K: ' + ks.kqk + ' RU: ' + rguuid +
       ' AV: ' + avrsn);
  ZMDC.GetCentralKeysToSync(net.plugin, net.collections, ks,
  function(gerr, frozen) {
    if (gerr) next(gerr, null);
    else {
      if (frozen) {
        ZH.e('CD-COMMIT-DELTA ON FROZEN-KEY: K: ' + ks.kqk + ' AV: ' + avrsn);
        var ckey  = ZS.GetDeltaCommitted(ks, author);
        net.plugin.do_set_field(net.collections.delta_coll,
                                ckey, "value", true, next);
      } else {
        var okey = ZS.GetOOODelta(ks, author);
        net.plugin.do_get_field(net.collections.delta_coll, okey, "value",
        function(oerr, oooav) {
          if (oerr) next(oerr, null);
          else {
            if (oooav) {
              var avrsn = author.agent_version;
              ZH.e('CD-COMMIT-DELTA ON OOO-SD: K: ' + ks.kqk + ' AV: ' + avrsn);
              var ckey  = ZS.GetDeltaCommitted(ks, author);
              net.plugin.do_set_field(net.collections.delta_coll,
                                      ckey, "value", true, next);
            } else {
              do_storage_geo_commit_delta(net, ks, author, rchans,
                                          rguuid, next);
            }
          }
        });
      }
    }
  });
}

exports.RetryGeoCommitDelta = function(plugin, collections, pc, rerr, next) {
  var ks  = pc.ks;
  var net = ZH.CreateNetPerRequest(ZH.Central);
  ZH.l('ZGack.RetryGeoCommitDelta: K: ' + ks.kqk);
  var internal = rerr ? false : true;
  storage_geo_commit_delta(net, pc, function(serr, sres) {
    if (serr) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(null, null);
    }
  });
}

exports.GetGeoCommitID = function(pc) {
  var ks     = pc.ks;
  var author = pc.extra_data.author;
  var avrsn  = author.agent_version;
  return ks.kqk + '-' + avrsn;
}

function add_fixlog_storage_geo_commit_delta(net, pc, next) {
  var ks   = pc.ks;
  var udid = exports.GetGeoCommitID(pc);
  ZH.l('add_fixlog_storage_geo_commit_delta: K: ' + ks.kqk + ' UD: ' + udid);
  var ukey = ZS.FixLog;
  net.plugin.do_set_field(net.collections.global_coll, ukey, udid, pc, next);
}

exports.HandleStorageGeoClusterCommitDelta = function(net, ks, author,
                                                      rchans, rguuid, next) {
  if (!ZH.AmStorage) {
    throw(new Error("ZGack.HandleStorageGeoClusterCommitDelta: LOGIC ERROR"));
  }
  if (!ZH.CentralSynced) { return next(new Error(ZS.Errors.InGeoSync), null); }
  var op    = 'GeoCommitDelta';
  var edata = {author : author, rchans : rchans, rguuid : rguuid};
  var pc    = ZH.InitPreCommitInfo(op, ks, null, null, edata);
  add_fixlog_storage_geo_commit_delta(net, pc, function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      var rfunc = exports.RetryGeoCommitDelta;
      storage_geo_commit_delta(net, pc, function(aerr, ares) {
        if (aerr) rfunc(net.plugin, net.collections, pc, aerr, next);
        else {
          ZFix.RemoveFromFixLog(net.plugin, net.collections, pc, next);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INTERNAL COMMIT DELTA (SKIPS ROUTER & NETWORK FOR ERROR CASES) ------------

function internal_geo_subscriber_commit_delta(net, ks, author, rchans, next) {
  var avrsn  = author.agent_version;
  ZH.l('internal_geo_subscriber_commit_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  var rguuid = ZH.MyDataCenter;
  exports.HandleStorageGeoClusterCommitDelta(net, ks, author, rchans, rguuid,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      ZDQ.PushRouterGeoCommitDelta(net.plugin, net.collections,
                                   ks, author, rchans, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROUTER HANDLE GEO CLUSTER SUBSCRIBER COMMIT DELTA -------------------------

exports.RouterHandleGeoClusterCommitDelta = function(plugin, collections,
                                                     ks, author, rchans,
                                                     rguuid, next) {
  var avrsn = author.agent_version;
  ZH.l('ZGack.RouterHandleGeoClusterCommitDelta: K: ' + ks.kqk +
       ' AV: ' + avrsn);
  ZDQ.PushStorageGeoClusterCommitDelta(plugin, collections,
                                       ks, author, rchans, rguuid, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLEANUP ACKED DELTA -------------------------------------------------------

function cleanup_acked_delta(net, ks, rchans, auuid, avrsn, next) {
  ZH.l('cleanup_acked_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  var author = {agent_uuid : auuid, agent_version : avrsn};
  remove_storage_delta(net, ks, author, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      if (rchans) {
        internal_geo_subscriber_commit_delta(net, ks, author, rchans, next);
      } else {
        ZMDC.GetKeyRepChans(net.plugin, net.collections, ks,
        function(ferr, orchans) {
          if (ferr) next(ferr, null);
          else {
            if (!orchans) { // NO DELTA NOR ORCHANS -> NO COMMIT POSSIBLE
              next(null, null);
            } else {
              internal_geo_subscriber_commit_delta(net, ks, author, rchans,
                                                   next);
            }
          }
        });
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ACK-GEO-DELTA CALLBACK ----------------------------------------------------

exports.GetGeoAckDeltaID = function(pc) {
  var ks   = pc.ks;
  var dres = pc.extra_data.dres;
  var berr = pc.extra_data.berr;
  var author, rguuid;
  if (dres) {
    author = dres.author;
    rguuid = dres.datacenter;
  } else {
    author = berr.details.author;
    rguuid = berr.details.datacenter;
  }
  if (!author) return ks.kqk; // ERROR ACKS
  var auuid = author.agent_uuid;
  var avrsn = author.agent_version;
  ZH.l('ZGack.GetGeoAckDeltaID: K: ' + ks.kqk + ' AU: ' + auuid +
       ' AV: ' + avrsn + ' RU: ' + rguuid);
  return ks.kqk + '_' + auuid + '_' + avrsn + '_' + rguuid;
}

exports.CmpGeoAckID = function(pca, pcb) {
  var aarr     = pca.id.split('_');
  var a_kqk    = aarr[0];
  var a_auuid  = Number(aarr[1]);
  var a_avrsn  = aarr[2];
  var a_rguuid = aarr[3];
  var barr     = pcb.id.split('_');
  var b_kqk    = barr[0];
  var b_auuid  = Number(barr[1]);
  var b_avrsn  = barr[2];
  var b_rguuid = barr[3];
  if      (a_kqk   !== b_kqk)   return (a_kqk   > b_kqk)   ? 1 : -1;
  else if (a_auuid !== b_auuid) return (a_auuid > b_auuid) ? 1 : -1;
  else if (a_avrsn !== b_avrsn) return (a_avrsn > b_avrsn) ? 1 : -1;
  else {
    return (a_rguuid === b_rguuid) ? 0 : (a_rguuid > b_rguuid) ? 1 : -1;
  }
}

// NOTE: no QUEUE needed as NGACKs is a counter
function ack_geo_delta(net, ks, author, rchans, rguuid, internal, next) {
  var is_commit = false;
  var avrsn     = author.agent_version;
  ZH.l('AckGeoDelta: K: ' + ks.kqk + ' RU: ' + rguuid + ' AV: ' + avrsn);
  set_geo_ack_map(net, ks, author, rguuid, is_commit, function (serr, gmap) {
    if (serr) next(serr, null);
    else {
      if (!gmap) next(null, null);
      else {
        var gacked = check_geo_ack_final(ks, author, gmap, is_commit);
        if (!gacked) next(null, null);
        else {
          ZDQ.PushRouterGeoCommitDelta(net.plugin, net.collections,
                                       ks, author, rchans, next);
        }
      }
    }
  });
}

// NOTE: Used by ZRAD.transaction_storage_delta_commit() &
//               ZRAD.push_auto_dentry_to_router()
exports.InternalAckGeoDelta = function(net, ks, dentry, next) {
  var author = dentry.delta._meta.author;
  var rchans = dentry.delta._meta.replication_channels;
  ack_geo_delta(net, ks, author, rchans, ZH.MyDataCenter, true, next);
}

function ack_geo_delta_ooo(net, ks, amp, rguuid, next) {
  if (ZH.ChaosMode === 20) {
    ZH.e(ZH.ChaosDescriptions[20]);
    return next(null, null);
  }
  ZH.l('ack_geo_delta_ooo: K: ' + ks.kqk + ' RU: ' + rguuid);
  var gnode = ZCLS.GetGeoNodeByUUID(rguuid);
  if (!gnode) next(null, null);
  else {
    var force   = amp; // NOTE: PRIMARY-DC FORCE (NO FREEZE)
    var unacked = false;
    ZGDD.DoFlowDrainGeoDeltas(net, ks, gnode, force, unacked, next);
  }
}

function do_handle_ack_geo_delta(net, pc, next) {
  var ks   = pc.ks;
  var berr = pc.extra_data.berr;
  var dres = pc.extra_data.dres;
  if (dres) {
    var author   = dres.author;
    var rguuid   = dres.datacenter;
    var rchans   = dres.replication_channels;
    var internal = (rguuid === ZH.MyDataCenter);
    ack_geo_delta(net, ks, author, rchans, rguuid, internal, next);
  } else if (berr) {
    if (berr.message === ZS.Errors.OutOfOrderDelta) {
      var amp    = berr.am_primary;
      var rguuid = berr.details.datacenter;
      ack_geo_delta_ooo(net, ks, amp, rguuid, next);
    } else if (berr.message === ZS.Errors.DeltaNotSync) {
      var rchans = berr.details.replication_channels;
      var author = berr.details.author;
      var auuid  = author.agent_uuid;
      var avrsn  = author.agent_version;
      cleanup_acked_delta(net, ks, rchans, auuid, avrsn, next);
    } else {
      ZH.l('handle_ack_geo_delta: UNKNOWN ERROR'); ZH.p(berr);
      next(null, null);
    }
  }
}

exports.RetryGeoAckDelta = function(plugin, collections, pc, rerr, next) {
  var ks  = pc.ks;
  var net = ZH.CreateNetPerRequest(ZH.Central);
  ZH.l('ZGack.RetryGeoAckDelta: K: ' + ks.kqk);
  var internal = rerr ? false : true;
  do_handle_ack_geo_delta(net, pc, function(serr, sres) {
    if (serr) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(null, null);
    }
  });
}

function add_fixlog_ack_geo_delta(plugin, collections, pc, next) {
  var ks   = pc.ks;
  var udid = exports.GetGeoAckDeltaID(pc);
  ZH.l('add_fixlog_ack_geo_delta: K: ' + ks.kqk + ' UD: ' + udid);
  var ukey = ZS.FixLog;
  plugin.do_set_field(collections.global_coll, ukey, udid, pc, next);
}

function handle_ack_geo_delta(net, ks, berr, dres, next) {
  if (!ZH.AmStorage) throw(new Error("handle_ack_geo_delta() LOGIC ERROR"));
  if (!ZH.CentralSynced) { return next(new Error(ZS.Errors.InGeoSync), null); }
  var op    = 'AckGeoDelta';
  var edata = {berr : berr, dres : dres};
  var pc    = ZH.InitPreCommitInfo(op, ks, null, null, edata);
  add_fixlog_ack_geo_delta(net.plugin, net.collections, pc,
  function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      var rfunc = exports.RetryGeoAckDelta;
      do_handle_ack_geo_delta(net, pc, function(aerr, ares) {
        if (aerr) rfunc(net.plugin, net.collections, pc, aerr, next);
        else {
          ZFix.RemoveFromFixLog(net.plugin, net.collections, pc, next);
        }
      });
    }
  });
}

exports.FlowHandleStorageAckGeoDelta = function(plugin, collections, qe, next) {
  var tn    = T.G();
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  var ks    = qe.ks;
  ZH.l('<-|(G): ZGack.FlowHandleStorageAckGeoDelta: K: ' + ks.kqk);
  var data  = qe.data;
  var berr  = data.berr;
  var dres  = data.dres;
  handle_ack_geo_delta(net, ks, berr, dres, function(aerr, ares) {
    T.X(tn);
    qnext(aerr, qhres);
    next(null, null);
  });
}

exports.HandleStorageAckGeoDelta = function(net, ks, berr, dres, next) {
  var data  = {berr : berr, dres : dres};
  var mname = 'STORAGE_GEO_DELTA_ACK';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, next);
  var flow  = {k : ks.kqk,
               q : ZQ.StorageKeySerializationQueue,
               m : ZQ.StorageKeySerialization,
               f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

exports.HandleAckGeoDelta = function(net, ks, author, rchans, rguuid, next) {
  var dres  = {ks                   : ks,
               author               : author,
               datacenter           : rguuid,
               replication_channels : rchans}
  ZDQ.PushStorageAckGeoDelta(net.plugin, net.collections, ks, null, dres, next);
}

exports.HandleAckGeoDeltaError = function(net, ks, author, rchans,
                                          berr, rguuid, next) {
  ZDQ.PushStorageAckGeoDelta(net.plugin, net.collections, ks, berr, null, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ACK GEO DENTRIES ----------------------------------------------------------

//TODO THIS PATH (AckGeoDentries) NO LONGER HAPPENS -> (I THINK?)
function do_handle_ack_geo_dentries(net, pc, next) {
  var ks   = pc.ks;
  var berr = pc.extra_data.berr;
  var dres = pc.extra_data.dres;
  if (dres) {
    var errors = dres.errors;
    if (errors.length === 0) next(null, null);
    else {
      for (var i = 0; i < errors.length; i++) {
        var serr = errors[i];
        if (serr.message === ZS.Errors.OutOfOrderDelta) { // FIRST OOO-GD
          var amp    = serr.am_primary;
          var rguuid = dres.datacenter;
          ack_geo_delta_ooo(net, ks, amp, rguuid, next);
          next(null, null);
          return;                                            // NOTE RETURN
        }
      }
      ZH.e('do_handle_ack_geo_dentries: UNHANDLED ERRORS'); ZH.e(errors);
      next(null, null);
    }
  } else {
    ZH.e('handle_ack_geo_dentries: UNKNOWN ERROR'); ZH.e(berr);
    next(null, null);
  }
}

exports.RetryGeoAckDentries = function(plugin, collections, pc, rerr, next) {
  var ks  = pc.ks;
  var net = ZH.CreateNetPerRequest(ZH.Central);
  ZH.l('ZGack.RetryGeoAckDentries: K: ' + ks.kqk);
  var internal = rerr ? false : true;
  do_handle_ack_geo_dentries(net.plugin, net.collections, pc,
  function(serr, sres) {
    if (serr) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(null, null);
    }
  });
}

function add_fixlog_ack_geo_dentries(plugin, collections, pc, next) {
  var ks   = pc.ks;
  var udid = pc.ks.kqk;
  ZH.l('add_fixlog_ack_geo_dentries: K: ' + ks.kqk + ' UD: ' + udid);
  var ukey = ZS.FixLog;
  plugin.do_set_field(collections.global_coll, ukey, udid, pc, next);
}

function handle_ack_geo_dentries(net, ks, berr, dres, next) {
  if (!ZH.AmStorage) throw(new Error("handle_ack_geo_dentries() LOGIC ERROR"));
  if (!ZH.CentralSynced) { return next(new Error(ZS.Errors.InGeoSync), null); }
  var op    = 'AckGeoDentries';
  var edata = {berr : berr, dres : dres};
  var pc    = ZH.InitPreCommitInfo(op, ks, null, null, edata);
  add_fixlog_ack_geo_dentries(net.plugin, net.collections, pc,
  function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      var rfunc = exports.RetryGeoAckDentries;
      do_handle_ack_geo_dentries(net, pc, function(aerr, ares) {
        if (aerr) rfunc(net.plugin, net.collections, pc, aerr, next);
        else      ZFix.RemoveFromFixLog(net.plugin, net.collections, pc, next);
      });
    }
  });
}

// NOTE: runs under ZQ.StorageKeySerializationQueue.LOCK(kqk)
exports.FlowHandleStorageAckGeoDentries = function(plugin, collections,
                                                   qe, next) {
  var tn    = T.G();
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  var ks    = qe.ks;
  ZH.l('<-|(G): ZGack.FlowHandleStorageAckGeoDentries: K: ' + ks.kqk);
  var data = qe.data;
  handle_ack_geo_dentries(net, ks, data.berr, data.dres, function(aerr, ares) {
    T.X(tn);
    qnext(aerr, qhres);
    next(null, null);
  });
}

exports.HandleStorageAckGeoDentries = function(net, ks, berr, dres, next) {
  if (!dres && (!berr || !berr.details)) return next(null, null);
  var data  = {berr : berr, dres : dres};
  var mname = 'STORAGE_GEO_DENTRIES_ACK';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, next);
  var flow  = {k : ks.kqk,
               q : ZQ.StorageKeySerializationQueue,
               m : ZQ.StorageKeySerialization,
               f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

// Response from ZPio.GeoSendDentries()
exports.CallbackAckGeoDentries = function(berr, dres) {
  var net = ZH.CreateNetPerRequest(ZH.Central);
  if (!dres && (!berr || !berr.details)) return;
  var ks  = dres ? dres.ks : berr.details.ks;
  if (!ks) {
    ZH.e('-ERROR: ZGack.CallbackAckGeoDentries: NO KEY'); ZH.e(berr);ZH.e(dres);
    return;
  }
  ZH.l('ZGack.CallbackAckGeoDentries: K: ' + ks.kqk);
  ZDQ.PushStorageAckGeoDentries(net.plugin, net.collections,
                                ks, berr, dres, ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// POST GEO VOTE CLEANUP DIRTY DELTAS ----------------------------------------

function do_post_geo_vote_cleanup(net, ks, auuid, avrsn, next) {
  var author = {agent_uuid : auuid, agent_version : avrsn};
  var dkey   = ZS.GetCentralGeoAckDeltaMap(ks, author);
  net.plugin.do_get(net.collections.key_coll, dkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var gmap = gres[0];
        ZRAD.FetchStorageDelta(net.plugin, net.collections, ks, author,
        function(ferr, fdentry) {
          if (ferr) next(ferr, null);
          else {
            if (!fdentry) next(null, null);
            else {
              var is_commit = false;
              var gacked    = check_geo_ack_final(ks, author, gmap, is_commit);
              if (!gacked) next(null, null);
              else {
                var rchans = fdentry.delta._meta.replication_channels;
                internal_geo_subscriber_commit_delta(net, ks, author, rchans,
                                                     next);
              }
            }
          }
        });
      }
    }
  });
}

function post_geo_vote_cleanup(net, dvals, next) {
  if (dvals.length === 0) next(null, null);
  else {
    var dval  = dvals.shift();
    var res   = dval.split('-');
    var kqk   = res[0];
    var auuid = Number(res[1]);
    var avrsn = res[2];
    var ks    = ZH.ParseKQK(kqk);
    ZH.l('post_geo_vote_cleanup: K: ' + ks.kqk + ' AU: ' + auuid +
         ' AV: ' + avrsn);
    do_post_geo_vote_cleanup(net, ks, auuid, avrsn, function(cerr, cres) {
      if (cerr) next(cerr, null);
      else      setImmediate(post_geo_vote_cleanup, net, dvals, next);
    });
  }
}

exports.StoragePostGeoVoteCleanupDirtyDeltas = function(net, next) {
  if (!ZH.AmStorage) { throw(new Error("PostGeoVote LOGIC ERROR")); }
  if (ZH.ClusterNetworkPartitionMode ||
      ZH.ClusterVoteInProgress) {
    return next(null, null);
  }
  ZH.l('StoragePostGeoVoteCleanupDirtyDeltas');
  var dkey = ZS.CentralDirtyDeltas; //TODO SCAN
  net.plugin.do_get(net.collections.global_coll, dkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var dmap  = gres[0];
        delete(dmap._id);
        var dvals = [];
        for (var dval in dmap) {
          var res   = dval.split('-');
          var kqk   = res[0];
          var ks    = ZH.ParseKQK(kqk);
          var cnode = ZPart.GetKeyNode(ks); // KQK-RANGE
          if (cnode && cnode.device_uuid === ZH.MyUUID) dvals.push(dval);
        }
        ZH.l('post_geo_vote_cleanup: #Ds: ' + dvals.length);
        post_geo_vote_cleanup(net, dvals, next);
      }
    }
  });
}

