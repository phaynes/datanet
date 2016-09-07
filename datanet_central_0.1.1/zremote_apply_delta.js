"use strict";

require('./setImmediate');

var ZPub   = require('./zpublisher');
var ZGack  = require('./zgack');
var ZMerge = require('./zmerge');
var ZAD    = require('./zapply_delta');
var ZSD    = require('./zsubscriber_delta');
var ZGC    = require('./zgc');
var ZDT    = require('./zdatatypes');
var ZDS    = require('./zsubscriber_delta');
var ZOOR   = require('./zooo_replay');
var ZMDC   = require('./zmemory_data_cache');
var ZDS    = require('./zdatastore');
var ZFix   = require('./zfixlog');
var ZCLS   = require('./zcluster');
var ZDQ    = require('./zdata_queue');
var ZMicro = require('./zmicro');
var ZQ     = require('./zqueue');
var ZS     = require('./zshared');
var ZH     = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var FixOOOGCVDeltaSleep = 20000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

exports.GetClusterDeltaID = function(pc) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  var auuid  = dentry.delta._meta.author.agent_uuid;
  var avrsn  = dentry.delta._meta.author.agent_version;
  return ks.kqk + '_' + auuid + '_' + avrsn;
}

exports.FetchStorageDelta = function(plugin, collections, ks, author, next) {
  var auuid = author.agent_uuid;
  var avrsn = author.agent_version;
  ZH.l('ZRAD.FetchStorageDelta: K: ' + ks.kqk + ' AV: ' + avrsn);
  var pkey = ZS.GetCentralPersistDelta(ks.kqk, auuid, avrsn);
  plugin.do_get_field(collections.delta_coll, pkey, "dentry", next);
}

exports.FetchStorageKeyMetadata = function(net, ks, author, next) {
  var auuid = author.agent_uuid;
  ZMDC.GetGCVersion(net.plugin, net.collections, ks, function(gerr, gcv) {
    if (gerr) next(gerr, null);
    else {
      ZDS.ForceRetrieveCrdt(net.plugin, net.collections, ks,
      function(ferr, ocrdt) {
        if (ferr) next(ferr, null);
        else {
          var akey = ZS.GetAgentKeyOOOGCV(ks, auuid);
          net.plugin.do_get_field(net.collections.global_coll, akey, "value",
          function(gerr, need) {
            if (gerr) next(gerr, null);
            else {
              ZMDC.GetCentralKeysToSync(net.plugin, net.collections, ks,
              function(cerr, frozen) {
                if (cerr) next(cerr, null);
                else {
                  if (!ZH.CentralSynced) frozen = true;
                  var md = {gcv     : gcv,
                            ocrdt   : ocrdt,
                            knreo   : need,
                            frozen  : frozen}
                  next(null, md);
                }
              });
            }
          });
        }
      });
    }
  });
}

exports.FetchApplyStorageDeltaMetadata = function(net, ks, dentry, next) {
  var author = dentry.delta._meta.author;
  exports.FetchStorageKeyMetadata(net, ks, author, function(gerr, md) {
    if (gerr) next(gerr, null);
    else {
      var rchans = dentry.delta._meta.replication_channels;
      ZPub.GetSubs(net.plugin, net.collections, ks, rchans,
      function(rerr, subs) {
        if (rerr) next(rerr, null);
        else {
          md.subs = subs;
          next(null, md);
        }
      });
    }
  });
}

function push_router_send_ack_geo_delta(net, ks, dentry, rguuid, next) {
  var author        = dentry.delta._meta.author;
  var rchans        = dentry.delta._meta.replication_channels;
  var dirty_central = dentry.delta._meta.dirty_central;
  ZDQ.PushRouterSendAckGeoDelta(net.plugin, net.collections,
                                rguuid, ks, author,
                                rchans, dirty_central, next);
}

function get_fix_delta_key(ks, author) {
  var avrsn = author.agent_version;
  return ks.kqk + '-' + avrsn;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL-AGENT-VERSIONS/DEPENDENCIES ---------------------------------------

exports.GetCentralAgentVersions = function(net, ks, next) {
  ZH.l('ZRAD.GetCentralAgentVersions: K: ' + ks.kqk);
  var kkey = ZS.GetKeyToCentralAgentVersion(ks.kqk);
  net.plugin.do_get(net.collections.global_coll, kkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, {}); // NOTE: {}
      else {
        var cavrsns = gres[0];
        delete(cavrsns._id);
        next(null, cavrsns);
      }
    }
  });
}

exports.GetCentralDependencies = function(net, ks, next) {
  ZH.l('ZRAD.GetCentralDependencies: K: ' + ks.kqk);
  exports.GetCentralAgentVersions(net, ks, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PUSH ROUTER ACK-GEO-DELTA-ERROR -------------------------------------------

function push_router_send_ack_geo_delta_error(net, ks, meta, rguuid, message,
                                              next) {
  exports.GetCentralAgentVersions(net, ks, function(serr, cavrsns) {
    if (serr) next(serr, null);
    else {
      var details = create_error_details(ks, meta, null, cavrsns, null);
      ZDQ.PushRouterSendAckGeoDeltaError(net.plugin, net.collections,
                                         rguuid, ks, null, null,
                                         message, details, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIX OOO GCV TIMER ---------------------------------------------------------

var FixOOOGCVDeltaTimer = {};

function fix_ooo_gcv_delta_with_ooo_gdack(net, ks, dentry, next) {
  var meta          = dentry.delta._meta;
  var gnode         = ZCLS.GetPrimaryDataCenter();
  var rguuid        = gnode.device_uuid;
  var message       = ZS.Errors.OutOfOrderDelta;
  ks.security_token = ZH.GenerateKeySecurityToken(ks);
  push_router_send_ack_geo_delta_error(net, ks, meta, rguuid, message, next);
}

exports.FlowFixOOOGCVDelta = function(plugin, collections, qe, next) {
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var net    = qe.net;
  var ks     = qe.ks;
  var data   = qe.data;
  var dentry = data.dentry;
  ZH.e('<-|(I): ZRAD.FlowFixOOOGCVDelta: K: ' + ks.kqk);
  fix_ooo_gcv_delta_with_ooo_gdack(net, ks, dentry, function(serr, sres) {
    qnext(serr, qhres);
    next(null, null);
  });
}

function serialized_fix_ooo_gcv_delta(net, ks, dentry) {
  ZH.l('serialized_fix_ooo_gcv_delta: K: ' + ks.kqk);
  var data  = {dentry : dentry};
  var mname = 'STORAGE_FIX_OOO_GCV_DELTA';
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks, mname,
                               net, data, ZH.NobodyAuth, {}, ZH.OnErrLog);
  var flow  = {k : ks.kqk,
               q : ZQ.StorageKeySerializationQueue,
               m : ZQ.StorageKeySerialization,
               f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function set_fix_ooo_gcv_delta_timer(net, ks, dentry) {
  var author = dentry.delta._meta.author;
  var tkey   = get_fix_delta_key(ks, author);
  if (FixOOOGCVDeltaTimer[tkey]) {
    ZH.e('ALREADY SET: set_fix_ooo_gcv_delta_timer: TK: ' + tkey + ' -> NO-OP');
  } else {
    ZH.e('set_fix_ooo_gcv_delta_timer: TK: ' + tkey);
    FixOOOGCVDeltaTimer[tkey] = setTimeout(function() {
      FixOOOGCVDeltaTimer[tkey] = null;
      serialized_fix_ooo_gcv_delta(net, ks, dentry);
    }, FixOOOGCVDeltaSleep);
  }
}

function clear_fix_ooo_gcv_delta_timer(ks, dentry) {
  var author = dentry.delta._meta.author;
  var tkey   = get_fix_delta_key(ks, author);
  if (FixOOOGCVDeltaTimer[tkey]) {
    ZH.e('clear_fix_ooo_gcv_delta_timer: TK: ' + tkey);
    clearTimeout(FixOOOGCVDeltaTimer[tkey]);
    FixOOOGCVDeltaTimer[tkey] = null;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIX OOO-GCV DELTA ---------------------------------------------------------

// NOTE calls "next(null, CRDT)"
exports.PrimaryFixOOOGCVDelta = function(net, ks, dentry, md, next) {
  var dgcv   = ZH.GetDentryGCVersion(dentry);
  var author = dentry.delta._meta.author;
  var auuid  = author.agent_uuid;
  var avrsn  = author.agent_version;
  ZMerge.FreshenCentralDelta(net, ks, dentry, md, function(serr, dgcs) {
    if (serr) next(serr, null);
    else {
      ZH.e('SEND AUTO-DELTA (REORDER): K: ' + ks.kqk + ' AV: ' + avrsn);
      ZGC.SendReorderDelta(net, ks, md, dgcs, auuid, avrsn, dgcv, next);
    }
  });
}
 
// NOTE calls "next(null, CRDT)"
function fix_ooo_gcv_delta(net, ks, dentry, md, next) {
  if (ZH.ChaosMode === 32) {
    ZH.e(ZH.ChaosDescriptions[32]);
    return next(null, null);
  }
  if (ZCLS.AmPrimaryDataCenter()) {
    exports.PrimaryFixOOOGCVDelta(net, ks, dentry, md, next)
  } else {
    next(null, null);
    // NOTE: set_fix_ooo_gcv_delta_timer() is ASYNC
    set_fix_ooo_gcv_delta_timer(net, ks, dentry);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// POST APPLY DELTA ----------------------------------------------------------

function send_microservice_event(pc) {
  var ks       = pc.ks;
  var ncrdt    = pc.ncrdt;
  var pmd      = pc.post_merge_deltas;
  var gc       = pc.DO_GC;
  var reo      = pc.DO_REORDER;
  var dentry   = pc.dentry;
  var subs     = pc.extra_data.md.subs;
  var meta     = dentry.delta._meta;
  var is_geo   = meta.is_geo;
  var auuid    = meta.author.agent_uuid;
  var username = meta.author.username;
  var json     = ZH.CreatePrettyJson(ncrdt);
  var rchans   = meta.replication_channels;
  var flags    = {initialize      : pc.store  ? true  : false,
                  remove          : pc.remove ? true  : false,
                  garbage_collect : gc        ? true  : false,
                  reorder         : reo       ? true  : false,
                  is_local        : is_geo    ? false : true};
  ZMicro.SendEvent(auuid, username, ks, rchans, subs, flags, json, pmd);
}

function send_ds_delta(net, pc, dts, do_ds, next) {
  if (!do_ds) next(null, null);
  else {
    var ks   = pc.ks;
    var crdt = pc.ncrdt;
    var md   = pc.extra_data.md;
    ZH.l('send_ds_delta: K: ' + ks.kqk);
    ZDT.SendDSdelta(net, ks, md, crdt, dts, function(serr, ncrdt) {
      if (serr) next(serr, null);
      else {
        if (ncrdt) ZH.SetNewCrdt(pc, md, ncrdt, false);
        next(null, null);
      }
    });
  }
}

function send_gc_delta(net, pc, do_gc, next) {
  if (!do_gc) next(null, null);
  else {
    var ks   = pc.ks;
    var crdt = pc.ncrdt;
    var md   = pc.extra_data.md;
    ZH.l('send_gc_delta: K: ' + ks.kqk);
    ZGC.SendGCdelta(net, ks, md, crdt, function(serr, ncrdt) {
      if (serr) next(serr, null);
      else {
        if (ncrdt) ZH.SetNewCrdt(pc, md, ncrdt, true);
        next(null, null);
      }
    });
  }
}

function send_auto_delta(net, pc, do_gc, do_ds, next) {
  var ks   = pc.ks;
  var crdt = pc.ncrdt;
  ZH.l('send_auto_delta: do_gc: ' + do_gc + ' do_ds: ' + do_ds);
  var dts  = crdt._meta.DS_DTD ? ZH.clone(crdt._meta.DS_DTD) : null;
  delete(crdt._meta.DO_GC);
  delete(crdt._meta.DO_DS);
  delete(crdt._meta.DS_DTD);
  send_ds_delta(net, pc, dts, do_ds, function(serr, sres) {
    if (serr) next(serr, null);
    else      send_gc_delta(net, pc, do_gc, next);
  });
}

function determine_send_auto_delta(net, pc, next) {
  var ks         = pc.ks;
  var dentry     = pc.dentry;
  var ncrdt      = pc.ncrdt;
  var am_primary = ZCLS.AmPrimaryDataCenter();
  var do_gc      = am_primary && ncrdt && ncrdt._meta.DO_GC;
  var do_ds      = am_primary && ncrdt && ncrdt._meta.DO_DS;
  if (dentry.delta._meta.DO_DS) { // DO_DS saved in CRDT, Don't PUBLISH
    dentry.delta._meta = ZH.clone(dentry.delta._meta);
    delete(dentry.delta._meta.DO_DS);
    delete(dentry.delta._meta.DS_DTD);
  }
  if (do_gc || do_ds) send_auto_delta(net, pc, do_gc, do_ds, next);
  else                next(null, null);
}

function post_apply_check_send_auto_deltas(net, pc, next) {
  // NOTE: send_microservice_event() is ASYNC
  send_microservice_event(pc);
  determine_send_auto_delta(net, pc, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMIT CLUSTER DELTA ------------------------------------------------------

function push_router_watch_delta(net, pc, next) {
  var ks   = pc.ks;
  var md   = pc.extra_data.md;
  var subs = md.subs;
  if (!subs.has_watch) next(null, null);
  else {
    ZH.l('push_router_watch_delta: K: ' + ks.kqk);
    var dentry = pc.dentry;
    var ooogcv = dentry.delta._meta.OOO_GCV;
    if (ooogcv) next(null, null);
    else {
      var pmd = pc.post_merge_deltas;
      ZDQ.PushRouterWatchDelta(net.plugin, net.collections,
                               ks, dentry, pmd, next);
    }
  }
}

function conditional_lru_index_key(net, pc, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  var remove = dentry.delta._meta.remove;
  var expire = dentry.delta._meta.expire;
  if (!remove && !expire) {
    next(null, null);
  } else if (remove) {
    var lkey       = ZS.LRUIndex;
    var expiration = dentry.delta._meta.expiration;
    if (!expiration) next(null, null);
    else {
      ZH.l('LRU-INDEX: REMOVE: K: ' + ks.kqk);
      net.plugin.do_sorted_remove(net.collections.global_coll,
                                  lkey, ks.kqk, next);
    }
  } else if (expire) {
    var lkey       = ZS.LRUIndex;
    var expiration = dentry.delta._meta.expiration;
    ZH.l('LRU-INDEX: ADD: K: ' + ks.kqk + ' E: ' + expiration);
    net.plugin.do_sorted_add(net.collections.global_coll,
                             lkey, expiration, ks.kqk, next);
  }
}

function finalize_apply_storage_delta(net, pc, next) {
  conditional_lru_index_key(net, pc, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      ZGack.PostApplyDeltaConditionalCommit(net, pc.ks, pc.dentry,
      function(serr, sres) {
        if (serr) next(serr, null);
        else      push_router_watch_delta(net, pc, next);
      });
    }
  });
}

function process_delta_created(plugin, collections, dentry, created, next) {
  var rcreated = dentry.delta._meta.author.created;
  var ccreated = ZCLS.LastGeoCreated[rguuid];
  if (!rcreated || (ccreated && (rcreated < ccreated))) next(null, null);
  else {
    var rguuid = dentry.delta._meta.author.datacenter;
    ZCLS.SaveSingleLastGeoCreated(plugin, collections, rguuid, rcreated, next);
  }
}

function old_channels_metadata(plugin, collections, pc, is_merge, next) {
  if (is_merge || !pc.orchans) next(null, null);
  else {
    var nbytes  = pc.ncrdt ? pc.ncrdt._meta.num_bytes : 0;
    var mod     = pc.dentry.delta._meta.created[ZH.MyDataCenter];
    var orchans = ZH.clone(pc.orchans);
    ZAD.UpdateDeltaMetadata(plugin, collections,
                            pc, nbytes, mod, orchans, false, next);
  }
}

function new_channels_metadata(plugin, collections, pc, is_merge, next) {
  var nbytes  = pc.ncrdt ? pc.ncrdt._meta.num_bytes : 0;
  var dmeta   = is_merge ? pc.xcrdt._meta : pc.dentry.delta._meta;
  var mod     = dmeta.created[ZH.MyDataCenter];
  var drchans = ZH.clone(dmeta.replication_channels);
  ZAD.UpdateDeltaMetadata(plugin, collections,
                          pc, nbytes, mod, drchans, true, next);
}

function update_channels_metadata(plugin, collections, pc, is_merge, next) {
  old_channels_metadata(plugin, collections, pc, is_merge,
  function(oerr, ores) {
    if (oerr) next(oerr, null);
    else      new_channels_metadata(plugin, collections, pc, is_merge, next)
  });
}

function conditional_storage_apply_delta(net, pc, next) {
  var ks = pc.ks;
  if (pc.dentry.delta._meta.reference_uuid) {
    ZH.l('ConditionalStorageDelta: K: ' + ks.kqk + ' -> DO_REORDER');
    ZOOR.ApplyReorderDelta(net, pc, false, next);
  } else {
    ZH.l('ConditionalStorageDelta: K: ' + ks.kqk + ' -> NORMAL');
    ZAD.CentralApplyDelta(net, pc, next);
  }
}

function storage_set_agent_version(plugin, collections,
                                   ks, auuid, avrsn, next) {
  ZH.l('storage_set_agent_version: K: ' + ks.kqk +
       ' U: ' + auuid + ' AV: ' + avrsn);
  ZMDC.SetDeviceToCentralAgentVersion(plugin, collections, auuid, ks, avrsn,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var kkey = ZS.GetKeyToCentralAgentVersion(ks.kqk);
      plugin.do_set_field(collections.global_coll, kkey, auuid, avrsn, next);
    }
  });
}

function internal_apply_storage_delta(net, pc, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  var auuid  = dentry.delta._meta.author.agent_uuid;
  var avrsn  = dentry.delta._meta.author.agent_version;
  storage_set_agent_version(net.plugin, net.collections, pc.ks, auuid, avrsn,
  function(cerr, cres) {
    if (cerr) next(cerr, null);
    else {
      conditional_storage_apply_delta(net, pc, function(aerr, ares) {
        var dnsync = (aerr && aerr.message === ZS.Errors.DeltaNotSync);
        if (aerr && !dnsync) next(aerr, null);
        else {
          if (dnsync) { // DeltaNotSync still need to be COMMITTED
            ZGack.InternalAckGeoDelta(net, pc.ks, pc.dentry, next);
          } else {
            update_channels_metadata(net.plugin, net.collections, pc, false,
            function(uerr, ures) {
              if (uerr) next(uerr, null);
              else {
                var meta    = pc.dentry.delta._meta;
                var created = meta.created[ZH.MyDataCenter];
                process_delta_created(net.plugin, net.collections,
                                      pc.dentry, created,
                function(serr, sres) {
                  if (serr) next(serr, null);
                  else {
                    ZGack.InternalAckGeoDelta(net, pc.ks, pc.dentry,
                    function(werr, wres) {
                      if (werr) next(werr, null);
                      else      finalize_apply_storage_delta(net, pc, next);
                    });
                  }
                });
              }
            });
          }
        }
      });
    }
  });
}

function do_persist_storage_delta(net, ks, dentry, next) {
  ZH.l('do_persist_storage_delta: ' + ZH.SummarizeDelta(ks.key, dentry));
  var author = dentry.delta._meta.author;
  var avrsn  = author.agent_version;
  var pkey   = ZS.GetPersistedStorageDelta(ks, author);
  net.plugin.do_set_field(net.collections.delta_coll, pkey, "value", true,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var gkey = ZS.GetCentralKeyAgentVersions(ks.kqk);
      net.plugin.do_push(net.collections.delta_coll, gkey, "aversions", avrsn,
      function(perr, pres) {
        if (perr) next(perr, null);
        else {
          ZDS.CentralStoreDelta(net.plugin, net.collections, ks, dentry,
          function(aerr, ares) {
            if (aerr) next(aerr, null);
            else {
              var dkey = ZS.CentralDirtyDeltas;
              var dval = ZS.CentralDirtyDeltaKey(ks.kqk, author);
              var now  = ZH.GetMsTime();
              net.plugin.do_set_field(net.collections.global_coll,
                                      dkey, dval, now, next);
            }
          });
        }
      });
    }
  });
}

function conditional_persist_storage_delta(net, ks, dentry, next) {
  var author = dentry.delta._meta.author;
  var avrsn  = author.agent_version;
  var pkey   = ZS.GetPersistedStorageDelta(ks, author);
  net.plugin.do_get_field(net.collections.delta_coll, pkey, "value",
  function(gerr, p) {
    if (gerr) next(gerr, null);
    else {
      if (!p) do_persist_storage_delta(net, ks, dentry, next)
      else {
        ZH.l('ZRAD.persist_storage_delta: K: ' + ks.kqk +
             ' AV: ' + avrsn + ' DELTA ALREADY PERSISTED -> NO-OP');
        next(null, null);
      }
    }
  });
}

function populate_meta_auto_deltas(dentry) {
  var meta = dentry.delta._meta;
  if (!meta.is_geo && (meta.DO_GC || meta.DO_DS)) { // OVERWRITE
    meta.xaction = ZH.GenerateDeltaSecurityToken(dentry);
  }
}

function post_apply_storage_delta(net, pc, replay, next) {
  post_apply_check_send_auto_deltas(net, pc, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else { // NOTE: OOO can be queued on NORMAL StorageApplyDeltas
      if (replay) next(null, null); // NO RECURSION
      else {
        ZOOR.CheckReplayOOODeltas(net, pc, true, false, next);
      }
    }
  });
}

function do_apply_storage_delta(net, pc, replay, next) {
  populate_meta_auto_deltas(pc.dentry); // must precede ApplyDelta
  conditional_persist_storage_delta(net, pc.ks, pc.dentry,
  function (perr, pres) {
    if (perr) next(perr, null);
    else {
      internal_apply_storage_delta(net, pc, function(serr, sres) {
        if (serr) next(serr, null);
        else      post_apply_storage_delta(net, pc, replay, next);
      });
    }
  });
}

exports.RetryClusterDelta = function(plugin, collections, pc, rerr, next) {
  var ks  = pc.ks;
  var net = ZH.CreateNetPerRequest(ZH.Central);
  ZH.l('ZRAD.RetryClusterDelta: K: ' + ks.kqk);
  var internal = rerr ? false : true;
  do_apply_storage_delta(net, pc, false, function(serr, sres) {
    if (serr) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(null, null);
    }
  });
}

function add_fixlog_cdelta(plugin, collections, pc, next) {
  var ks   = pc.ks;
  var udid = exports.GetClusterDeltaID(pc);
  var ukey = ZS.FixLog;
  ZH.l('add_fixlog_cdelta: K: ' + ks.kqk + ' UD: ' + udid);
  plugin.do_set_field(collections.global_coll, ukey, udid, pc, next);
}

exports.CreateStorageDeltaPC = function(net, ks, dentry, md, next) {
  var orchans = md.ocrdt ? md.ocrdt._meta.replication_channels : null;
  var created = dentry.delta._meta.created[ZH.MyDataCenter];
  var op      = 'ClusterDelta';
  var edata   = {created : created, md : md};
  var pc      = ZH.InitPreCommitInfo(op, ks, dentry, null, edata);
  pc.orchans  = orchans;
  ZH.CalculateRchanMatch(pc, md.ocrdt, dentry.delta._meta);
  next(null, pc);
}

// NOTE calls "next(null, CRDT)"
function apply_storage_delta(net, ks, dentry, md, replay, next) {
  ZH.l('apply_storage_delta: ' + ZH.SummarizeDelta(ks.key, dentry));
  exports.CreateStorageDeltaPC(net, ks, dentry, md, function(serr, pc) {
    if (serr) next(serr, null);
    else {
      add_fixlog_cdelta(net.plugin, net.collections, pc, function(uerr, ures) {
        if (uerr) next(uerr, pc);
        else {
          var rfunc = exports.RetryClusterDelta;
          do_apply_storage_delta(net, pc, replay, function(cerr, cres) {
            if (cerr && cerr.message !== ZS.Errors.DeltaNotSync) {
             rfunc(net.plugin, net.collections, pc, cerr, next);
            } else {
              ZFix.RemoveFromFixLog(net.plugin, net.collections, pc,
              function(rerr, rres) {
                if (rerr) next(rerr, null);
                else      next(cerr, pc.ncrdt); // NOTE: cerr, NOT rerr
              });
            }
          });
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CHECK STORAGE GCV ---------------------------------------------------------
 
// NOTE calls "next(null, CRDT)"
function do_handle_storage_delta(net, ks, dentry, md, replay, next) {
  var meta   = dentry.delta._meta;
  var do_reo = meta.DO_REORDER;
  if (do_reo) meta.GC_version = md.gcv; // REORDER always GCV-OK
  apply_storage_delta(net, ks, dentry, md, replay, next);
}

// NOTE calls "next(null, CRDT)"
function handle_ok_av_storage_delta(net, ks, dentry, md, replay, next) {
  ZH.l('handle_ok_av_storage_delta: ' + ZH.SummarizeDelta(ks.key, dentry));
  ZSD.CheckDeltaGCV(net, ks, md, dentry, false, function(serr, ok) {
    if (serr) next(serr, null);
    else {
      if (ok) do_handle_storage_delta(net, ks, dentry, md, replay, next);
      else    fix_ooo_gcv_delta(net, ks, dentry, md, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CHECK STORAGE AV ----------------------------------------------------------
 
function create_error_details(ks, meta, cavrsn, cavrsns, reason) {
  return {ks                     : ks,
          author                 : meta.author,
          replication_channels   : meta.replication_channels,
          delta_info             : ZH.CreateDeltaInfo(meta),
          central_agent_version  : cavrsn,
          central_agent_versions : cavrsns,
          reason                 : reason,
          datacenter             : ZH.MyDataCenter};
}

exports.HandleUnexpectedOOODelta = function(net, ks, einfo, next) {
  var meta          = einfo.meta;
  var message       = ZS.Errors.OutOfOrderDelta;
  ks.security_token = ZH.GenerateKeySecurityToken(ks);
  if (meta.is_geo) {
    var rguuid = einfo.rguuid;
    push_router_send_ack_geo_delta_error(net, ks, meta, rguuid, message, next);
  } else {
    var auuid   = meta.author.agent_uuid;
    var details = create_error_details(ks, meta, null, null, null);
    ZDQ.PushRouterSendAckAgentDeltaError(net.plugin, net.collections,
                                         auuid, ks, null, null,
                                         message, details, next);
  }
}

function do_agent_version_check(net, ks, dentry, rguuid, md, next) {
  var author = dentry.delta._meta.author;
  var auuid  = author.agent_uuid;
  var avrsn  = author.agent_version;
  ZMDC.GetDeviceToCentralAgentVersion(net.plugin, net.collections, auuid, ks,
  function(gerr, cavrsn) {
    if (gerr) next(gerr, null);
    else {
      var cavnum = ZH.GetAvnum(cavrsn);
      var eavnum = cavnum + 1;
      var avnum  = ZH.GetAvnum(avrsn);
      var rpt    = (avnum <   eavnum);
      var oooav  = (avnum !== eavnum);
      exports.GetCentralDependencies(net, ks, function(ferr, cdeps) {
        if (ferr) next(ferr, null);
        else {
          var deps   = dentry.delta._meta.dependencies;
          var ooodep = ZH.CheckOOODependencies(ks, avrsn, deps, cdeps);
          var einfo  = {is_sub  : false,
                        kosync  : md.frozen,
                        knreo   : md.knreo,
                        ooodep  : ooodep,
                        oooav   : oooav,
                        central : false,
                        meta    : dentry.delta._meta,
                        cavrsn  : cavrsn,
                        rguuid  : rguuid};
          ZH.l('(C)AV-CHECK: K: ' + ks.kqk + ' U: ' + auuid +
               ' CAVN: ' + cavnum + ' EAVN: ' + eavnum + ' AVN: ' + avnum +
               ' RPT: ' + rpt + ' OOOAV: ' + oooav + ' OOODEP: ' + ooodep +
               ' kosync: ' + einfo.kosync + ' knreo: ' + einfo.knreo +
               ' rguuid: ' + rguuid);
          var c       = {cavrsn : cavrsn,
                         rpt    : rpt,
                         einfo  : einfo};
          next(null, c);
        }
      });
    }
  });
}

function agent_version_check(net, ks, dentry, rguuid, md, next) {
  var avrsn = dentry.delta._meta.author.agent_version;
  do_agent_version_check(net, ks, dentry, rguuid, md, function(gerr, c) {
    if (gerr) next(gerr, null);
    else {
      if (c.rpt) {
        ZH.e('REPEAT-AV: STORAGE-DELTA: K: ' + ks.kqk + ' AV: ' + avrsn);
        next(null, false);
      } else if (c.einfo.kosync) {
        ZH.e('AV-CHECK FAIL: SUBSCRIBER-DELTA: KEY NOT IN SYNC: K: ' + ks.kqk)
        ZOOR.PersistOOODelta(net, ks, dentry, c.einfo, function(serr, sres) {
          next(serr, false);
        });
      } else if (c.einfo.oooav) {
        ZH.e('OOO-AV: STORAGE DELTA: CAV: ' + c.cavrsn + ' AV: ' + avrsn);
        ZOOR.PersistOOODelta(net, ks, dentry, c.einfo, function(serr, sres) {
          next(serr, false);
        });
      } else if (c.einfo.ooodep) {
        ZOOR.PersistOOODelta(net, ks, dentry, c.einfo, function(serr, sres) {
          next(serr, false);
        });
      } else {
        next(null, true);
      }
    }
  });
}

function do_ok_internal_handle_storage_delta(net, ks, dentry, md, auto, next) {
  if (auto) { // AUTO-DELTAS are GCV-OK on REPLAY -> SKIP CheckDeltaGCV
    do_handle_storage_delta(net, ks, dentry, md, true, next);
  } else {
    handle_ok_av_storage_delta(net, ks, dentry, md, true, next);
  }
}

// NOTE: next(serr, ok, ncrdt) - 3 ARGUMENTS
exports.InternalHandleStorageDelta = function(net, ks, dentry,
                                              md, is_ref, next) {
  var avrsn  = dentry.delta._meta.author.agent_version;
  var auto   = dentry.delta._meta.AUTO;
  ZH.l('ZRAD.InternalHandleStorageDelta: K: ' + ks.kqk + ' AV: ' + avrsn +
       ' AUTO: ' + auto + ' REF: ' + is_ref);
  if (is_ref) clear_fix_ooo_gcv_delta_timer(ks, dentry);
  var rguuid = null; // NOT NEEDED for Internal handlers
  do_agent_version_check(net, ks, dentry, rguuid, md, function(gerr, c) {
    if (gerr) next(gerr, false, null);
    else {
      if (c.rpt) {
        ZH.e('REPEAT-AV: INTERNAL: K: ' + ks.kqk +
             ' AV: ' + avrsn + ' -> NO-OP');
        next(null, false, null);
      } else if (c.einfo.oooav) {
        ZH.e('OOO-AV: INTERNAL: CAV: ' + c.cavrsn +
             ' AV: ' + avrsn +' -> NO-OP');
        next(null, false, null);
      } else if (c.einfo.ooodep) {
        ZH.e('OOO-DEP: INTERNAL -> NO-OP');
        next(null, false, null);
      } else { // NOTE: kosync check not needed
        do_ok_internal_handle_storage_delta(net, ks, dentry, md, auto,
        function(serr, ncrdt) {
          next(serr, true, ncrdt);
        });
      }
    }
  });
}

function handle_ok_av_storage_delta_store_crdt(net, ks, dentry,
                                               md, replay, next) {
  handle_ok_av_storage_delta(net, ks, dentry, md, replay,
  function(serr, ncrdt) {
    if (serr) next(serr, null);
    else {
      if (!ncrdt) next(null, null);
      else {
        var avrsn = ncrdt._meta.author.agent_version;
        ZH.l('ZRAD.StoreCrdt: K: ' + ks.kqk + ' AV: ' + avrsn +
             ' GCV: ' + md.gcv);
        ZAD.StoreCrdtAndGCVersion(net, ks, ncrdt, md.gcv, 0, next);
      }
    }
  });
}

function do_storage_handle_apply_delta(net, ks, dentry, rguuid,
                                       md, replay, next) {
  agent_version_check(net, ks, dentry, rguuid, md, function(aerr, ok) {
    if (aerr) next(aerr, null);
    else {
     if (!ok) next(null, null);
     else {
       handle_ok_av_storage_delta_store_crdt(net, ks, dentry, md, replay, next);
     }
    }
  });
}

function post_handle_storage_apply_delta(net, ks, dentry, rguuid, md, next) {
  push_router_send_ack_geo_delta(net, ks, dentry, rguuid, next);
}

// NOTE: REPLAY means from DENTRIES[]
exports.StorageHandleApplyDelta = function(net, ks, dentry, rguuid, replay,
                                           hres, next) {
  ZH.l('ZRAD.StorageHandleApplyDelta: K: ' + ks.kqk);
  hres.ks = ks; // USed in response
  ZH.CleanupExternalDentry(dentry);
  exports.FetchApplyStorageDeltaMetadata(net, ks, dentry, function(ferr, md) {
    if (ferr) next(ferr, hres);
    else {
      do_storage_handle_apply_delta(net, ks, dentry, rguuid, md, replay,
      function(aerr, ares) {
        if (aerr) next(aerr, hres);
        else {
          post_handle_storage_apply_delta(net, ks, dentry, rguuid, md,
          function(uerr, ares) {
            next(uerr, hres);
          });
        }
      });
    }
  });
}

function do_storage_handle_apply_dentries(net, ks, dentries, rguuid,
                                          hres, next) {
  if (dentries.length === 0) next(null, hres);
  else {
    var dentry = dentries.shift();
    ZH.l('do_storage_handle_apply_dentries: K: ' + ks.kqk +
         ' #D: ' + dentries.length);
    exports.StorageHandleApplyDelta(net, ks, dentry, rguuid, true, hres,
    function(serr, sres) {
      if (serr) ZH.e('ERROR: storage_handle_apply_dentries: ' + serr.message);
      setImmediate(do_storage_handle_apply_dentries,
                   net, ks, dentries, rguuid, hres, next);
    })
  }
}

exports.StorageHandleApplyDentries = function(net, ks, dentries, rguuid,
                                              hres, next) {
  ZH.l('ZRAD.StorageHandleApplyDentries: K: ' + ks.kqk);
  hres.ks = ks; // USed in response
  do_storage_handle_apply_dentries(net, ks, dentries, rguuid, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AUTO_DELTA ----------------------------------------------------------------

function push_auto_dentry_to_router(net, ks, dentry, next) {
  ZGack.InternalAckGeoDelta(net, ks, dentry, function(perr, pres) {
    if (perr) next(perr, null);
    else {
      ZDQ.PushRouterBroadcastAutoDelta(net.plugin, net.collections,
                                       ks, dentry, next);
    }
  });
}

// NOTE: push_auto_dentry_to_router FIRST -> preserves ORDERING
//       send_auto_delta() may recursively send deltas
// NOTE calls "next(null, CRDT)"
exports.ProcessAutoDelta = function(net, ks, md, dentry, next) {
  ZH.l('ZRAD.ProcessAutoDelta: K: ' + ks.kqk);
  push_auto_dentry_to_router(net, ks, dentry, function(serr, sres) {
    if (serr) next(serr, null);
    else      handle_ok_av_storage_delta(net, ks, dentry, md, false, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZNM.apply_store_ack_GNM() &
//               ZPub.router_update_key_channels()
exports.UpdateChannelsMetadata = function(plugin, collections,
                                          pc, is_merge, next) {
  update_channels_metadata(plugin, collections, pc, is_merge, next);
}

// NOTE: Used by ZOOR.conditional_persist_delta()
exports.ConditionalPersistStorageDelta = function(net, ks, dentry, next) {
  conditional_persist_storage_delta(net, ks, dentry, next);
}

// NOTE: Used by ZPub.send_subscriber_server_filter_failed()
exports.CreateErrorDetails = function(ks, meta, cavrsn, cavrsns, reason) {
  create_error_details(ks, meta, cavrsn, cavrsns, reason);
}

/* TODO:
   NOTES: 
     1.) in check_cluster_delta_gcv() add delta._meta.has_added : true/false
         if (false) update dgcv to gcv withOUT Freshening
*/
