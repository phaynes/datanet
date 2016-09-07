"use strict";

var ZMerge, ZDelt, ZCache, ZAD, ZAS, ZCR, ZDack, ZOOR, ZLat;
var ZDS, ZMDC, ZFix, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZMerge = require('./zmerge');
  ZDelt  = require('./zdeltas');
  ZCache = require('./zcache');
  ZAD    = require('./zapply_delta');
  ZAS    = require('./zactivesync');
  ZCR    = require('./zcreap');
  ZDack  = require('./zdack');
  ZOOR   = require('./zooo_replay');
  ZLat   = require('./zlatency');
  ZDS    = require('./zdatastore');
  ZMDC   = require('./zmemory_data_cache');
  ZFix   = require('./zfixlog');
  ZAF    = require('./zaflow');
  ZQ     = require('./zqueue');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// APPLY SUBSCRIBER DELTA HELPERS --------------------------------------------

function save_new_crdt(net, desc, ks, md, ncrdt, next) {
  if (!ncrdt) {
    ZH.l(desc + ': NOT STORING CRDT: K: ' + ks.kqk);
    next(null, null);
  } else {
    ZH.l(desc + ': STORE CRDT: K: ' + ks.kqk);
    ZAD.StoreCrdtAndGCVersion(net, ks, ncrdt, md.gcv, 0, next);
  }
}

exports.GetSubscriberDeltaID = function(pc) {
  var ks     = pc.ks;
  var author = pc.dentry.delta._meta.author;
  var auuid  = author.agent_uuid;
  var avrsn  = author.agent_version;
  return ks.kqk + '_' + auuid + '_' + avrsn;
}

exports.CmpSubscriberDeltaID = function(pca, pcb) {
  var aarr    = pca.id.split('_');
  var a_kqk   = aarr[0];
  var a_auuid = aarr[1];
  var a_avrsn = aarr[1];
  var barr    = pcb.id.split('_');
  var b_kqk   = barr[0];
  var b_auuid = barr[1];
  var b_avrsn = barr[1];
  if (a_kqk   !== b_kqk)   return (a_kqk   > b_kqk)   ? 1 : -1;
  if (a_auuid !== b_auuid) return (a_auuid > b_auuid) ? 1 : -1;
  else {
    return (a_avrsn === b_avrsn) ? 0 : (a_avrsn > b_avrsn) ? 1 : -1;
  }
}

exports.FetchSubscriberDelta = function(plugin, collections, ks, author, next) {
  var auuid = author.agent_uuid;
  var avrsn = author.agent_version;
  ZH.l('ZSD.FetchSubscriberDelta: K: ' + ks.kqk + ' AV: ' + avrsn);
  var pkey  = ZS.GetAgentPersistDelta(ks.kqk, auuid, avrsn);
  plugin.do_get_field(collections.delta_coll, pkey, "dentry", next);
}

//TODO SEPARATE CRDT: w/ dentry call ZCache.GetMemcacheCrdtDataFromDelta()
//function fetch_local_crdt(net, ks, dentry, md, next) 
function fetch_local_crdt(net, ks, next) {
  ZDS.ForceRetrieveCrdt(net.plugin, net.collections, ks, next);
}

function get_key_crdt_gcv_metadata(net, ks, md, next) {
  ZMDC.GetGCVersion(net.plugin, net.collections, ks, function(gerr, gcv) {
    if (gerr) next(gerr, null);
    else {
      md.gcv  = gcv; // NOTE: SET MD.GCV
      md.ogcv = gcv; // NOTE: Used in ZOOR.post_check_agent_gc_wait() (MD.OGCV)
      fetch_local_crdt(net, ks, function(ferr, ocrdt) {
        if (ferr) next(ferr, null);
        else {
          md.ocrdt = ocrdt;
          next(null, null);
        }
      });
    }
  });
}

function get_subscriber_delta_key_info(net, ks, md, next) {
  var kkey = ZS.GetKeyInfo(ks);
  net.plugin.do_get_field(net.collections.kinfo_coll, kkey, "separate",
  function(gerr, sep) {
    if (gerr) next(gerr, null);
    else {
      md.sep = sep;
      net.plugin.do_get_field(net.collections.kinfo_coll, kkey, "present",
      function(uerr, pres) {
        if (uerr) next(uerr, null);
        else {
          md.present = pres;
          next(null, null);
        }
      });
    }
  });
}

exports.GetAgentSyncStatus = function(net, ks, md, next) {
  var skey = ZS.AgentKeysToSync;
  net.plugin.do_get_field(net.collections.global_coll, skey, ks.kqk,
  function(serr, tosync) {
    if (serr) next(serr, null);
    else {
      md.to_sync_key = tosync ? true : false
      var okey = ZS.AgentKeysOutOfSync;
      net.plugin.do_get_field(net.collections.global_coll, okey, ks.kqk,
      function(oerr, osync) {
        if (oerr) next(oerr, null);
        else {
          md.out_of_sync_key = osync ? true : false;
          next(null, md);
        }
      });
    }
  });
}

function fetch_local_key_metadata(net, ks, md, next) {
  get_key_crdt_gcv_metadata(net, ks, md, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      ZMDC.GetAgentWatchKeys(net.plugin, net.collections, ks, ZH.MyUUID,
      function(werr, watch) {
        if (werr) next(werr, null);
        else {
          md.watch = watch;
          get_subscriber_delta_key_info(net, ks, md, function(gerr, gres) {
            if (gerr) next(gerr, null);
            else {
              var dk_key = ZS.GetDeviceToKeys(ZH.MyUUID);
              net.plugin.do_get_field(net.collections.dtok_coll, dk_key, ks.kqk,
              function (uerr, cached) {
                if (uerr) next(uerr, hres);
                else {
                  md.need_evict = cached && (!md.ocrdt && md.present);
                  exports.GetAgentSyncStatus(net, ks, md, next)
                }
              });
            }
          });
        }
      });
    }
  });
}

function fetch_subscriber_delta_metadata(net, ks, dentry, next) {
  var md    = {};
  var auuid = dentry.delta._meta.author.agent_uuid;
  fetch_local_key_metadata(net, ks, md, function(ferr, md) {
    if (ferr) next(ferr, null);
    else {
      var akey = ZS.GetAgentKeyOOOGCV(ks, auuid);
      net.plugin.do_get_field(net.collections.global_coll, akey, "value",
      function(gerr, need) {
        if (gerr) next(gerr, null);
        else {
          if (need) md.knreo = true;
          next(null, md);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// POST APPLY SUBSCRIBER DELTA CLEANUP ---------------------------------------

function post_apply_cleanup(net, ks, dentry, md, next) {
  var author = dentry.delta._meta.author;
  ZOOR.DoRemoveOOODelta(net.plugin, net.collections, ks, author,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      ZLat.AddToSubscriberLatencies(net, ks, dentry, function(serr, sres) {
        if (serr) next(serr, null);
        else {
          if (!md.need_evict) next(null, null);
          else { // EVICTED BY EXTERNAL CACHE
            ZCache.AgentEvict(net, ks, true, {}, next);
          }
        }
      });
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// APPLY SUBSCRIBER DELTA ----------------------------------------------------

function finalize_apply_subscriber_delta(net, pc, next) {
  ZAS.PostApplyDeltaConditionalCommit(net, pc.ks, pc.dentry,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var created = pc.dentry.delta._meta.created
      ZDack.SetAgentCreateds(net.plugin, net.collections, created,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          next(null, null);
          if (!pc.extra_data.md.is_merge) {
            // NOTE: BELOW is ASYNC
            ZDelt.OnDataChange(net.plugin, net.collections, pc, false, false);
          }
        }
      });
    }
  });
}

function conditional_apply_subscriber_delta(net, pc, next) {
  var ks = pc.ks;
  if (pc.extra_data.watch) {
    ZH.l('ConditionalSubscriberDelta K: ' + ks.kqk + ' -> WATCH');
    var merge = {post_merge_deltas : pc.dentry.post_merge_deltas};
    assign_post_merge_deltas(pc, merge);
    next(null, null);
  } else if (pc.dentry.delta._meta.reference_uuid) {
    ZH.l('ConditionalSubscriberDelta: K: ' + ks.kqk + ' -> DO_REORDER');
    ZOOR.ApplyReorderDelta(net, pc, true, next);
  } else {
    ZH.l('ConditionalSubscriberDelta: K: ' + ks.kqk + ' -> NORMAL');
    ZAD.ApplySubscriberDelta(net, pc, next);
  }
}

function do_set_subscriber_agent_version(plugin, collections,
                                         ks, avrsn, auuid, next) {
  var skey   = ZS.GetDeviceSubscriberVersion(auuid);
  var sentry = {value : avrsn, when : ZH.GetMsTime()};
  ZH.l('do_set_subscriber_AV: K: ' + ks.kqk + ' U: ' + auuid + ' AV: ' + avrsn);
  plugin.do_set_field(collections.key_coll, skey, ks.kqk, sentry,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var kkey = ZS.GetPerKeySubscriberVersion(ks.kqk);
      plugin.do_set_field(collections.key_coll, kkey, auuid, avrsn, next);
    }
  });
}

function set_subscriber_agent_version(plugin, collections, pc, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  var meta   = dentry.delta._meta;
  var avrsn  = meta.author.agent_version;
  var auuid  = meta.author.agent_uuid;
  do_set_subscriber_agent_version(plugin, collections, ks, avrsn, auuid, next);
}

function internal_apply_subscriber_delta(net, pc, next) {
  var ks       = pc.ks;
  var md       = pc.extra_data.md;
  var o_nbytes = md.ocrdt ? md.ocrdt._meta.num_bytes : 0;
  set_subscriber_agent_version(net.plugin, net.collections, pc,
  function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      conditional_apply_subscriber_delta(net, pc, function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          var rchans = pc.dentry.delta._meta.replication_channels;
          ZMDC.CasKeyRepChans(net.plugin, net.collections, ks, rchans,
           function(berr, bres) {
             if (berr) next(berr, null);
             else {
               ZCR.StoreNumBytes(net, pc, o_nbytes, false,
               function(serr, sres) {
                 if (serr) next(serr, null);
                 else {
                   finalize_apply_subscriber_delta(net, pc, next);
                 }
              });
            }
          });
        }
      });
    }
  });
}

function do_persist_subscriber_delta(net, ks, dentry, next) {
  ZH.l('do_persist_subscriber_delta: ' + ZH.SummarizeDelta(ks.kqk, dentry));
  var author = dentry.delta._meta.author;
  var auuid  = author.agent_uuid;
  var avrsn  = author.agent_version;
  var pkey   = ZS.GetPersistedSubscriberDelta(ks, author);
  net.plugin.do_set_field(net.collections.delta_coll, pkey, "value", true,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var dkey = ZS.GetAgentKeyDeltaVersions(ks.kqk);
      net.plugin.do_push(net.collections.delta_coll, dkey, "aversions", avrsn,
      function(verr, vres) {
        if (verr) next(verr, null);
        else {
          ZDS.StoreSubscriberDelta(net.plugin, net.collections, ks, dentry,
          function(serr, sres) {
            if (serr) next(serr, null);
            else {
              var now  = ZH.GetMsTime();
              var dkey = ZS.AgentDirtyDeltas;
              var dval = ZS.GetDirtySubscriberDeltaKey(ks.kqk, author);
              net.plugin.do_set_field(net.collections.global_coll,
                                      dkey, dval, now, next);
            }
          });
        }
      });
    }
  });
}

function conditional_persist_subscriber_delta(net, ks, dentry, next) {
  var author = dentry.delta._meta.author;
  var avrsn  = author.agent_version;
  var pkey   = ZS.GetPersistedSubscriberDelta(ks, author);
  net.plugin.do_get_field(net.collections.delta_coll, pkey, "value",
  function(gerr, p) {
    if (gerr) next(gerr, null);
    else {
      if (!p) do_persist_subscriber_delta(net, ks, dentry, next);
      else {
        ZH.l('ZDS.persist_subscriber_delta: K: ' + ks.kqk +
             ' AV: ' + avrsn + ' DELTA ALREADY PERSISTED -> NO-OP');
        next(null, null);
      }
    }
  });
}

function do_apply_subscriber_delta(net, pc, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  conditional_persist_subscriber_delta(net, ks, dentry, function(serr, sres) {
    if (serr) next(serr, null);
    else      internal_apply_subscriber_delta(net, pc, next);
  });
}

exports.RetrySubscriberDelta = function(plugin, collections, pc, rerr, next) {
  var ks  = pc.ks;
  var net = ZH.CreateNetPerRequest(ZH.Agent);
  ZH.l('ZSD.RetrySubscriberDelta: K: ' + ks.kqk);
  var internal = rerr ? false : true;
  do_apply_subscriber_delta(net, pc, function(serr, sres) {
    if (serr && serr.message !== ZS.Errors.DeltaNotSync) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(null, null);
    }
  });
}

function add_fixlog_sdelta(plugin, collections, pc, next) {
  var ks   = pc.ks;
  var udid = exports.GetSubscriberDeltaID(pc);
  var ukey = ZS.FixLog;
  ZH.l('add_fixlog_sdelta: K: ' + ks.kqk + ' UD: ' + udid);
  plugin.do_set_field(collections.global_coll, ukey, udid, pc, next);
}

exports.CreateSubscriberDeltaPC = function(net, ks, dentry, md, next) {
  var watch   = md.watch;
  var sep     = md.sep;
  var op      = 'SubscriberDelta';
  var edata   = {watch : watch, md : md};
  var pc      = ZH.InitPreCommitInfo(op, ks, dentry, null, edata);
  pc.separate = sep;
  ZH.CalculateRchanMatch(pc, md.ocrdt, dentry.delta._meta);
  next(null, pc);
}

// NOTE calls "next(null, CRDT)"
function apply_subscriber_delta(net, ks, dentry, md, replay, next) {
  exports.CreateSubscriberDeltaPC(net, ks, dentry, md, function(serr, pc) {
    if (serr) next(serr, null);
    else {
      add_fixlog_sdelta(net.plugin, net.collections, pc, function(uerr, ures) {
        if (uerr) next(uerr, null);
        else {
          var rfunc = exports.RetrySubscriberDelta;
          do_apply_subscriber_delta(net, pc, function(cerr, cres) {
            if (cerr && cerr.message !== ZS.Errors.DeltaNotSync) {
              rfunc(net.plugin, net.collections, pc, cerr, next);
            } else {
              ZFix.RemoveFromFixLog(net.plugin, net.collections, pc,
              function(rerr, rres) {
                if (rerr) next(rerr, null);
                else {
                  if (cerr && cerr.message === ZS.Errors.DeltaNotSync) {
                    next(cerr, null); // NOTE: cerr, NOT rerr
                  } else {
                    post_apply_subscriber_delta(net, pc, replay,
                    function(aerr, ares) {
                      next(aerr, pc.ncrdt); // NOTE: next(CRDT)
                    });
                  }
                }
              });
            }
          });
        }
      });
    }
  });
}

function post_apply_subscriber_delta(net, pc, replay, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  var md     = pc.extra_data.md;
  var avrsn  = dentry.delta._meta.author.agent_version;
  ZH.l('post_apply_subscriber_delta: K: ' + ks.kqk + ' AV: ' + avrsn +
      ' REPLAY: ' + replay);
  post_apply_cleanup(net, ks, dentry, md, function(serr, sres) {
    if (serr) next(serr, null);
    else { // NOTE: OOO can be queued on NORMAL SubscriberDeltas
      if (replay) next(null, null); // NO RECURSION
      else        ZOOR.CheckReplayOOODeltas(net, pc, true, true, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CHECK SUBSCRIBER GCV ------------------------------------------------------

exports.CheckDeltaGCV = function(net, ks, md, dentry, is_sub, next) {
  var auto      = dentry.delta._meta.AUTO;
  var do_reo    = dentry.delta._meta.DO_REORDER;
  var do_reap   = dentry.delta._meta.DO_REAP;
  var do_ignore = dentry.delta._meta.DO_IGNORE;
  var no_check  = (auto || do_reo || do_reap || do_ignore);
  if (no_check) {
    ZH.l('zsd_check_delta_gcv: NO_CHECK: CGCV: ' + md.gcv);
    next(null, true);
  } else {
    var dgcv = ZH.GetDentryGCVersion(dentry);
    ZH.l('DELTA: CHECK_GCV: K: ' + ks.kqk + ' GCV: ' + md.gcv +
        ' DGCV: ' + dgcv);
    if (dgcv >= md.gcv) { // dgcv > gcv, delta contains post GC deltas -> SAFE
      var author = dentry.delta._meta.author;
      ZOOR.UnsetOOOGCVDelta(net, ks, author, function(serr, sres) {
        next(serr, true);
      });
    } else {
      var avrsn = dentry.delta._meta.author.agent_version;
      ZH.e('DELTA: OOO-GCV: K: ' + ks.kqk + ' AV: ' + avrsn +
           ' GCV: ' + md.gcv + ' DGCV: ' + dgcv);
      ZOOR.HandleOOOGCVDelta(net, ks, md, dentry, is_sub,
      function(serr, ncrdt) {
        if (serr) next(serr, null);
        else {
          save_new_crdt(net, "ZSD.CheckDeltaGCV", ks, md, ncrdt,
          function(aerr, ares) {
            next(aerr, false);
          });
        }
      });
    }
  }
}

// NOTE calls "next(null, CRDT)"
function handle_ok_av_subscriber_delta(net, ks, dentry, md, replay, next) {
  exports.CheckDeltaGCV(net, ks, md, dentry, true, function(uerr, ok) {
    if (uerr) next(uerr, null);
    else {
      if (!ok) next(null, null); // OOOGCV -> NO-OP (wait on REORDER) 
      else     apply_subscriber_delta(net, ks, dentry, md, replay, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CHECK SUBSCRIBER AV -------------------------------------------------------

exports.HandleUnexpectedOOODelta = function(net, ks, einfo, next) {
  ZH.e('UNEXPECTED OOO-SD (UNEXSD) -> SYNC-KEY: K: ' + ks.kqk);
  ZAS.SetAgentSyncKeySignalAgentToSyncKeys(net, ks, next);
}

function get_check_sdav_metadata(net, ks, dentry, next) {
  var avmd  = {};
  var auuid = dentry.delta._meta.author.agent_uuid;
  var skey  = ZS.GetDeviceSubscriberVersion(auuid);
  net.plugin.do_get_field(net.collections.key_coll, skey, ks.kqk,
  function(gerr, sentry) {
    if (gerr) next(gerr, null);
    else {
      avmd.sentry = sentry;
      ZDelt.GetAgentDeltaDependencies(net, ks, function(ferr, mdeps) {
        if (ferr) next(ferr, null);
        else {
          avmd.mdeps = mdeps;
          next(null, avmd);
        }
      });
    }
  });
}

function do_check_subscriber_delta_agent_version(net, ks, dentry, md, next) {
  var meta  = dentry.delta._meta;
  var auuid = meta.author.agent_uuid;
  var avrsn = meta.author.agent_version;
  get_check_sdav_metadata(net, ks, dentry, function(gerr, avmd) {
    if (gerr) next(gerr, null);
    else {
      var savrsn = null;
      var savnum = 0;
      var ts     = Number.MAX_VALUE;
      if (avmd.sentry) {
        savrsn = avmd.sentry.value;
        ts     = avmd.sentry.when;
        savnum = ZH.GetAvnum(savrsn);
      }
      var einfo  = {is_sub  : true,
                    kosync  : (md.to_sync_key || md.out_of_sync_key),
                    knreo   : md.knreo,
                    central : meta.from_central,
                    oooav   : false,
                    ooodep  : false,
                    is_reo  : false};
      var rconn  = ZH.AgentLastReconnect;
      var wskip  = (md.watch && (ts < rconn));
      if (wskip) { // WATCHED KEYS skip first AV-check when returning online
        var c = {savrsn : savrsn,
                 einfo  : einfo,
                 rpt    : false};
        next(null, c);
      } else {
        var eavnum   = savnum + 1;
        var avnum    = ZH.GetAvnum(avrsn);
        var rpt      = (avnum <   eavnum);
        var oooav    = (avnum !== eavnum);
        var deps     = meta.dependencies;
        var ooodep   = ZH.CheckOOODependencies(ks, avrsn, deps, avmd.mdeps);
        var is_reo   = dentry.delta._meta.reference_uuid ? true : false
        einfo.oooav  = oooav;
        einfo.ooodep = ooodep;
        einfo.is_reo = is_reo;
        ZH.l('(S)AV-CHECK: K: ' + ks.kqk + ' U: ' + auuid +
             ' SAVN: ' + savnum + ' EAVN: ' + eavnum + ' AVN: ' + avnum +
             ' watch: ' + md.watch + ' RPT: ' + rpt + ' REO: ' + is_reo +
             ' OOOAV: ' + oooav + ' OOODEP: ' + ooodep +
             ' kosync: ' + einfo.kosync + ' knreo: ' + einfo.knreo +
             ' central: '  + einfo.central);
        var c = {savrsn : savrsn,
                 rpt    : rpt,
                 einfo  : einfo};
        next(null, c);
      }
    }
  });
}


function save_post_agent_gc_wait_crdt(net, pc, next) {
  var ks    = pc.ks;
  var ncrdt = pc.ncrdt;
  var md    = pc.extra_data.md;
  save_new_crdt(net, "save_post_agent_gc_wait_crdt", ks, md, ncrdt, next);
}

function check_subscriber_delta_av_fail(net, ks, dentry, md, c, next) {
  if (!c.einfo.is_reo) next(null, null);
  else {
    var avrsn = dentry.delta._meta.author.agent_version;
    ZH.l('check_subscriber_delta_av_fail: K: ' + ks.kqk  + ' AV: ' + avrsn);
    exports.CreateSubscriberDeltaPC(net, ks, dentry, md, function(serr, pc) {
      if (serr) next(serr, null);
      else {
        ZOOR.DecrementReorderDelta(net, pc, function(aerr, ares) {
          if (aerr) next(aerr, null);
          else      save_post_agent_gc_wait_crdt(net, pc, next);
        });
      }
    });
  }
}

function handle_subscriber_delta_av_fail(net, ks, dentry, md, c, next) {
  var avrsn = dentry.delta._meta.author.agent_version;
  ZH.e('OOO-AV SUBSCRIBER-DELTA: SAV: ' + c.savrsn + ' AV: ' + avrsn);
  ZOOR.PersistOOODelta(net, ks, dentry, c.einfo, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      check_subscriber_delta_av_fail(net, ks, dentry, md, c,
      function(aerr, ares) {
        next(aerr, false);
      });
    }
  });
}

function check_subscriber_delta_agent_version(net, ks, dentry, md, next) {
  var avrsn = dentry.delta._meta.author.agent_version;
  do_check_subscriber_delta_agent_version(net, ks, dentry, md,
  function(gerr, c) {
    if (gerr) next(gerr, null);
    else {
      if (c.rpt) {
        ZH.e('REPEAT SUBSCRIBER-DELTA: K: ' + ks.kqk + ' AV: ' + avrsn);
        next(null, false);
      } else if (c.einfo.kosync) {
        ZH.e('AV-CHECK FAIL: SUBSCRIBER-DELTA: KEY NOT IN SYNC: K: ' + ks.kqk);
        ZOOR.PersistOOODelta(net, ks, dentry, c.einfo, function(serr, sres) {
          next(serr, false);
        });
      } else if (c.einfo.oooav) {
        handle_subscriber_delta_av_fail(net, ks, dentry, md, c, next);
      } else if (c.einfo.ooodep) {
        ZH.e('OOO-DEP SUBSCRIBER-DELTA: K: ' + ks.kqk);
        ZOOR.PersistOOODelta(net, ks, dentry, c.einfo, function(serr, sres) {
          next(serr, false);
        });
      } else {
        next(null, true);
      }
    }
  });
}

function do_ok_internal_handle_subscriber_delta(net, ks, dentry, md, fc, next) {
  // (FC)AUTO-DELTAS are GCV-OK on REPLAY -> SKIP CheckDeltaGC
  if (fc) apply_subscriber_delta       (net, ks, dentry, md, true, next);
  else    handle_ok_av_subscriber_delta(net, ks, dentry, md, true, next);
}

// NOTE: next(serr, ok, ncrdt) - 3 ARGUMENTS
exports.InternalHandleSubscriberDelta = function(net, ks, dentry, md, next) {
  var avrsn = dentry.delta._meta.author.agent_version;
  var fc    = dentry.delta._meta.from_central;
  ZH.l('ZSD.InternalHandleSubscriberDelta: K: ' + ks.kqk + ' AV: ' + avrsn +
       ' FC: ' + fc);
  do_check_subscriber_delta_agent_version(net, ks, dentry, md,
  function(gerr, c) {
    if (gerr) next(gerr, false, null);
    else {
      if (c.rpt) {
        ZH.e('REPEAT-AV: INTERNAL: AV: ' + avrsn + ' -> NO-OP');
        next(null, false, null);
      } else if (c.einfo.oooav) {
        ZH.e('OOO-AV: INTERNAL: SAV: ' + c.savrsn +
             ' AV: ' + avrsn + ' -> NO-OP');
        next(null, false, null);
      } else if (c.einfo.ooodep) {
        ZH.e('OOO-DEP: INTERNAL -> NO-OP');
        next(null, false, null);
      } else { // NOTE: kosync check not needed
        do_ok_internal_handle_subscriber_delta(net, ks, dentry, md, fc,
        function(serr, ncrdt) {
          next(serr, true, ncrdt);
        });
      }
    }
  });
}

// NOTE calls "next(null, CRDT)"
function do_handle_apply_subscriber_delta(net, ks, dentry, md, replay, next) {
  ZH.l('do_handle_apply_subscriber_delta: K: ' + ks.kqk + ' (C)GCV: ' + md.gcv);
  check_subscriber_delta_agent_version(net, ks, dentry, md, function(serr, ok) {
    if (serr) next(serr, null);
    else {
      if (!ok) next(null, null);
      else {
        handle_ok_av_subscriber_delta(net, ks, dentry, md, replay, next);
      }
    }
  });
}

function do_flow_handle_subscriber_delta(net, ks, dentry, replay, next) {
  fetch_subscriber_delta_metadata(net, ks, dentry, function(ferr, md) {
    if (ferr) next(ferr, null);
    else {
      do_handle_apply_subscriber_delta(net, ks, dentry, md, replay,
      function(serr, ncrdt) {
        if (serr) next(serr, null);
        else      save_new_crdt(net, "subscriber_delta", ks, md, ncrdt, next);
      });
    }
  });
}

exports.FlowSubscriberDelta = function(plugin, collections, qe, next) {
  var net    = qe.net;
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var ks     = qe.ks;
  var data   = qe.data;
  var dentry = data.dentry;
  var replay = data.replay;
  do_flow_handle_subscriber_delta(net, ks, dentry, replay,
  function(cerr, cres) {
    qnext(cerr, qhres);
    next (null, null);
  });
}

// NOTE: REPLAY means from DENTRIES[]
exports.HandleSubscriberDelta = function(net, ks, dentry, replay, hres, next) {
  ZH.l('ZSD.HandleSubscriberDelta: ' + ZH.SummarizeDelta(ks.kqk, dentry));
  ZH.CleanupExternalDentry(dentry);
  dentry.delta._meta.subscriber_received = ZH.GetMsTime();
  var data = {dentry : dentry, replay : replay};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'SUBSCRIBER_DELTA',
                       net, data, null, hres, next);
  var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function do_handle_subscriber_dentries(net, ks, dentries, hres, next) {
  if (dentries.length === 0) next(null, null);
  else {
    ZH.l('do_handle_subscriber_dentries: K: ' + ks.kqk +
         ' #D: ' + dentries.length);
    var dentry    = dentries.shift();
    var meta      = dentry.delta._meta;
    var rchans    = ZH.GetReplicationChannels(meta);
    hres.channels = rchans;
    exports.HandleSubscriberDelta(net, ks, dentry, true, hres,
    function(serr, sres) {
      if (serr) ZH.l('ERROR: handle_subscriber_dentries: ' + serr.message);
      setImmediate(do_handle_subscriber_dentries,
                   net, ks, dentries, hres, next);
    });
  }
}

exports.HandleSubscriberDentries = function(net, ks, dentries, hres, next) {
  ZH.l('ZSD.HandleSubscriberDentries:  K: ' + ks.kqk +
       ' #D: ' + dentries.length);
  do_handle_subscriber_dentries(net, ks, dentries, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZSM.store_ether()
exports.ConditionalPersistSubscriberDelta = function(net, ks, dentry, next) {
  conditional_persist_subscriber_delta(net, ks, dentry, next);
}

// NOTE: Used by ZDelt.do_apply_store_agent_delta() &
//               ZRollback.rollback_agent_delta()
exports.DoSetSubscriberAgentVersion = function(plugin, collections,
                                               ks, avrsn, auuid, next) {
  do_set_subscriber_agent_version(plugin, collections, ks, avrsn, auuid, next);
}

// NOTE: Used by ZSM.process_apply_subscriber_merge()
exports.FetchLocalKeyMetadata = function(net, ks, md, next) {
  fetch_local_key_metadata(net, ks, md, next);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZSD']={} : exports);

