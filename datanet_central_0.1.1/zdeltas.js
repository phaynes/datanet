"use strict";

var ZMerge, ZDoc, ZConv, ZXact, ZDack, ZAD, ZSD, ZCR, ZCOAL;
var ZCache, ZOplog, ZGC, ZRollback;
var ZOOR, ZAio, ZAuth, ZMDC, ZDS, ZFix, ZNotify, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZMerge    = require('./zmerge');
  ZDoc      = require('./zdoc');
  ZConv     = require('./zconvert');
  ZXact     = require('./zxaction');
  ZDack     = require('./zdack');
  ZAD       = require('./zapply_delta');
  ZSD       = require('./zsubscriber_delta');
  ZCR       = require('./zcreap');
  ZCOAL     = require('./zcoalesce');
  ZCache    = require('./zcache');
  ZOplog    = require('./zoplog');
  ZGC       = require('./zgc');
  ZRollback = require('./zrollback');
  ZOOR      = require('./zooo_replay');
  ZAio      = require('./zaio');
  ZAuth     = require('./zauth');
  ZMDC      = require('./zmemory_data_cache');
  ZDS       = require('./zdatastore');
  ZFix      = require('./zfixlog');
  ZNotify   = require('./znotify');
  ZAF       = require('./zaflow');
  ZQ        = require('./zqueue');
  ZS        = require('./zshared');
  ZH        = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET NEXT RPC ID (CENTRAL & AGENT) -----------------------------------------

//TODO move to ZH
var NextRpcId = 1; // Rpc-ID is reset on ANY (Central or Agent) restart
exports.GetNextRpcID = function(plugin, collections, cnode) {
  NextRpcId += 1;
  return ZH.MyUUID + '-' + cnode.device_uuid + '-' + NextRpcId;
}

exports.GetNextAgentRpcID = function(plugin, collections) {
  NextRpcId += 1;
  return ZH.MyUUID + '-' + ZH.MyDataCenter   + '-' + NextRpcId;
}

exports.GetNextSubscriberRpcID = function(plugin, collections, sub) {
  NextRpcId += 1;
  return ZH.MyUUID + '-' + sub.UUID          + '-' + NextRpcId;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ON DATA CHANGE ------------------------------------------------------------

function callback_on_data_change(authed, pc, ks, crdt, pmd, full_doc, selfie) {
  var json = ZH.CreatePrettyJson(crdt);
  var flags = {selfie             : selfie,
               full_document_sync : full_doc,
               remove             : pc.remove ? true : false,
               initialize         : pc.store  ? true : false}
  var ecb = authed ? ZS.EngineCallback.DataChange :
                     ZS.EngineCallback.PreAuthorizationDataChange;
  if (!ecb) return;
  try {
    ecb(ks, json, pmd, flags);
  } catch(e) {
    ZH.e('USERLAND-ERROR: callback_on_data_change: ' + e.message);
  }
}

function do_on_data_change(plugin, collections, pc, full_doc, selfie) {
  var ks     = pc.ks;
  var crdt   = pc.ncrdt;
  var pmd    = pc.post_merge_deltas;
  var auth   = ZH.BrowserAuth;
  if (!auth) return;
  if (auth === ZH.NobodyAuth) {
    callback_on_data_change(false, pc, ks, crdt, pmd, full_doc, selfie);
  } else {
    var net = ZH.CreateNetPerRequest(ZH.Agent);
    ZAuth.HasReadPermissions(net, auth, ks, ZH.MyUUID, function(aerr, ok) {
      if (aerr) ZH.e(aerr);
      else {
        if (!ok) return;
        callback_on_data_change(true, pc, ks, crdt, pmd, full_doc, selfie);
      }
    });
  }
}

exports.OnDataChange = function(plugin, collections, pc, full_doc, selfie) {
  var ks = pc.ks;
  ZH.l('ZDelt.OnDataChange: K: ' + ks.kqk);
  if (ZH.AmBrowser) {
    if (ZS.EngineCallback.DataChange ||
        ZS.EngineCallback.PreAuthorizationDataChange) {
      do_on_data_change(plugin, collections, pc, full_doc, selfie);
    }
  } else {
    if (ZS.EngineCallback.Notify) {
     ZNotify.NotifyOnDataChange(plugin, collections, pc, full_doc, selfie);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE JSON HELPERS --------------------------------------------------------

function create_crdt_from_json(ks, jdata) {
  var key   = jdata._id;
  var rchans;
  if (ZH.IsDefined(jdata._channels)) {
    rchans  = jdata._channels;
  }
  var expiration;
  if (ZH.IsDefined(jdata._expire)) {
    var nows   = (ZH.GetMsTime() / 1000);
    expiration = nows + Number(jdata._expire);
    delete(jdata._expire);
  }
  var sfname;
  if (ZH.IsDefined(jdata._server_filter)) {
    sfname  = jdata._server_filter;
  }
  var meta  = ZH.InitMeta(ks.ns, ks.cn, key, rchans, expiration, sfname);
  var json  = ZH.clone(jdata); // clone -> delete keyword fields
  for (var i = 0; i < ZH.DocumentKeywords.length; i++) {
    var kw = ZH.DocumentKeywords[i];
    delete(json[kw]); // extract KWs from CRDT's body(_data)
  }
  var crdtd = ZConv.ConvertJsonObjectToCrdt(json, meta, false);
  return      ZH.FormatCrdtForStorage(key, meta, crdtd);
}

function init_first_delta(crdt) {
  var meta  = crdt._meta;
  var delta = ZH.InitDelta(meta);
  var data  = crdt._data.V;
  for (var dkey in data) { // [+] ALL first level members
    data[dkey]["+"] = true;
  }
  return delta;
}

exports.GetAgentAgentVersions = function(net, ks, next) {
  ZH.l('ZDelt.GetAgentAgentVersions');
  var kkey = ZS.GetPerKeySubscriberVersion(ks.kqk);
  net.plugin.do_get(net.collections.key_coll, kkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var avrsns = gres[0];
        delete(avrsns._id);
        next(null, avrsns);
      }
    }
  });
}

exports.GetAgentDeltaDependencies = function(net, ks, next) {
  ZH.l('ZDelt.GetAgentDeltaDependencies');
  exports.GetAgentAgentVersions(net, ks, function(gerr, avrsns) {
    if (gerr) next(gerr, null);
    else {
      if (!avrsns) next(null, null);
      else {
        delete(avrsns[ZH.MyUUID]);
        next(null, avrsns);
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DRAIN DELTA QUEUE (FROM AGENT TO CENTRAL) ---------------------------------

exports.DeleteMetaTimings = function(dentry) {
  var meta = dentry.delta._meta;
  delete(meta.agent_received);
  delete(meta.geo_sent);
  delete(meta.geo_received);
  delete(meta.subscriber_sent);
  delete(meta.subscriber_received);
}

function cleanup_agent_delta_before_send(dentry) {
  var meta = dentry.delta._meta;
  delete(meta.is_geo); // Fail-safe
  delete(meta.last_member_count);
  delete(meta.op_count);
  delete(meta.overwrite);
  if (meta.removed_channels && !meta.removed_channels.length) {
    delete(meta.removed_channels);
  }
  if (meta.initial_delta === false) {
    delete(meta.initial_delta);
  }
}

function drain_agent_delta(net, ks, dentry, auth, next) {
  ZH.l('drain_agent_delta: ' + ZH.SummarizeDelta(ks.key, dentry));
  var avrsn = dentry.delta._meta.author.agent_version;
  var now   = ZH.GetMsTime();
  var dkey  = ZS.AgentSentDeltas;
  var dval  = ZS.GetSentAgentDeltaKey(ks.kqk, avrsn);
  ZH.l('AgentSentDeltas: SET: dval: ' + dval);
  net.plugin.do_set_field(net.collections.global_coll, dkey, dval, now,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      cleanup_agent_delta_before_send(dentry);
      // NOTE: ZAio.SendCentralAgentDelta() is ASYNC
      ZAio.SendCentralAgentDelta(net.plugin, net.collections, ks, dentry, auth,
                                 ZH.OnErrLog);
      next(null, null);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMIT AGENT DELTA --------------------------------------------------------

function online_drain_agent_delta(net, pc, dentry, next) {
  var ks      = pc.ks;
  var offline = ZH.GetAgentOffline();
  ZH.l('online_drain_agent_delta:  K: ' + ks.kqk + ' OFF: ' + offline);
  if (offline) next(null, null);
  else         drain_agent_delta(net, ks, dentry, pc.auth, next);
}

function post_commit_agent_delta(net, pc, next) {
  ZH.l('post_commit_agent_delta: K: ' + pc.ks.kqk);
  online_drain_agent_delta(net, pc, pc.pdentry, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var applied = {crdt : pc.ncrdt};
      next (null, applied);
      // NOTE: BELOW is ASYNC
      exports.OnDataChange(net.plugin, net.collections, pc, false, true);
    }
  });
}

exports.GetAgentDeltaID = function(pc) {
  var udid = pc.ks.kqk + '-' + pc.new_agent_version;
  ZH.l('ZDelt.GetAgentDeltaID: UD: ' + udid);
  return udid;
}

exports.CmpAgentDeltaID = function(pca, pcb) {
  var aarr   = pca.id.split('-');
  var akqk   = aarr[0];
  var aavrsn = aarr[1];
  var barr   = pcb.id.split('-');
  var bkqk   = barr[0];
  var bavrsn = barr[1];
  if (akqk !== bkqk) return (akqk > bkqk) ? 1 : -1;
  else { 
    return (aavrsn === bavrsn) ? 0 : (aavrsn > bavrsn) ? 1 : -1;
  } 
}

function push_agent_version(plugin, collections, ks, navrsn, avinc, next) {
  if (avinc === 0) next(null, null); // CoalescedDelta already pushed
  else {
    var ks   = ks;
    var dkey = ZS.GetAgentKeyDeltaVersions(ks.kqk);
    plugin.do_push(collections.delta_coll, dkey, "aversions", navrsn, next);
  }
}

function store_agent_delta(net, ks, dentry, navrsn, avinc, auth, next) {
  var author = {agent_uuid : ZH.MyUUID, agent_version : navrsn};
  var pkey   = ZS.GetPersistedSubscriberDelta(ks, author);
  net.plugin.do_set_field(net.collections.delta_coll, pkey, "value", true,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      push_agent_version(net.plugin, net.collections, ks, navrsn, avinc,
      function(perr, pres) {
        if (perr) next(perr, null);
        else {
          ZDS.StoreAgentDelta(net.plugin, net.collections, ks, dentry, auth,
          function(aerr, ares) {
            if (aerr) next(aerr, null);
            else {
              var now  = ZH.GetMsTime();
              var dkey = ZS.AgentDirtyDeltas;
              var dval = ZS.GetDirtyAgentDeltaKey(ks.kqk, ZH.MyUUID, navrsn);
              net.plugin.do_set_field(net.collections.global_coll,
                                      dkey, dval, now, next);
            }
          });
        }
      });
    }
  });
}

// NOTE: MUST PRECEDE ZCR.StoreNumBytes()
function commit_delta_set_auto_cache(net, pc, next) {
  var ks     = pc.ks;
  var acache = pc.dentry.delta._meta.auto_cache
  if (!acache) next(null, null);
  else         ZCache.AgentStoreCacheKeyMetadata(net, ks, false, pc.auth, next);
}

function finish_commit_delta(net, pc, o_nbytes, next) {
  var ks = pc.ks;
  commit_delta_set_auto_cache(net, pc, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      ZCR.StoreNumBytes(net, pc, o_nbytes, true, function(serr, sres) {
        if (serr) next(serr, null);
        else      ZDS.StoreCrdt(net, ks, pc.ncrdt, next);
      });
    }
  });
}

// NOTE: pdentry is publish-dentry -> may be a coalesced delta
//       pc.dentry is apply-dentry -> delta generated from oplog
function do_commit_delta(net, pc, pdentry, next) {
  pc.pdentry   = pdentry;
  var ks       = pc.ks;
  var md       = pc.extra_data.md;
  var o_nbytes = md.ocrdt ? md.ocrdt._meta.num_bytes : 0;
  var akey     = ZS.GetKeyAgentVersion(ks.kqk);
  var navrsn   = pc.new_agent_version;
  net.plugin.do_set_field(net.collections.key_coll, akey, "value", navrsn,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      ZSD.DoSetSubscriberAgentVersion(net.plugin, net.collections,
                                      ks, navrsn, ZH.MyUUID,
      function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          ZAD.AgentApplyDelta(net, pc, pc.dentry, function(werr, wres) {
            if (werr) next(werr, null);
            else {
              var adkey  = ZS.AgentDirtyKeys;
              net.plugin.do_increment(net.collections.global_coll,
                                      adkey, ks.kqk, 1,
              function(aerr, ares) {
                if (aerr) next(aerr, null);
                else {
                  store_agent_delta(net, ks, pc.pdentry, navrsn,
                                    pc.avinc, pc.auth,
                  function(uerr, ures) {
                    if (uerr) next(uerr, null);
                    else      finish_commit_delta(net, pc, o_nbytes, next);
                  });
                }
              });
            }
          });
        }
      });
    }
  });
}

function add_fixlog_agent_delta(plugin, collections, pc, next) {
  var ks   = pc.ks;
  var udid = exports.GetAgentDeltaID(pc);
  ZH.l('add_fixlog_agent_delta: K: ' + ks.kqk + ' UD: ' + udid);
  var ukey = ZS.FixLog;
  plugin.do_set_field(collections.global_coll, ukey, udid, pc, next);
}

exports.DoCommitDelta = function(net, cinfo, avinc, next) {
  var ks   = cinfo.ks;
  var akey = ZS.GetKeyAgentVersion(ks.kqk);
  net.plugin.do_get_field(net.collections.key_coll, akey, "value",
  function(gerr, oavrsn) {
    if (gerr) next(gerr, null);
    else {
      var dentry                 = cinfo.dentry;
      var oavnum                 = ZH.GetAvnum(oavrsn);
      var navnum                 = oavnum + avinc;
      var new_agent_version      = ZH.CreateAvrsn(ZH.MyUUID, navnum);
      var dmeta                  = dentry.delta._meta;
      dmeta.author.agent_version = new_agent_version;
      var md                     = {gcv   : cinfo.gcv,
                                    ocrdt : cinfo.ocrdt};
      var op    = 'AgentDelta';
      var edata = {md : md};
      var pc    = ZH.InitPreCommitInfo(op, ks, dentry, null, edata);
      if (!ZH.CalculateRchanMatch(pc, md.ocrdt, dentry.delta._meta)) {
        next(new Error(ZS.Errors.RchanChange), null);
      } else {
        //TODO next 8 variables should be in EDATA
        pc.separate          = cinfo.sep;
        pc.expiration        = cinfo.locex;
        pc.auth              = cinfo.auth;
        pc.gcv               = cinfo.gcv;
        pc.odentry           = cinfo.odentry;
        pc.avinc             = avinc;
        pc.old_agent_version = oavrsn;
        pc.new_agent_version = new_agent_version;
        add_fixlog_agent_delta(net.plugin, net.collections, pc,
        function(uerr, ures) {
          if (uerr) next(uerr, null);
          else {
            var rfunc = ZRollback.RollbackAdelta;
            do_commit_delta(net, pc, cinfo.pdentry, function(cerr, cres) {
              if (cerr && cerr.message !== ZS.Errors.DeltaNotSync) {
                rfunc(net.plugin, net.collections, pc, cerr, next);
              } else {
                ZFix.RemoveFromFixLog(net.plugin, net.collections, pc,
                function(uerr, ures) {
                  if (uerr) next(uerr, null);
                  else {
                    if (cerr && cerr.message === ZS.Errors.DeltaNotSync) {
                      next(cerr, null); // NOTE: cerr, NOT rerr
                    } else {
                      post_commit_agent_delta(net, pc, next);
                    }
                  }
                });
              }
            });
          }
        });
      }
    }
  });
}

function check_auto_cache(plugin, collections, ks, dentry, auth, next) {
  var net    = ZH.CreateNetPerRequest(ZH.Agent);
  var meta   = dentry.delta._meta;
  var rchans = meta.replication_channels;
  ZAuth.IsAgentAutoCacheKey(net, auth, ks, rchans, function(aerr, acache) {
    if (aerr) next(aerr, null);
    else {
      if (acache) meta.auto_cache = true;
      next(null, null);
    }
  });
}

// NOTE: AgentDeltas authored at MAX-GCV
function commit_agent_delta(net, ks, dentry, ocrdt, sep, locex, auth, next) {
  ZH.l('commit_agent_delta: ' + ZH.SummarizeDelta(ks.key, dentry));
  var cinfo = {ks : ks, ocrdt : ocrdt, sep : sep, locex : locex, auth : auth};
  check_auto_cache(net.plugin, net.collections, ks, dentry, auth,
  function(cerr, cres) {
    if (cerr) next(cerr, null);
    else {
      var mkey = ZS.GetAgentKeyMaxGCVersion(ks.kqk);
      net.plugin.do_get_field(net.collections.key_coll, mkey, "gc_version",
      function(gerr, mgcv) {
        if (gerr) next(gerr, null);
        else {
          cinfo.gcv = mgcv ? mgcv : 0;
          ZMerge.FreshenAgentDelta(net, ks, dentry, ocrdt, cinfo.gcv,
          function(ferr, ndentry) {
            if (ferr) next(ferr, null);
            else {
              if (!ndentry) next(new Error(ZS.Errors.StaleAgentDelta), null);
              else {
                cinfo.dentry = ndentry;
                var adkey    = ZS.AgentDirtyKeys;
                net.plugin.do_get_field(net.collections.global_coll,
                                        adkey, ks.kqk,
                function(derr, ndirty) {
                  if (derr) next(derr, null);
                  else {
                    if (!ndirty) ndirty = 0;
                    cinfo.ndirty  = ndirty;
                    cinfo.pdentry = ndentry; // DEFAULT -> NOT COALESCED
                    cinfo.odentry = null;    // DEFAULT -> NOT COALESCED
                    ZCOAL.CheckCoalesceDeltas(net, cinfo, next);
                  }
                });
              }
            }
          });
        }
      });
    }
  });
}

function finalize_commit_dentry(net, ks, ocrdt, ncrdt, delta,
                                oplog, remove, expire, next) {
  var ncnt      = ZXact.GetOperationCount(ncrdt, oplog, remove, expire, delta);
  var meta      = ncrdt._meta;
  meta.op_count = ncnt;
  ZXact.ReserveOpIdRange(net.plugin, net.collections, ZH.MyUUID, ncnt,
  function(uerr, dvrsn) {
    if (uerr) next(uerr, null);
    else {
      var ts     = ZH.GetMsTime();
      dvrsn      = ZXact.UpdateCrdtMetaAndAdded(ncrdt, ZH.MyUUID,
                                                ncnt, dvrsn, ts);
      var perr   = ZXact.FinalizeDeltaForCommit(ncrdt, delta, dvrsn, ts);
      if (perr) return next(perr, null);
      var dentry = ZH.CreateDentry(delta, ks);
      next(null, dentry);
    }
  });
}

// NOTE: ToSyncKeys: modifiable, whereas OutOfSyncKeys: temporarily frozen
function check_out_of_sync_key(plugin, collections, ks, next) {
  var skey = ZS.AgentKeysOutOfSync;
  plugin.do_get_field(collections.global_coll, skey, ks.kqk,
  function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres) {
        ZH.e('COMMIT ON OUT-OF-SYNC K: ' + ks.kqk + ' -> FAIL');
        next(new Error(ZS.Errors.CommitOnOutOfSyncKey), null);
      } else {
        next(null, null);
      }
    }
  });
}

function __do_create_client_store_dentry(net, ks, jdata, oplog, ocrdt, next) {
  ZMDC.GetGCVersion(net.plugin, net.collections, ks, function(gerr, gcv) {
    if (gerr) next(gerr, null);
    else {
      var overwrite = ocrdt ? true : false;
      var ncrdt     = create_crdt_from_json(ks, jdata);
      var delta     = init_first_delta(ncrdt);
      exports.GetAgentDeltaDependencies(net, ks, function(ferr, deps) {
        if (ferr) next(ferr, null);
        else {
          var meta           = ncrdt._meta;
          ZH.InitAuthor(meta, deps);
          meta.initial_delta = true;
          meta.overwrite     = overwrite;
          meta.GC_version    = gcv ? gcv : 0
          var kkey           = ZS.GetKeyInfo(ks);
          net.plugin.do_set_field(net.collections.kinfo_coll,
                                  kkey, "present", true,
          function(aerr, ares) {
            if (aerr) next(aerr, null);
            else {
              var remove = false;
              var expire = false;
              finalize_commit_dentry(net, ks, ocrdt, ncrdt,
                                     delta, oplog, remove, expire, next);
            }
          });
        }
      });
    }
  });
}

function do_create_client_store_delta(net, ks, jdata, oplog, sep, locex, ocrdt,
                                      auth, next) {
  ZH.l('do_create_client_store_delta: K: ' + ks.kqk + ' SEP: ' + sep);
  __do_create_client_store_dentry(net, ks, jdata, oplog, ocrdt,
  function(perr, dentry) {
    if (perr) next(perr, null);
    else {
      commit_agent_delta(net, ks, dentry, ocrdt, sep, locex, auth, next);
    }
  });
}

function __do_create_client_commit_delta(net, ks, ccrdt, oplog, ocrdt, next) {
  ZOplog.CreateDelta(ccrdt, oplog, function(verr, delta) {
    if (verr) next(verr, null);
    else {
      exports.GetAgentDeltaDependencies(net, ks, function(ferr, deps) {
        if (ferr) next(ferr, null);
        else {
          var meta           = ccrdt._meta;
          ZH.InitAuthor(meta, deps);
          meta.initial_delta = false;
          var remove         = false;
          var expire         = false;
          finalize_commit_dentry(net, ks, ocrdt, ccrdt, delta,
                                 oplog, remove, expire, next);
        }
      });
    }
  });
}

function do_create_client_commit_delta(net, ks, ccrdt, oplog, sep, locex, ocrdt,
                                       auth, next) {
  ZH.l('do_create_client_commit_delta: K: ' + ks.kqk + ' SEP: ' + sep);
  check_out_of_sync_key(net.plugin, net.collections, ks, function(cerr, cres) {
    if (cerr) next(cerr, null);
    else {
      __do_create_client_commit_delta(net, ks, ccrdt, oplog, ocrdt,
      function(perr, dentry) {
        if (perr) next(perr, null);
        else {
          commit_agent_delta(net, ks, dentry, ocrdt, sep, locex, auth, next);
        }
      });
    }
  });
}

function __do_create_client_remove_delta(net, ks, ocrdt, next) {
  if (!ocrdt) next(new Error(ZS.Errors.NoKeyToRemove), null);
  else {
    exports.GetAgentDeltaDependencies(net, ks, function(ferr, deps) {
      if (ferr) next(ferr, null);
      else {
        var meta           = ocrdt._meta;
        ZH.InitAuthor(meta, deps);
        meta.remove        = true;
        meta.initial_delta = false;
        var delta          = ZH.InitDelta(meta);
        var remove         = true;
        var expire         = false;
        finalize_commit_dentry(net, ks, ocrdt, ocrdt, delta,
                               null, remove, expire, next);
      }
    });
  }
}

function do_create_client_remove_delta(net, ks, sep, locex, ocrdt, auth, next) {
  ZH.l('do_create_client_remove_delta: K: ' + ks.kqk);
  check_out_of_sync_key(net.plugin, net.collections, ks, function(cerr, cres) {
    if (cerr) next(cerr, null);
    else {
      __do_create_client_remove_delta(net, ks, ocrdt, function(perr, dentry) {
        if (perr) next(perr, null);
        else {
          commit_agent_delta(net, ks, dentry, ocrdt, sep, locex, auth, next);
        }
      });
    }
  });
}

function __do_create_client_expire_delta(net, ks, expiration, ocrdt, next) {
  if (!ocrdt) next(new Error(ZS.Errors.NoKeyToExpire), null);
  else {
    exports.GetAgentDeltaDependencies(net, ks, function(ferr, deps) {
      if (ferr) next(ferr, null);
      else {
        var meta           = ocrdt._meta;
        ZH.InitAuthor(meta, deps);
        meta.expire        = true;
        meta.expiration    = expiration
        meta.initial_delta = false;
        var delta          = ZH.InitDelta(meta);
        var remove         = false;
        var expire         = true;
        finalize_commit_dentry(net, ks, ocrdt, ocrdt, delta,
                               null, remove, expire, next);
      }
    });
  }
}

function do_create_client_expire_delta(net, ks, expiration, sep, locex, ocrdt,
                                       auth, next) {
  ZH.l('do_create_client_expire_delta: K: ' + ks.kqk);
  check_out_of_sync_key(net.plugin, net.collections, ks, function(cerr, cres) {
    if (cerr) next(cerr, null);
    else {
      __do_create_client_expire_delta(net, ks, expiration, ocrdt,
     function(perr, dentry) {
        if (perr) next(perr, null);
        else {
          commit_agent_delta(net, ks, dentry, ocrdt, sep, locex, auth, next);
        }
      });
    }
  });
}

function do_create_client_delta(net, ks, store, commit, remove, expire,
                                jdata, ccrdt, oplog, expiration,
                                sep, locex, auth, next) {
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function (ferr, ocrdt) {
    if (ferr) next(ferr, null);
    else {
      if (store) {         // STORE
        do_create_client_store_delta(net, ks, jdata, oplog, sep, locex, ocrdt,
                                     auth, next);
      } else if (commit) { // COMMIT
        do_create_client_commit_delta(net, ks, ccrdt, oplog, sep, locex, ocrdt,
                                      auth, next)
      } else if (remove) { // REMOVE
        do_create_client_remove_delta(net, ks, sep, locex, ocrdt, auth, next)
      } else { // EXPIRE
        do_create_client_expire_delta(net, ks, expiration, sep, locex, ocrdt,
                                      auth, next)
      }
    }
  });
}

function create_client_delta(net, ks, store, commit, remove, expire,
                             jdata, ccrdt, oplog, expiration,
                             sep, locex, auth, next) {
  ZH.l('create_client_delta: store: ' + store + ' remove: ' + remove +
       ' commit: ' + commit);
  if (store) { // STORE
    do_create_client_delta(net, ks, store, commit, remove, expire,
                           jdata, ccrdt, oplog, expiration,
                           sep, locex, auth, next);
  } else {     // COMMIT/REMOVE/EXPIRE
    ZMDC.GetAgentWatchKeys(net.plugin, net.collections, ks, ZH.MyUUID,
    function(gerr, watched) {
      if (gerr) next(gerr, null);
      else {
        if (watched) { // WATCH
          next(new Error(ZS.Errors.ModifyWatchKey), null);
        } else {
          var kkey = ZS.GetKeyInfo(ks);
          net.plugin.do_get_field(net.collections.kinfo_coll, kkey, "separate",
          function(gerr, sep) {
            if (gerr) next(gerr, null);
            else {
               do_create_client_delta(net, ks, store, commit, remove, expire,
                                      jdata, ccrdt, oplog, expiration,
                                      sep, locex, auth, next);
            }
          });
        }
      }
    });
  }
}

function do_flow_commit_delta(plugin, collections, qe, next) {
  var net        = qe.net;
  var ks         = qe.ks;
  var auth       = qe.auth;
  var data       = qe.data;
  var store      = data.store;
  var commit     = data.commit;
  var remove     = data.remove;
  var expire     = data.expire;
  var jdata      = data.jdata;
  var ccrdt      = data.ccrdt;
  var oplog      = data.oplog;
  var expiration = data.expiration;
  var sep        = data.sep;
  var locex      = data.locex;
  create_client_delta(net, ks, store, commit, remove, expire,
                      jdata, ccrdt, oplog, expiration,
                      sep, locex, auth, next);
}

exports.FlowCommitDelta = function(plugin, collections, qe, next) {
  var qnext = qe.next;
  var qhres = qe.hres;
  do_flow_commit_delta(plugin, collections, qe, function(uerr, applied) {
    qhres.applied = applied; // NOTE: Used in response
    qnext(uerr, qhres);
    next (null, null);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-STORE/COMMIT/REMOVE -------------------------------------------------

exports.AgentStore = function(net, ks, sep, locex, jdata, auth, hres, next) {
  var err = ZH.ValidateJson(jdata);
  if (err) next(err, hres);
  else {
    var data = {store  : true,
                commit : false,
                remove : false,
                expire : false,
                jdata  : jdata,
                sep    : sep,
                locex  : locex};
    ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'CLIENT_COMMIT',
                         net, data, auth, hres, next);
    var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
                m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}

exports.AgentCommit = function(net, ks, ccrdt, oplog, locex, auth, hres, next) {
  ZH.l('AgentCommit: K: ' + ks.kqk); ZH.p(oplog);
  if (oplog.length === 0) {
    ZDoc.CreateZDoc(ks.ns, ks.cn, ks.key, ccrdt, function(cerr, cres) {
      if (cerr) next(cerr, hres);
      else {
        var applied  = {crdt : ccrdt};
        hres.applied = applied;
        next(null, hres);
      }
    });
  } else {
    var data = {store  : false,
                commit : true,
                remove : false,
                expire : false,
                ccrdt  : ccrdt,
                oplog  : oplog,
                locex  : locex};
    ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'CLIENT_COMMIT',
                         net, data, auth, hres, next);
    var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
                m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}

exports.AgentRemove = function(net, ks, auth, hres, next) {
  var data = {store  : false,
              commit : false,
              remove : true,
              expire : false};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'CLIENT_COMMIT',
                       net, data, auth, hres, next);
  var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


//TODO move REST to zdeltas_advanced.js
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-MEMCACHE-COMMIT -----------------------------------------------------

//TODO move to zmemcache_commit.js
function check_separate_status(net, ks, sep, next) {
  var kkey = ZS.GetKeyInfo(ks);
  net.plugin.do_get(net.collections.kinfo_coll, kkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var kinfo = gres[0];
        var osep  = kinfo.separate;
        if (sep === osep) next(null, null);
        else {
          ZH.e('MC: REMOVING OLD KEY FIRST: K: ' + ks.kqk + ' OSEP: ' + osep);
          ZDS.RemoveKey(net, ks, osep, next);
        }
      }
    }
  });
}

function do_nonstandard_commit(net, ks, oplog, crdt, sep, locex,
                               sticky, auth, next) {
  var kkey   = ZS.GetKeyInfo(ks);
  var kentry = {present : true, sticky : sticky, separate : sep};
  net.plugin.do_set(net.collections.kinfo_coll, kkey, kentry,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var ncrdt = ZH.clone(crdt);
      var ocrdt = ZH.clone(crdt);
      do_create_client_commit_delta(net, ks, ncrdt, oplog, sep, locex, ocrdt,
                                    auth, next);
    }
  });
}

function do_memcache_commit(net, ks, oplog, crdt, sep, locex, auth, next) {
  ZH.l('do_memcache_commit: K: ' + ks.kqk);
  var sticky = true;
  do_nonstandard_commit(net, ks, oplog, crdt, sep, locex, sticky, auth, next);
}

function do_memcache_store(net, ks, vjson, sep, locex, auth, next) {
  ZH.l('do_memcache_store: K: ' + ks.kqk);
  var kkey = ZS.GetKeyInfo(ks);
  net.plugin.do_set_field(net.collections.kinfo_coll, kkey, "separate", sep,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      do_create_client_store_delta(net, ks, vjson, null, sep, locex, null,
                                   auth, next);
    }
  });
}

//TODO FIXLOG for 2 steps (StoreVirgin & CommitOplog)
function handle_virgin_memcache_commit(net, acrdt, ks, oplog, rchans,
                                       sep, locex, auth, next) {
  if (acrdt) next(null, false);
  else {
    if (!rchans) next(new Error(ZS.Errors.VirginNoRepChans), null);
    else {
      var jdata = ZMerge.CreateVirginJsonFromOplog(ks, rchans, oplog);
      //ZH.l('handle_virgin_memcache_commit: JSON'); ZH.p(jdata);
      do_memcache_store(net, ks, jdata, sep, locex, auth, function(serr, sres) {
        if (serr) next(serr, null);
        else {
          process_flow_memcache_commit(net, ks, oplog, rchans,
                                       sep, locex, auth, next);
        }
      });
    }
  }
}

// NOTE: RECACHE needed when MEMCACHE contents do not cover OPLOG
function force_recache_then_commit(net, ks, oplog, rchans,
                                   sep, locex, auth, next) {
  var pin      = false;
  var watch    = false;
  var sticky   = true;
  var force    = true;
  var internal = true;
  ZH.l('FORCE-RECACHE: K: ' + ks.kqk);
  ZCache.AgentCache(net, ks, pin, watch, sticky, force, internal, auth, {},
  function(aerr, ares) {
    if (aerr && aerr.message !== ZS.Errors.NoDataFound) next(serr, null);
    else {
      var acrdt = ares.applied ? ares.applied.crdt : null;
      do_memcache_commit(net, ks, oplog, acrdt, sep, locex, auth, next);
    }
  });
}

function process_flow_memcache_commit(net, ks, oplog, rchans, sep, locex,
                                      auth, next) {
  var pin      = false;
  var watch    = false;
  var sticky   = true;
  var force    = false;
  var internal = true;
  ZCache.AgentCache(net, ks, pin, watch, sticky, force, internal, auth, {},
   function(serr, sres) {
    if (serr && serr.message !== ZS.Errors.NoDataFound) next(serr, null);
    else {
      var acrdt = sres.applied ? sres.applied.crdt : null;
      handle_virgin_memcache_commit(net, acrdt, ks, oplog, rchans,
                                    sep, locex, auth,
      function(verr, handled) {
        if (verr) next(verr, null);
        else {
          if (handled) next(null, handled); // COMMITTED in handler()
          else {
            check_separate_status(net, ks, sep, function(aerr, ares) {
              if (aerr) next(aerr, null);
              else {
                if (acrdt._data) { // FULL CRDT CACHED
                  do_memcache_commit(net, ks, oplog, acrdt, sep, locex,
                                     auth, next);
                } else {          // SEPARATE CRDT
                  ZCache.GetMemcacheCrdtDataFromOplog(net, ks, acrdt, oplog,
                  function(gerr, dcrdt) {
                    if (gerr) next(gerr, null);
                    else {
                      if (dcrdt) { // HIT -> ALL FIELDS CACHED
                        do_memcache_commit(net, ks, oplog, dcrdt, sep, locex,
                                           auth, next);
                      } else {     // MISS - FIELD NOT CACHED
                        force_recache_then_commit(net, ks, oplog, rchans,
                                                  sep, locex, auth, next);
                      }
                    }
                  });
                }
              }
            });
          }
        }
      });
    }
  });
}

function do_flow_memcache_commit_delta(net, qe, next) {
  var ks       = qe.ks;
  var auth     = qe.auth;
  var data     = qe.data;
  var oplog    = data.oplog;
  var rchans   = data.rchans;
  var sep      = data.sep;
  var locex    = data.locex;
  ZH.l('memcache_commit_delta: K: ' + ks.kqk + ' SEP: ' + sep + ' X: ' + ex);
  process_flow_memcache_commit(net, ks, oplog, rchans, sep, locex, auth, next);
}

exports.FlowMemcacheCommitDelta = function(plugin, collections, qe, next) {
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  do_flow_memcache_commit_delta(net, qe, function(uerr, applied) {
    qhres.applied = applied; // NOTE: Used in response
    qnext(uerr, qhres);
    next (null, null);
  });
}

exports.AgentMemcacheCommit = function(net, ks, oplog, rchans, sep, locex,
                                       auth, hres, next) {
  ZH.l('ZDelt.AgentMemcacheCommit: K: ' + ks.kqk);
  var data = {oplog : oplog, rchans : rchans, sep : sep, locex : locex};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks,
                       'CLIENT_MEMCACHE_COMMIT',
                       net, data, auth, hres, next);
  var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT STATELESS COMMIT ----------------------------------------------------

function do_stateless_commit(net, ks, oplog, crdt, auth, next) {
  ZH.l('do_stateless_commit: K: ' + ks.kqk);
  var sep    = false;
  var locex  = 0;
  var sticky = false;
  do_nonstandard_commit(net, ks, oplog, crdt, sep, locex, sticky, auth, next);
}

//TODO FIXLOG for 2 steps (StoreVirgin & CommitOplog)
function process_flow_stateless_commit(net, ks, oplog, rchans, auth, next) {
  var pin      = false;
  var watch    = false;
  var sticky   = false;
  var force    = false;
  var internal = true;
  ZCache.AgentCache(net, ks, pin, watch, sticky, force, internal, auth, {},
   function(serr, sres) {
    if (serr && serr.message !== ZS.Errors.NoDataFound) next(serr, null);
    else {
      var acrdt = sres.applied ? sres.applied.crdt : null;
      if (acrdt) {
        do_stateless_commit(net, ks, oplog, acrdt, auth, next);
      } else {
        if (!rchans) next(new Error(ZS.Errors.VirginNoRepChans), null);
        else {
          var sep   = false;
          var locex = 0;
          var jdata = ZMerge.CreateVirginJsonFromOplog(ks, rchans, oplog);
          //ZH.l('virgin_statless_commit: JSON'); ZH.p(jdata);
          do_create_client_store_delta(net, ks, jdata, null,
                                       sep, locex, null, auth,
          function(serr, sres) {
            if (serr) next(serr, null);
            else {
              process_flow_stateless_commit(net, ks, oplog, rchans, auth, next);
            }
          });
        }
      }
    }
  });
}

function do_flow_stateless_commit_delta(net, qe, next) {
  var ks       = qe.ks;
  var auth     = qe.auth;
  var data     = qe.data;
  var oplog    = data.oplog;
  var rchans   = data.rchans;
  ZH.l('stateless_commit_delta: K: ' + ks.kqk);
  process_flow_stateless_commit(net, ks, oplog, rchans, auth, next);
}

exports.FlowStatelessCommitDelta = function(plugin, collections, qe, next) {
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  do_flow_stateless_commit_delta(net, qe, function(uerr, applied) {
    qhres.applied = applied; // NOTE: Used in response
    qnext(uerr, qhres);
    next (null, null);
  });
}

exports.AgentStatelessCommit = function(net, ks, oplog, rchans,
                                        auth, hres, next) {
  ZH.l('ZDelt.AgentStatlessCommit: K: ' + ks.kqk);
  var data = {oplog : oplog, rchans : rchans};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks,
                       'CLIENT_STATELESS_COMMIT',
                       net, data, auth, hres, next);
  var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL CLIENT DIRECT CALLS -----------------------------------------------

exports.StorageCreateClientStoreDelta = function(net, ks, json, next) {
  ZH.l('ZDelt.StorageCreateClientStoreDelta: K: ' + ks.kqk);
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function (ferr, ocrdt) {
    if (ferr) next(ferr, null);
    else {
      __do_create_client_store_dentry(net, ks, json, null, ocrdt,
      function(perr, dentry) {
        if (perr) next(perr, null);
        else {
          var cres  = {dentry : dentry};
          next(null, cres);
        }
      });
    }
  });
}

exports.StorageCreateClientCommitDelta = function(net, ks, ccrdt, oplog, next) {
  ZH.l('ZDelt.StorageCreateClientCommitDelta: K: ' + ks.kqk);
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function (ferr, ocrdt) {
    if (ferr) next(ferr, null);
    else {
      __do_create_client_commit_delta(net, ks, ccrdt, oplog, ocrdt,
      function(perr, dentry) {
        if (perr) next(perr, null);
        else {
          var cres  = {dentry : dentry};
          next(null, cres);
        }
      });
    }
  });
}

exports.StorageCreateClientRemoveDelta = function(net, ks, next) {
  ZH.l('ZDelt.StorageCreateClientRemoveDelta: K: ' + ks.kqk);
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function (ferr, ocrdt) {
    if (ferr) next(ferr, null);
    else {
      __do_create_client_remove_delta(net, ks, ocrdt, function(perr, dentry) {
        if (perr) next(perr, null);
        else {
          var cres  = {dentry : dentry};
          next(null, cres);
        }
      });
    }
  });
}

function do_create_client_stateless_commit_delta(net, ks, rchans,
                                                 oplog, ocrdt, dentries, next) {
  var ccrdt = ZH.clone(ocrdt);
  __do_create_client_commit_delta(net, ks, ccrdt, oplog, ocrdt,
  function(perr, dentry) {
    if (perr) next(perr, null);
    else {
      dentries.push(dentry);
      var cres  = {dentries : dentries};
      next(null, cres);
    }
  });
}

//TODO FIXLOG for 2 steps (StoreVirgin & CommitOplog)
exports.StorageCreateClientStatelessCommitDelta = function(net, ks, rchans,
                                                           oplog, next) {
  ZH.l('ZDelt.StorageCreateClientStatelessCommitDelta: K: ' + ks.kqk);
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function (ferr, ocrdt) {
    if (ferr) next(ferr, null);
    else {
      var dentries = [];
      if (!ocrdt) {
        var jdata = ZMerge.CreateVirginJsonFromOplog(ks, rchans, oplog);
        __do_create_client_store_dentry(net, ks, jdata, null, null,
        function(perr, dentry) {
          if (perr) next(perr, null);
          else {
            dentries.push(dentry);
            //TODO FIXME DENTRY NO LONGER HAS CRDT -> THIS IS BROKEN
            var ncrdt = ZH.clone(dentry.crdt); // BROKEN
            do_create_client_stateless_commit_delta(net, ks, rchans, oplog,
                                                    ncrdt, dentries, next);
          }
        });
      } else {
        do_create_client_stateless_commit_delta(net, ks, rchans, oplog,
                                                ocrdt, dentries, next);
      }
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE PULL ZDOC FROM CLIENT ----------------------------------------------

//TODO move to zpull.js
function no_crdt_found(applied) {
  applied.crdt  = null;
  applied.json  = null;
  applied.oplog = null;
}

// TODO KQK SERIALIZATION
exports.AgentPull = function(net, ks, crdt, oplog, auth, hres, next) {
  var duuid = ZH.MyUUID;
  ZOplog.CreateDelta(crdt, oplog, function(verr, delta) {
    if (verr) next(verr, hres);
    else {
      var remove   = false;
      var expire   = false;
      var ncnt     = ZXact.GetOperationCount(crdt, oplog,
                                             remove, expire, delta);
      hres.applied = {};
      ZXact.ReserveOpIdRange(net.plugin, net.collections, duuid, ncnt,
      function(uerr, dvrsn) {
        if (uerr) next(uerr, null);
        else {
          ZDS.RetrieveCrdt(net.plugin, net.collections, ks,
          function (ferr, ocrdt) {
            if (ferr) next(ferr, null);
            else {
              if (!ocrdt) {
                no_crdt_found(hres.applied);
                next(null, hres);
              } else {
                ZXact.AssignCrdtInternals(crdt, duuid, ncnt, dvrsn)
                var perr = ZXact.PrepCrdtDeltaForCommit(crdt, delta);
                if (perr) return next(perr, hres);
                var a_mismatch = ZMerge.CompareZDocAuthors(ocrdt._meta,
                                                           delta._meta);
                if (a_mismatch) {
                  var dcreated = delta._meta.document_creation["@"];
                  var fcreated = ocrdt._meta.document_creation["@"];
                  if (dcreated < fcreated) {
                    ZH.l('PULL: Local NEWER -> RETURN Local');
                    hres.applied.crdt  = ocrdt;
                    hres.applied.json  = ZH.CreatePrettyJson(ocrdt);
                    hres.applied.oplog = [];
                    next(null, hres);
                  } else { // DELTA NEWER
                    next(new Error(ZS.Errors.PullDeltaClock), null);
                  }
                } else {
                  if (delta._meta.remove === true) {
                    ZH.l('PULL: RemoveKey -> RETURN NULL');
                    no_crdt_found(hres.applied);
                    next(null, hres);
                  } else {
                    var merge = ZMerge.ApplyDeltas(ocrdt, delta, false);
                    ZH.l('PULL: NORMAL');
                    var crdtd = merge.crdtd;
                    var meta  = ZH.clone(ocrdt._meta);
                    var mcrdt = ZH.FormatCrdtForStorage(ks.key, meta, crdtd);
                    // Fetched CRDT
                    hres.applied.crdt = ocrdt;
                    // Merged JSON
                    hres.applied.json = ZH.CreatePrettyJson(mcrdt);
                    hres.applied.oplog = oplog;
                    next(null, hres);
                  }
                }
              }
            }
          });
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE FETCH FROM CLIENT --------------------------------------------------

function do_flow_agent_fetch(net, ks, hres, next) {
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function (ferr, ocrdt) {
    if (ferr) next(ferr, hres);
    else {
      if (!ocrdt) next(new Error(ZS.Errors.NoDataFound), hres);
      else {
        ZCR.SetLruLocalRead(net, ks, function(serr, sres) {
          if (serr) next(serr, null);
          else {
            hres.applied    = {crdt : ocrdt};
            hres.datacenter = ZH.MyDataCenter; // Used in reply
            hres.connected  = ZH.Agent.cconn;  // Used in reply
            next(null, hres);
          }
        });
      }
    }
  });
}

exports.FlowAgentFetch = function(plugin, collections, qe, next) {
  var net   = qe.net;
  var qnext = qe.next;
  var qhres = qe.hres;
  var ks    = qe.ks;
  do_flow_agent_fetch(net, ks, qhres, function(uerr, applied) {
    qnext(uerr, qhres);
    next (null, null);
  });
}

exports.AgentFetch = function(net, ks, hres, next) {
  var data = {};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'CLIENT_FETCH',
                       net, data, null, hres, next);
  var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE EXPIRE FROM CLIENT -------------------------------------------------

exports.AgentExpire = function(net, ks, expire, auth, hres, next) {
  if (isNaN(expire)) next(new Error(ZS.Errors.ExpireNaN), hres);
  else {
    var expiration = (ZH.GetMsTime() / 1000) + Number(expire);
    var data       = {store      : false,
                      commit     : false,
                      remove     : false,
                      expire     : true,
                      expiration : expiration};
    ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'CLIENT_COMMIT',
                         net, data, auth, hres, next);
    var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
                m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPER -------------------------------------------------------------------

// NOTE USED by ZHB.create_adh_field/uuid_oplog()
exports.CreateClientDelta = function(net, ks, store, commit, remove, expire,
                                     jdata, ccrdt, oplog, expiration,
                                     sep, locex, auth, next) {
  create_client_delta(net, ks, store, commit, remove, expire,
                      jdata, ccrdt, oplog, expiration,
                      sep, locex, auth, next);
}

// NOTE: Used by ZAio.SendCentralAgentDentries()
exports.CleanupAgentDeltaBeforeSend = function(dentry) {
  cleanup_agent_delta_before_send(dentry)
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZDelt']={} : exports);

