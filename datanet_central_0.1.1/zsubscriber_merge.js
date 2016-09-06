"use strict";

var ZConv, ZMerge, ZAD, ZSD, ZAS, ZDelt, ZCache, ZADaem, ZCR, ZDack, ZOOR, ZGC;
var ZMDC, ZDS, ZFix, ZNotify, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZConv   = require('./zconvert');
  ZMerge  = require('./zmerge');
  ZAD     = require('./zapply_delta');
  ZSD     = require('./zsubscriber_delta');
  ZAS     = require('./zactivesync');
  ZDelt   = require('./zdeltas');
  ZCache  = require('./zcache');
  ZADaem  = require('./zagent_daemon');
  ZCR     = require('./zcreap');
  ZDack   = require('./zdack');
  ZOOR    = require('./zooo_replay');
  ZGC     = require('./zgc');
  ZMDC    = require('./zmemory_data_cache');
  ZDS     = require('./zdatastore');
  ZFix    = require('./zfixlog');
  ZNotify = require('./znotify');
  ZAF     = require('./zaflow');
  ZQ      = require('./zqueue');
  ZS      = require('./zshared');
  ZH      = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

var DeepDebugSubscriberMerge = true; //TODO FIXME HACK

function deep_debug_subscriber_merge(ncrdt) {
  if (DeepDebugSubscriberMerge) {
    ZH.l('DeepDebugSubscriberMerge');
    ZMerge.DeepDebugMergeCrdt(ncrdt._meta, ncrdt._data.V, false);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

exports.SetKeyInSync = function(net, ks, next) {
  var skey = ZS.AgentKeysToSync;
  net.plugin.do_unset_field(net.collections.global_coll, skey, ks.kqk,
  function(kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      var okey = ZS.AgentKeysOutOfSync;
      net.plugin.do_unset_field(net.collections.global_coll,
                                okey, ks.kqk, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EVICT-MERGE-RACE ----------------------------------------------------------

function handle_evict_merge_race(net, ks, is_cache, next) {
  if (is_cache) next(null, false);
  else {
    var md = {};
    ZSD.GetAgentSyncStatus(net, ks, md, function(gerr, gres) {
      if (gerr) next(gerr, null);
      else {
        if (md.to_sync_key || md.out_of_sync_key) next(null, false);
        else {
          var ekey = ZS.GetEvicted(ks);
          net.plugin.do_get_field(net.collections.global_coll, ekey, "value",
          function(werr, evicted) {
            if (werr) next(werr, null);
            else {
              if (!evicted) next(null, false);
              else {
                ZH.e('NO-OP: EVICT WINS OVER NEED-MERGE'); // EVICT-MERGE-RACE
                ZCache.AgentEvict(net, ks, true, {}, function(aerr, ares) {
                  next(aerr, true);
                });
              }
            }
          });
        }
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER MERGE ----------------------------------------------------------

var DebugMerge = true;

function notify_merge_case(ks, c) {
  if (!ZH.AmBrowser) {
    if (DebugMerge) {
      ZH.e('MERGE: K: ' + ks.kqk + ' CASE: ' + c);
      var debug = {subscriber_merge_case : c};
      ZNotify.DebugNotify(ks, debug);
    }
  }
}

function subscriber_merge_not_sync(net, pc, next) {
  ZH.l('subscriber_merge_not_sync: K: ' + pc.ks.kqk);
  ZAS.SetAgentSyncKeySignalAgentToSyncKeys(net, pc.ks, next);
}

function finalize_apply_subscriber_merge(net, pc, next) {
  ZAD.AssignPostMergeDeltas(pc, null);
  next(null, null);
  if (!pc.extra_data.is_cache) { // CACHE returns ZDOC to userland
    // NOTE: BELOW is ASYNC
    ZDelt.OnDataChange(net.plugin, net.collections, pc, true, false);
  }
}

function post_apply_subscriber_merge(net, pc, next) {
  var ks = pc.ks;
  exports.SetKeyInSync(net, ks, function(kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      ZADaem.GetAgentKeyDrain(net, ks, true, function(uerr, ures) {
        if (uerr) next(uerr, null);
        else      finalize_apply_subscriber_merge(net, pc, next);
      });
    }
  });
}

function store_subscriber_merge_crdt(net, pc, ncrdt, next) {
  var ks    = pc.ks;
  var xcrdt = pc.xcrdt;
  var md    = pc.extra_data.md;
  var xgcv  = ZH.GetCrdtGCVersion(xcrdt._meta);
  ZH.l('store_subscriber_merge_crdt: K: ' + ks.kqk);
  ZH.SetNewCrdt(pc, md, ncrdt, true);
  deep_debug_subscriber_merge(pc.ncrdt);
  ZAD.StoreCrdtAndGCVersion(net, ks, pc.ncrdt, md.gcv, xgcv, next);
}

function store_lww_ocrdt(net, pc, next) {
  var ks      = pc.ks;
  var md      = pc.extra_data.md;
  var oexists = md.ocrdt ? true : false;
  ZH.l('store_lww_ocrdt: K: ' + ks.kqk + ' O: ' + oexists);
  if (!oexists) { // NOTE: do NOT GCV-SET -> done by Reorder(Remove)Delta
    next(null, null);
  } else {
    var xcrdt = pc.xcrdt;
    var xgcv  = ZH.GetCrdtGCVersion(xcrdt._meta);
    ZH.SetNewCrdt(pc, md, md.ocrdt, true); // MERGE-WINNER(OCRDT)
    ZAD.StoreCrdtAndGCVersion(net, ks, pc.ncrdt, md.gcv, xgcv, next);
  }
}

function store_remove_subscriber_merge_crdt(net, pc, next) {
  pc.remove   = true;
  var ks      = pc.ks;
  var xcrdt   = pc.xcrdt;
  var md      = pc.extra_data.md;
  var xgcv    = ZH.GetCrdtGCVersion(xcrdt._meta);
  var oexists = md.ocrdt ? true : false;
  ZH.l('store_remove_subscriber_merge_crdt: K: ' + ks.kqk);
  ZAD.RemoveCrdtStoreGCVersion(net, ks, xcrdt, xgcv, oexists, next);
}

function store_lww_xcrdt(net, pc, next) {
  var ks     = pc.ks;
  var remove = pc.extra_data.remove;
  ZH.l('store_lww_xcrdt: K: ' + ks.kqk);
  if (remove) store_remove_subscriber_merge_crdt(net, pc, next);
  else        store_subscriber_merge_crdt(net, pc, pc.xcrdt, next);
}

function lww_xcrdt(net, pc, mcase, next) {
  var ks     = pc.ks;
  var remove = pc.extra_data.remove;
  var md     = pc.extra_data.md;
  ZH.l('lww_xcrdt: K: ' + ks.kqk);
  notify_merge_case(ks, mcase);
  if (!remove) ZH.SetNewCrdt(pc, md, pc.xcrdt, true); // MERGE-WINNER(XCRDT)
  store_lww_xcrdt(net, pc, next);
}

function lww_ocrdt(net, pc, mcase, next) {
  var ks = pc.ks;
  ZH.l('lww_ocrdt: K: ' + ks.kqk);
  notify_merge_case(ks, mcase);
  store_lww_ocrdt(net, pc, function(serr, sres) {
    if (serr) next(serr, null);
    else      ZOOR.PrepareLocalDeltasPostSubscriberMerge(net, pc, next);
  });
}

function do_normal_subscriber_merge(net, pc, next) {
  var ks     = pc.ks;
  var remove = pc.extra_data.remove;
  if (remove) lww_xcrdt(net, pc, 'K', next);
  else {
    ZH.l('SubMerge: NORMAL MERGE');
    notify_merge_case(ks, 'M');
    ZOOR.ReplayMergeDeltas(net, ks, pc, true, function(aerr, ares) {
      if (aerr) next(aerr, null);
      else      store_subscriber_merge_crdt(net, pc, pc.ncrdt, next);
    });
  }
}

function do_subscriber_merge(net, pc, next) {
  var ks     = pc.ks;
  var xcrdt  = pc.xcrdt;
  var md     = pc.extra_data.md;
  if (!md.ocrdt) { // TOP-LEVEL: NO LOCAL CRDT
    var lkey = ZS.GetLastRemoveDelta(ks);
    net.plugin.do_get_field(net.collections.ldr_coll, lkey, "meta",
    function(gerr, rmeta) {
      if (gerr) next(gerr, null);
      else {
        if (!rmeta) { // NO LDR -> NEVER EXISTED LOCALLY
          lww_xcrdt(net, pc, 'A', next);
        } else {      // LDR EXISTS -> REMOVED WHILE OFFLINE
          var r_mismatch = ZMerge.CompareZDocAuthors(xcrdt._meta, rmeta);
          if (r_mismatch) { // MISMATCH: LDR & XCRDT DOC-CREATION VERSION
            var rcreated = rmeta.document_creation["@"];
            var xcreated = xcrdt._meta.document_creation["@"];
            if (DebugMerge) ZH.e('rc: ' + rcreated + ' xc: ' + xcreated);
            // NOTE: case B is impossible, DrainDeltas runs before ToSyncKeys
            if        (rcreated > xcreated) {      // LWW: OCRDT wins
              ZH.l('SubMerge: DocMismatch & LDR[@] NEWER -> NO-OP');
              lww_ocrdt(net, pc, 'B', next);
            } else if (rcreated < xcreated) {      // LWW: XCRDT wins
              lww_xcrdt(net, pc, 'C', next);
            } else { /* (rcreated === xcreated) */ // LWW: TIEBRAKER
              var r_uuid = rmeta.author.agent_uuid;
              var x_uuid = xcrdt._meta.author.agent_uuid;
              if (DebugMerge) ZH.e('ru: ' + r_uuid + ' xu: ' + x_uuid);
              if (r_uuid > x_uuid) {
                ZH.l('SubMerge: DocMismatch & LDR[_] HIGHER -> NO-OP');
                lww_ocrdt(net, pc, 'D', next);
              } else {
                lww_xcrdt(net, pc, 'E', next);
              }
            }
          } else { // MATCH: LDR & XCRDT DOC-CREATION VERSION -> [_,#] match
            ZH.l('SubMerge: Same[@] -> NO-OP');
            lww_ocrdt(net, pc, 'F', next);
          }
        }
      }
    });
  } else {             // TOP-LEVEL: Local CRDT exists
    var a_mismatch = ZMerge.CompareZDocAuthors(xcrdt._meta, md.ocrdt._meta);
    if (a_mismatch) { // MISMATCH: OCRDT & XCRDT DOC-CREATION VERSION
      var ocreated = md.ocrdt._meta.document_creation["@"];
      var xcreated = xcrdt._meta.document_creation["@"];
      if (DebugMerge) ZH.e('oc: ' + ocreated + ' xc: ' + xcreated);
      if        (ocreated > xcreated) {      // LWW: OCRDT wins
        ZH.l('SubMerge: OCRDT[@] NEWER -> NO-OP');
        lww_ocrdt(net, pc, 'G', next);
      } else if (ocreated < xcreated) {      // LWW: XCRDT wins
        lww_xcrdt(net, pc, 'H', next);
      } else { /* (ocreated === xcreated) */ // LWW: TIEBRAKER
        var o_uuid = md.ocrdt._meta.document_creation._;
        var x_uuid = xcrdt._meta.document_creation._;
        if (DebugMerge) ZH.e('ou: ' + o_uuid + ' xu: ' + x_uuid);
        if (o_uuid > x_uuid) {
          ZH.l('SubMerge: Same[@] & OCRDT[_] HIGHER -> NO-OP');
          lww_ocrdt(net, pc, 'I', next);
        } else {
          lww_xcrdt(net, pc, 'J', next);
        }
      }
    } else {          // MATCH: OCRDT & XCRDT DOC-CREATION VERSION
      do_normal_subscriber_merge(net, pc, next);
    }
  }
}

function store_ether(net, pc, tos, next) {
  if (tos.length === 0) next(null, null);
  else {
    var ks     = pc.ks;
    var crdt   = pc.xcrdt;
    var dentry = tos.shift();
    var auuid  = dentry.delta._meta.author.agent_uuid;
    if (auuid === ZH.MyUUID) { // Do NOT store SELFIE
      setImmediate(store_ether, net, pc, tos, next);
    } else {
      ZSD.ConditionalPersistSubscriberDelta(net, ks, dentry,
      function(serr, sres){
        if (serr) next(serr, null);
        else      setImmediate(store_ether, net, pc, tos, next);
      });
    }
  }
}

function store_subscriber_merge_ether(net, pc, next) {
  var ether = pc.extra_data.ether;
  var tos   = [];
  for (var ekey in ether) {
    tos.push(ether[ekey]);
  }
  store_ether(net, pc, tos, next);
}

function remove_subscriber_versions(net, ks, auuids, next) {
  if (auuids.length === 0) next(null, null);
  else {
    var auuid = auuids.shift();
    ZH.l('remove_subscriber_versions: K: ' + ks.kqk + ' AU: ' + auuid);
    var skey  = ZS.GetDeviceSubscriberVersion(auuid);
    net.plugin.do_unset_field(net.collections.key_coll, skey, ks.kqk,
    function(rerr, rres) {
      if (rerr) next(rerr, null);
      else      setImmediate(remove_subscriber_versions, net, ks, auuids, next);
    });
  }
}

function reset_subscriber_AVs(net, ks, next) {
  var kkey = ZS.GetPerKeySubscriberVersion(ks.kqk);
  net.plugin.do_get(net.collections.key_coll, kkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var kauuids = [];
      if (gres.length !== 0) {
        var auuids = gres[0];
        delete(auuids._id);
        for (var auuid in auuids) kauuids.push(auuid);
      }
      remove_subscriber_versions(net, ks, kauuids, function(rerr, rres) {
        if (rerr) next(rerr, null);
        else {
          var kkey = ZS.GetPerKeySubscriberVersion(ks.kqk);
          net.plugin.do_remove(net.collections.key_coll, kkey, next);
        }
      });
    }
  });
}


function do_set_subscriber_AV(net, ks, auuid, cavrsn, next) {
  ZH.l('do_set_subscriber_AV: K: ' + ks.kqk + ' U: ' + auuid +
       ' (C)AV: ' + cavrsn);
  var skey   = ZS.GetDeviceSubscriberVersion(auuid);
  var sentry = {value : cavrsn, when : ZH.GetMsTime()};
  net.plugin.do_set_field(net.collections.key_coll, skey, ks.kqk, sentry,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var kkey = ZS.GetPerKeySubscriberVersion(ks.kqk);
      net.plugin.do_set_field(net.collections.key_coll,
                              kkey, auuid, cavrsn, next);
    }
  });
}

function set_subscriber_AVs(net, ks, cavs, next) {
  if (cavs.length === 0) next(null, null);
  else {
    var cav    = cavs.shift();
    var auuid  = cav.auuid;
    var cavrsn = cav.cavrsn;
    do_set_subscriber_AV(net, ks, auuid, cavrsn, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(set_subscriber_AVs, net, ks, cavs, next);
    });
  }
}

function set_subscriber_agent_versions(net, pc, next) {
  var ks      = pc.ks;
  var cavrsns = pc.extra_data.cavrsns;
  reset_subscriber_AVs(net, ks, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      var cavs = [];
      for (var auuid in cavrsns) {
        cavs.push({auuid : Number(auuid), cavrsn : cavrsns[auuid]});
      }
      set_subscriber_AVs(net, ks, cavs, next);
    }
  });
}

function set_key_metadata(net, pc, next) {
  var ks = pc.ks;
  if (!pc.xcrdt) next(null, null); // REMOVE
  else {
    var rchans = pc.xcrdt._meta.replication_channels;
    ZMDC.CasKeyRepChans(net.plugin, net.collections, ks, rchans,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        var kkey = ZS.GetKeyInfo(ks);
        net.plugin.do_set_field(net.collections.kinfo_coll,
                                kkey, "present", true,
        function(aerr, ares) {
          if (aerr) next(aerr, null);
          else {
            var created = pc.xcrdt._meta.created
            ZDack.SetAgentCreateds(net.plugin, net.collections, created, next);
          }
        });
      }
    });
  }
}

function do_apply_subscriber_merge(net, pc, next) {
  var ks       = pc.ks;
  ZH.l('do_apply_subscriber_merge: K: ' + ks.kqk);
  var xcrdt    = pc.xcrdt;
  var md       = pc.extra_data.md;
  var o_nbytes = md.ocrdt ? md.ocrdt._meta.num_bytes : 0;
ZH.l('do_apply_subscriber_merge (O)#B: ' + o_nbytes); ZH.p(md.ocrdt);
  store_subscriber_merge_ether(net, pc, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var gcsumms = pc.extra_data.gcsumms;
      ZGC.SaveGCVSummaries(net.plugin, net.collections, ks, gcsumms,
      function(perr, pres) {
       if (perr) next(perr, null);
       else {
          set_subscriber_agent_versions(net, pc, function(aerr, ares) {
            if (aerr) next(aerr, null);
            else {
              set_key_metadata(net, pc, function(berr, bres) {
                 if (berr) next(berr, null);
                 else {
                   ZGC.CancelAgentGCWait(net, ks, function(cerr, cres) {
                     if (cerr) next(cerr, null);
                     else {
                       pc.ncrdt = pc.xcrdt; // NOTE: START ZCR BOOK-KEEPING
                       ZCR.StoreNumBytes(net, pc, o_nbytes, false,
                       function(werr, wres) {
                         if (werr) next(werr, null);
                         else {
                           delete(pc.ncrdt); // NOTE: END ZCR BOOK-KEEPING
                           do_subscriber_merge(net, pc, function(merr, mres) {
                            if (merr) {
                              if (merr.message !== ZS.Errors.DeltaNotSync) {
                                next(merr, null);
                              } else {
                                subscriber_merge_not_sync(net, pc, next);
                              }
                            } else {
                              post_apply_subscriber_merge(net, pc, next);
                            }
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
      });
    }
  });
}

exports.RetrySubscriberMerge = function(plugin, collections, pc, rerr, next) {
  var ks  = pc.ks;
  var net = ZH.CreateNetPerRequest(ZH.Agent);
  ZH.l('ZSM.RetrySubscriberMerge: K: ' + ks.kqk);
  var internal = rerr ? false : true;
  do_apply_subscriber_merge(net, pc, function(serr, sres) {
    if (serr) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(null, null);
    }
  });
}

function add_fixlog_subscriber_merge(net, pc, next) {
  var ks   = pc.ks;
  var udid = ks.kqk;
  ZH.l('add_fixlog_subscriber_merge: K: ' + ks.kqk + ' UD: ' + udid);
  var ukey = ZS.FixLog;
  net.plugin.do_set_field(net.collections.global_coll, ukey, udid, pc, next);
}

function start_subscriber_merge(net, ks, xcrdt, cavrsns, gcsumms, ether,
                                remove, is_cache, md, next) {
  var sep   = md.sep;
  ZH.l('start_subscriber_merge: K: ' + ks.kqk + ' (C)GCV: ' + md.gcv);
  var op    = 'SubscriberMerge';
  var edata = {cavrsns : cavrsns, gcsumms  : gcsumms,  ether  : ether,
               remove  : remove,  is_cache : is_cache, md     : md};
  var pc    = ZH.InitPreCommitInfo(op, ks, null, xcrdt, edata);
  pc.separate = sep;
  if (!remove) ZH.CalculateRchanMatch(pc, md.ocrdt, xcrdt._meta);
  add_fixlog_subscriber_merge(net, pc, function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      var rfunc = exports.RetrySubscriberMerge;
      do_apply_subscriber_merge(net, pc, function(rerr, rres) {
        if (rerr) rfunc(net.plugin, net.collections, pc, rerr, next);
        else      ZFix.RemoveFromFixLog(net.plugin, net.collections, pc, next);
      });
    }
  });
}

function get_subscriber_merge_metadata(net, ks, next) {
  var md = {};
  ZSD.FetchLocalKeyMetadata(net, ks, md, function(ferr, fres) {
    if (ferr) next(ferr, null);
    else {
      md.is_merge = true;
      next(null, md);
    }
  });
}

function process_apply_subscriber_merge(net, qe, next) {
  var ks       = qe.ks;
  var data     = qe.data;
  var xcrdt    = data.xcrdt;
  var cavrsns  = data.cavrsns;
  var gcsumms  = data.gcsumms;
  var ether    = data.ether;
  var remove   = data.remove;
  var is_cache = data.is_cache;
  get_subscriber_merge_metadata(net, ks, function(ferr, md) {
    if (ferr) next(ferr, null);
    else {
      start_subscriber_merge(net, ks, xcrdt, cavrsns, gcsumms, ether,
                             remove, is_cache, md, next);
    }
  });
}

function apply_subscriber_merge(plugin, collections, qe, next) {
  var net      = qe.net;
  var ks       = qe.ks;
  var data     = qe.data;
  var is_cache = data.is_cache;
  handle_evict_merge_race(net, ks, is_cache, function(merr, evicted) {
    if (merr) next(merr, null);
    else {
      if (evicted) next(null, null);
      else         process_apply_subscriber_merge(net, qe, next);
    }
  });
}

exports.FlowSubscriberMerge = function(plugin, collections, qe, next) {
  var qnext = qe.next;
  var qhres = qe.hres;
  apply_subscriber_merge(plugin, collections, qe, function(aerr, ares) {
    qnext(aerr, qhres);
    next (null, null);
  });
}

exports.DoProcessSubscriberMerge = function(net, ks, xcrdt, cavrsns, gcsumms,
                                            ether, remove, is_cache,
                                            hres, next) {
  if (remove) {
    ZH.l('ZSM.DoProcessSubscriberMerge: K: ' + ks.kqk + ' -> REMOVE');
  } else {
    var meta = xcrdt._meta;
    ZH.l('ZSM.DoProcessSubscriberMerge: ' + ZH.SummarizeCrdt(ks.kqk, meta));
  }
  var data = {xcrdt    : xcrdt, cavrsns : cavrsns, gcsumms  : gcsumms,
              ether    : ether, remove  : remove,  is_cache : is_cache};
  var op = 'SUBSCRIBER_MERGE';
  if (is_cache) {
    var qe = {ks : ks, op : op, net : net, data : data,
              hres : hres, next : next};
    apply_subscriber_merge(net.plugin, net.collections, qe, next);
  } else {
    ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, op,
                         net, data, null, hres, next);
    var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
                m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}

exports.ProcessSubscriberMerge = function(net, ks, xcrdt, cavrsns, gcsumms,
                                          ether, remove, hres, next) {
  hres.ks      = ks;     // Used in response
  hres.crdt    = xcrdt;  // Used in response
  hres.remove  = remove; // Used in response
  var is_cache = false;
  exports.DoProcessSubscriberMerge(net, ks, xcrdt, cavrsns, gcsumms,
                                   ether, remove, is_cache, hres, next);
}

// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZSM']={} : exports);

