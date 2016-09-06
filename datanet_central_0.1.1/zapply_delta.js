"use strict";

var ZMerge, ZConv, ZXact, ZISL, ZGC, ZCLS, ZDS, ZMDC, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZMerge = require('./zmerge');
  ZConv  = require('./zconvert');
  ZXact  = require('./zxaction');
  ZISL   = require('./zisolated');
  ZGC    = require('./zgc');
  ZCLS   = require('./zcluster');
  ZDS    = require('./zdatastore');
  ZMDC   = require('./zmemory_data_cache');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function create_crdt_from_initial_delta(ks, delta) {
  var json    = {}; // Empty JSON
  var crdtd   = ZConv.ConvertJsonObjectToCrdt(json, delta._meta, false);
  var crdt    = ZH.FormatCrdtForStorage(ks.key, delta._meta, crdtd);
  var merge   = ZMerge.ApplyDeltas(crdt, delta, false);
  var ncrdtd  = merge.crdtd;
  var meta    = ZH.clone(delta._meta);
  var ncrdt   = ZH.FormatCrdtForStorage(ks.key, meta, ncrdtd);
  return ncrdt;
}

function assign_post_merge_deltas(pc, merge) {
  if (merge) pc.post_merge_deltas = ZH.clone(merge.post_merge_deltas);
  else       pc.post_merge_deltas = [];
  if (pc.metadata.added_channels) {
    var entry = {added_channels   : pc.metadata.added_channels};
    pc.post_merge_deltas.push(entry);
  }
  if (pc.metadata.removed_channels) {
    var entry = {removed_channels : pc.metadata.removed_channels};
    pc.post_merge_deltas.push(entry);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HELPERS -----------------------------------------------------------

//TODO if (isn) {store meta.document_creation
//               may be in pc.xcrdt, pc.ncrdt, pc.dentry -> SPAGHETTI}
function set_channel_metadata(plugin, collections,
                              ks, nbytes, rchanid, mod, isn, next) {
  var ckey = ZS.GetChannelToKeys(rchanid);
  if (isn) {
    var kmod = {modification : mod,
                num_bytes    : nbytes};
    plugin.do_set_field(collections.global_coll, ckey, ks.kqk, kmod, next);
  } else {
    plugin.do_unset_field(collections.global_coll, ckey, ks.kqk, next);
  }
}

function update_chans(plugin, collections, ks, nbytes, mod, rchans, isn, next) {
  if (rchans.length === 0) next(null, 0);
  else {
    var rchan = rchans.shift();
    set_channel_metadata(plugin, collections, ks, nbytes, rchan, mod, isn,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        setImmediate(update_chans, plugin, collections,
                     ks, nbytes, mod, rchans, isn, next);
      }
    });
  }
}

function set_key_metadata(plugin, collections, ks, nbytes, mod, isn, next) {
  ZH.l('ZAD.set_key_metadata: K: ' + ks.kqk);
  var kkey = ZS.GetKeyModification(ks.kqk);
  var kmod = {modification : mod,
              num_bytes    : nbytes};
  if (isn) plugin.do_set(collections.global_coll, kkey, kmod, next);
  else     plugin.do_remove(collections.global_coll, kkey, next);
}

function update_delta_metadata(plugin, collections,
                               pc, nbytes, mod, rchans, isn, next) {
  if (ZH.AmStorage) { // ZH.AmBoth ALSO
    set_key_metadata(plugin, collections, pc.ks, nbytes, mod, isn,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        update_chans(plugin, collections,
                     pc.ks, nbytes, mod, rchans, isn, next);
      }
    });
  } else {
    if (pc.rchan_match) next(null, null);
    else {
      update_chans(plugin, collections, pc.ks, nbytes, mod, rchans, isn, next);
    }
  }
}

function remove_central_key_metadata(net, pc, next) {
  var orchans = pc.orchans;
  if (pc.ncrdt) {
     var rchans = ZH.clone(orchans);
     update_delta_metadata(net.plugin, net.collections,
                           pc, 0, 0, rchans, false, next);
  } else {
    ZMDC.GetKeyRepChans(net.plugin, net.collections, pc.ks,
    function(ferr, orchans) {
      if      (ferr)     next(ferr, null);
      else if (!orchans) next(null, null);
      else {
        var rchans = ZH.clone(orchans);
        update_delta_metadata(net.plugin, net.collections,
                              pc, 0, 0, rchans, false, next);
      }
    });
  }
}

function remove_central_key(net, pc, next) {
  var ks = pc.ks;
  ZH.l('remove_central_key: K: ' + ks.kqk);
  remove_central_key_metadata(net, pc, function(rerr, rres) {
    if (rerr) next(rerr, rres);
    else      ZDS.RemoveKey(net, pc.ks, pc.separate, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE CRDT/GCV ------------------------------------------------------------

function set_agent_max_gc_version(net, ks, gcv, next) {
  ZH.l('SET MAX-GCV: K: ' + ks.kqk + ' MAX-GCV: ' + gcv);
  var mkey = ZS.GetAgentKeyMaxGCVersion(ks.kqk);
  net.plugin.do_set_field(net.collections.key_coll,
                          mkey, "gc_version", gcv, next);
}

function set_agent_and_crdt_gc_version(net, ks, ncrdt, gcv, xgcv, next) {
  ZH.l('set_agent_and_crdt_gc_version: K: ' + ks.kqk + ' SET-GCV: ' + gcv);
  ZMDC.SetGCVersion(net.plugin, net.collections, ks, gcv, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      if (xgcv) {
        ncrdt._meta.GC_version = xgcv; // STORE XGCV (MAX-GCV) for FETCH()
        set_agent_max_gc_version(net, ks, xgcv, next);
      } else {
        var mkey = ZS.GetAgentKeyMaxGCVersion(ks.kqk);
        net.plugin.do_get_field(net.collections.key_coll, mkey, "gc_version",
        function(gerr, pmgcv) {
          if (gerr) next(gerr, null);
          else {
            var mgcv = pmgcv ? pmgcv : 0;
            if (gcv <= mgcv) {
              ncrdt._meta.GC_version = mgcv; // STORE MAX-GCV for FETCH()
              next(null, null);
            } else {
              ncrdt._meta.GC_version = gcv; // STORE (NEW)MAX-GCV for FETCH()
              set_agent_max_gc_version(net, ks, gcv, next);
            }
          }
        });
      }
    }
  });
}

exports.StoreCrdtAndGCVersion = function(net, ks, ncrdt, gcv, xgcv, next) {
  ZH.l('ZAD.StoreCrdtAndGCVersion: K: ' + ks.kqk);
  set_agent_and_crdt_gc_version(net, ks, ncrdt, gcv, xgcv,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else      ZDS.StoreCrdt(net, ks, ncrdt, next);
  });
}

function conditional_remove_key(net, ks, oexists, next) {
  if (!oexists) next(null, null);
  else          ZDS.RemoveKey(net, ks, false, next);
}

exports.RemoveCrdtStoreGCVersion = function(net, ks, crdt, gcv, oexists, next) {
  ZH.l('ZAD.RemoveCrdtStoreGCVersion: K: ' + ks.kqk);
  ZDS.StoreLastRemoveDelta(net.plugin, net.collections, ks, crdt._meta,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      conditional_remove_key(net, ks, oexists, function(rerr, rres) {
        if (rerr) next(rerr, null);
        else {
          var ncrdt = {_meta: {}}; // NOTE: DUMMY CRDT
          var xgcv  = 0;           // NOTE: DUMMY XGCV
          set_agent_and_crdt_gc_version(net, ks, ncrdt, gcv, xgcv, next);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// APPLY SUBSCRIBER-DELTA ----------------------------------------------------

function run_apply_remove_delta(net, pc, next) {
  var ks = pc.ks;
  if (ZH.AmCentral) remove_central_key(net, pc, next);
  else              ZDS.RemoveKey(net, ks, pc.separate, next);
}

// NOTE calls "next(null, CRDT)"
function store_remove_delta_metadata(net, pc, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry
  var gcv    = pc.extra_data.md.gcv;
  var meta   = dentry.delta._meta;
  ZH.l('store_remove_delta_metadata: K: ' + ks.kqk + ' SET-GCV: ' + gcv);
  ZDS.StoreLastRemoveDelta(net.plugin, net.collections, ks, meta,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var ncrdt = {_meta: {}}; // NOTE: DUMMY CRDT
      var xgcv  = 0;           // NOTE: DUMMY XGCV
      set_agent_and_crdt_gc_version(net, ks, ncrdt, gcv, xgcv,
      function(uerr, ures) {
        next(uerr, null); // NOTE: CRDT is NULL (REMOVE)
      });
    }
  });
}

// NOTE calls "next(null, CRDT)"
function do_apply_remove_delta(net, pc, next) {
  var md = pc.extra_data.md;
  ZH.SetNewCrdt(pc, md, undefined, false);
  run_apply_remove_delta(net, pc, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else      store_remove_delta_metadata(net, pc, next);
  });
}

// NOTE calls "next(null, CRDT)"
function do_apply_expire_delta(net, pc, next) {
  var dentry             = pc.dentry
  var md                 = pc.extra_data.md;
  var expiration         = dentry.delta._meta.expiration;
  var ncrdt              = ZH.clone(md.ocrdt);
  ncrdt._meta.author     = dentry.delta._meta.author; // ADD DELTA's AUTHOR
  ncrdt._meta.expiration = expiration;
  next(null, ncrdt);
}

// NOTE calls "next(null, CRDT)"
function do_store_initial_crdt(net, pc, delta, next) {
  var ks   = pc.ks;
  ZH.l('apply_delta: STORE CRDT INITIAL DELTA: K: ' + ks.kqk);
  var lkey = ZS.GetLastRemoveDelta(ks);
  net.plugin.do_remove(net.collections.ldr_coll, lkey, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      pc.store  = true;
      var ncrdt = create_crdt_from_initial_delta(ks, delta);
      next(null, ncrdt);
    }
  });
}

// NOTE calls "next(null, CRDT)"
function store_initial_crdt(net, pc, delta, next) {
  var ks   = pc.ks;
  var lkey = ZS.GetLastRemoveDelta(ks);
  net.plugin.do_get_field(net.collections.ldr_coll, lkey, "meta",
  function(gerr, rmeta) {
    if (gerr) next(gerr, null);
    else {
      if (!rmeta) { // NO LDR
        do_store_initial_crdt(net, pc, delta, next);
      } else {      // LDR EXISTS -> LWW on DOC-CREATION[@]
        var rcreated = rmeta.document_creation["@"];
        var dcreated = delta._meta.document_creation["@"];
        ZH.l('store_initial: rcreated: ' + rcreated + ' dcreated: ' + dcreated);
        if        (rcreated > dcreated) {      // LWW: LDR WINS
          next(new Error(ZS.Errors.DeltaNotSync), null);
        } else if (rcreated < dcreated) {      // LWW: DELTA WINS
          do_store_initial_crdt(net, pc, delta, next);
        } else { /* (ocreated === dcreated) */ // LWW: TIEBRAKER
          var r_uuid = rmeta.document_creation["_"];
          var d_uuid = delta._meta.document_creation["_"];
          ZH.l('store_initial: r_uuid: ' + r_uuid + ' d_uuid: ' + d_uuid);
          if (r_uuid > d_uuid) {
            next(new Error(ZS.Errors.DeltaNotSync), null);
          } else {
            do_store_initial_crdt(net, pc, delta, next);
          }
        }
      }
    }
  });
}

// NOTE calls "next(null, CRDT)"
function store_nondelta_crdt(net, pc, delta, next) {
  // NOTE: EITHER STORE OR REMOVE
  var remove = delta._meta.remove;
  if (remove) do_apply_remove_delta(net, pc, next);
  else        store_initial_crdt(net, pc, delta, next);
}

// NOTE calls "next(null, CRDT)"
function do_apply_GC_delta(net, pc, dentry, next) {
  var ks    = pc.ks;
  var avrsn = dentry.delta._meta.author.agent_version;
  var gcv   = dentry.delta._meta.GC_version;
  ZH.l('do_apply_GC_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  pc.DO_GC  = true; // Used in ZRAD.send_microservice_event()
  ZGC.GarbageCollect(net, pc, dentry, next);
}

// NOTE calls "next(null, CRDT)"
function do_apply_REORDER_delta(net, pc, dentry, is_agent, next) {
  var ks        = pc.ks;
  var avrsn     = dentry.delta._meta.author.agent_version;
  ZH.l('do_apply_REORDER_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  pc.DO_REORDER = true; // Used in ZRAD.send_microservice_event()
  delete(dentry.delta._meta.REORDERED); // STOP RECURSION
  do_apply_delta(net, pc, dentry, is_agent, true, function(aerr, acrdt) {
    if (aerr) next(aerr, null);
    else      ZGC.Reorder(net, ks, acrdt, dentry, next);
  });
}

function no_op_apply_delta(pc, dentry) {
  var md             = pc.extra_data.md;
  var ncrdt          = ZH.clone(md.ocrdt);
  ncrdt._meta.author = dentry.delta._meta.author; // ADD DELTA's AUTHOR
  return ncrdt;
}

// NOTE calls "next(null, CRDT)"
function do_apply_REAP_delta(net, pc, dentry, next) {
  var ks    = pc.ks;
  var ncrdt = no_op_apply_delta(pc, dentry);
  if (!ZH.AmCentral) { // NO-OP @AGENT
    ZH.l('do_apply_REAP_delta: K: ' + ks.kqk + ' AGENT -> NO-OP');
    next(null, ncrdt);
  } else {
    if (ZCLS.AmPrimaryDataCenter()) { // NOTE: Primary applies on COMMIT
      ZH.l('do_apply_REAP_delta: K: ' + ks.kqk + ' PRIMARY -> NO-OP');
      next(null, ncrdt);
    } else {
      var rgcv = dentry.delta._meta.reap_gc_version;;
      ZH.l('do_apply_REAP_delta: K: ' + ks.kqk + ' (R)GCV: ' + rgcv);
      ZGC.RemoveCentralGCVSummary(net, ks, rgcv, function(serr, sres) {
        next(null, ncrdt);
      });
    }
  }
}

// NOTE calls "next(null, CRDT)"
function do_apply_IGNORE_delta(net, pc, dentry, next) {
  var ks    = pc.ks;
  var ncrdt = no_op_apply_delta(pc, dentry);
  ZH.l('do_apply_IGNORE_delta: K: ' + ks.kqk + ' -> NO-OP');
  next(null, ncrdt);
}

// NOTE calls "next(null, CRDT)"
function do_apply_delta(net, pc, dentry, is_agent, is_reo, next) {
  var md        = pc.extra_data.md;
  var delta     = dentry.delta;
  var initial   = delta._meta.initial_delta;
  var remove    = delta._meta.remove;
  var expire    = delta._meta.expire;
  var do_gc     = delta._meta.DO_GC;
  var reod      = delta._meta.REORDERED;
  var do_ignore = delta._meta.DO_IGNORE;
  var do_reap   = delta._meta.DO_REAP;
  ZH.l('do_apply_delta: do_gc: ' + do_gc + ' reod: ' + reod +
       ' do_ignore: ' + do_ignore + ' do_reap: ' + do_reap);
  var do_other  = do_gc || reod || do_reap || do_ignore;
  if (!md.ocrdt) { // TOP-LEVEL: Create new CRDT (from Delta)
    if (do_other) { // REMOVE-KEY outraced a DO_* DELTA -> NO-OP
      next(null, null); // NOTE: CRDT is NULL (REMOVE)
    } else if (!initial && !remove) {
      // NOT-SYNC (no OCRDT & DELTA not initial/remove)
      ZH.l('do_apply_delta: DELTA_NOT_SYNC: NO CRDT & DELTA NOT INITIAL');
      next(new Error(ZS.Errors.DeltaNotSync), null);
    } else {
      store_nondelta_crdt(net, pc, delta, next);
    }
  } else {      // TOP-LEVEL: Update existing CRDT
    var a_mismatch = ZMerge.CompareZDocAuthors(md.ocrdt._meta, delta._meta);
    if (a_mismatch) {   // DOC-CREATION VERSION MISmatch
      ZH.l('DeltaAuthorVersion & CrdtAuthVersion MISMATCH');
      if (!initial && !remove) {
        ZH.l('do_apply_delta: DELTA_NOT_SYNC: Delta NOT INITIAL NOR REMOVE');
        next(new Error(ZS.Errors.DeltaNotSync), null);
      } else { // LWW: OCRDT[@] vs DELTA[@]
        var ocreated = md.ocrdt._meta.document_creation["@"];
        var dcreated = delta._meta.document_creation["@"];
        if        (ocreated > dcreated) {      // LWW: OCRDT WINS
          ZH.l('do_apply_delta: DELTA_NOT_SYNC: OCRDT NEWER');
          next(new Error(ZS.Errors.DeltaNotSync), null);
        } else if (ocreated < dcreated) {      // LWW: DELTA WINS
          store_nondelta_crdt(net, pc, delta, next);
        } else { /* (ocreated === dcreated) */ // LWW: TIEBRAKER
          var o_uuid = md.ocrdt._meta.document_creation._;
          var d_uuid = delta._meta.document_creation._;
          ZH.l('o_uuid: ' + o_uuid + ' d_uuid: ' + d_uuid);
          if (o_uuid > d_uuid) {
            ZH.l('do_apply_delta: DELTA_NOT_SYNC: Same[@]: OCRDT[_] HIGHER');
            next(new Error(ZS.Errors.DeltaNotSync), null);
          } else {
            store_nondelta_crdt(net, pc, delta, next);
          }
        }
      }
    } else {            // DOC-CREATION VERSION MATCH
      if (do_ignore) {
        do_apply_IGNORE_delta(net, pc, dentry, next);
      } else if (do_gc) {
        do_apply_GC_delta(net, pc, dentry, next);
      } else if (reod) {
        do_apply_REORDER_delta(net, pc, dentry, is_agent, next);
      } else if (do_reap) {
        do_apply_REAP_delta(net, pc, dentry, next);
      } else {
        if (remove) {        // REMOVE KEY
          pc.remove = true;
          do_apply_remove_delta(net, pc, next)
        } else if (expire) { // EXPIRE KEY
          pc.expire = true;
          do_apply_expire_delta(net, pc, next)
        } else {
          var merge = ZMerge.ApplyDeltas(md.ocrdt, delta, is_reo);
          // NOTE: Only AgentDelta REJECTS on errors, CD & SD process non-errors
          if (is_agent && merge.errors.length !== 0) {
            var merr = merge.errors.join(", ");
            next(merr, null);
          } else {                   // NORMAL APPLY DELTA
            assign_post_merge_deltas(pc, merge);
            var ncrdtd = merge.crdtd;
            var meta   = ZH.clone(delta._meta);
            var ncrdt  = ZH.FormatCrdtForStorage(pc.ks.key, meta, ncrdtd);
            var ntmbs  = ZXact.CountTombstones(ncrdt._data);
            ZGC.CheckGarbageCollect(pc.ks, ncrdt, ntmbs, next);
          }
        }
      }
    }
  }
}

function apply_delta(net, pc, is_agent, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  var md     = pc.extra_data.md;
  ZH.l('apply_delta: ' + ZH.SummarizeDelta(ks.kqk, dentry));
  do_apply_delta(net, pc, dentry, is_agent, false, function(aerr, ncrdt) {
    if (aerr) next(aerr, null);
    else {
      if (ncrdt) {
        ZH.SetNewCrdt(pc, md, ncrdt, false);
        var nbytes = ZConv.CalculateCrdtBytes(pc.ncrdt);
        pc.ncrdt._meta.num_bytes = nbytes;
      }
      next(null, null);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT APPLY DELTA ---------------------------------------------------------

exports.AgentApplyDelta = function(net, pc, dentry, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  var avrsn  = dentry.delta._meta.author.agent_version;
  ZH.l('AgentApplyDelta: key: ' + ks.kqk + ' AV: ' + avrsn);
  apply_delta(net, pc, true, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var nbytes = pc.ncrdt ? pc.ncrdt._meta.num_bytes : 0;
      pc.pdentry.delta._meta.num_bytes = nbytes;
      next(null, null);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER APPLY DELTA ----------------------------------------------------

exports.ApplySubscriberDelta = function(net, pc, next) {
  apply_delta(net, pc, false, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL APPLY DELTA -------------------------------------------------------

exports.CentralApplyDelta = function(net, pc, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  var avrsn  = dentry.delta._meta.author.agent_version;
  ZH.l('CentralApplyDelta: key: ' + ks.kqk + ' AV: ' + avrsn);
  apply_delta(net, pc, false, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZRAD.old/new_channels_metadata()
//       which is called from ZNM.create_cluster_subscriber_DC_merge() &
//                            ZPub.router_update_key_channels()
exports.UpdateDeltaMetadata = function(plugin, collections,
                                       pc, nbytes, mod, rchans, isn, next) {
  update_delta_metadata(plugin, collections,
                        pc, nbytes, mod, rchans, isn, next);
}

// NOTE: Used by ZNM.do_store_DC_merge()
exports.RemoveCentralKey = function(net, pc, next) {
  remove_central_key(net, pc, next);
}

// NOTE: Used by ZSM.post_apply_subscriber_merge()
exports.AssignPostMergeDeltas = function(pc, merge) { 
  assign_post_merge_deltas(pc, merge);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZAD']={} : exports);

