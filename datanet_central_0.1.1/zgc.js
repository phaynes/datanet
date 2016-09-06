"use strict";

var ZPub, ZRAD, ZMerge, ZDelt, ZXact, ZOOR, ZCLS, ZMDC, ZDS, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZPub   = require('./zpublisher');
  ZRAD   = require('./zremote_apply_delta');
  ZMerge = require('./zmerge');
  ZDelt  = require('./zdeltas');
  ZXact  = require('./zxaction');
  ZOOR   = require('./zooo_replay');
  ZCLS   = require('./zcluster');
  ZMDC   = require('./zmemory_data_cache');
  ZDS    = require('./zdatastore');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var TombstoneThreshold = 21; //TODO this should be between 50-1000

if (ZH.CentralReapTest || ZH.CentralDoGCAtFourTest) {
  TombstoneThreshold = 4;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GCV SUMMARIES -------------------------------------------------------------

exports.RemoveCentralGCVSummary = function(net, ks, gcv, next) {
  var gkey = ZS.GetGCVReapKeyHighwater(ks);
  ZH.l('GCVReapKeyHighwater: K: ' + ks.kqk + ' GCV: ' + gcv);
  net.plugin.do_set_field(net.collections.gc_coll, gkey, "value", gcv,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else      exports.RemoveGCVSummary(net, ks, gcv, next);
  });
}

exports.RemoveGCVSummary = function(net, ks, gcv, next) {
  var lkey = ZS.GetGCVSummary(ks.kqk, gcv);
  ZH.l('ZGC.RemoveGCVSummary: LKEY: ' + lkey);
  net.plugin.do_remove(net.collections.gcdata_coll, lkey, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      var vkey = ZS.GetGCVersionMap(ks.kqk);
      net.plugin.do_unset_field(net.collections.gc_coll, vkey, gcv, next);
    }
  });
}

function remove_gcv_summaries(net, ks, agcvrsns, next) {
  if (agcvrsns.length == 0) next(null, null);
  else {
    var gcv = agcvrsns.shift();
    exports.RemoveGCVSummary(net, ks, gcv, function(rerr, rres) {
      if (rerr) next(rerr, null);
      else      setImmediate(remove_gcv_summaries, net, ks, agcvrsns, next);
    });
  }
}

exports.RemoveGCVSummaries = function(net, ks, next) {
  ZH.l('ZGC.RemoveGCVSummaries: K: ' + ks.kqk);
  var vkey = ZS.GetGCVersionMap(ks.kqk);
  net.plugin.do_get(net.collections.gc_coll, vkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var gcvrsns  = gres[0];
        var agcvrsns = [];
        for (var gcv in gcvrsns) agcvrsns.push(gcv);
        remove_gcv_summaries(net, ks, agcvrsns, function(rerr, rres) {
          if (rerr) next(rerr, null);
          else {
            var vkey = ZS.GetGCVersionMap(ks.kqk);
            net.plugin.do_remove(net.collections.gc_coll, vkey, next);
          }
        });
      }
    }
  });
}

function save_gcv_summary(plugin, collections, ks, gcv, tentry, next) {
  ZH.l('save_gcv_summary: K: ' + ks.kqk + ' GCV: ' + gcv);
  var lkey   = ZS.GetGCVSummary(ks.kqk, gcv);
  tentry.gcv = gcv; // Used in ZMerge.forward_crdt_gc_version()
  plugin.do_set(collections.gcdata_coll, lkey, tentry, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var vkey = ZS.GetGCVersionMap(ks.kqk);
      plugin.do_set_field(collections.gc_coll, vkey, gcv, true, next);
    }
  });
}

function save_gcv_summaries(plugin, collections, ks, arr, next) {
  if (arr.length === 0) next(null, null);
  else {
    var entry  = arr.shift();
    var gcv    = entry.gcv;
    var tentry = entry.tentry;
    save_gcv_summary(plugin, collections, ks, gcv, tentry,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        setImmediate(save_gcv_summaries, plugin, collections, ks, arr, next);
      }
    });
  }
}

exports.SaveGCVSummaries = function(plugin, collections, ks, gcsumms, next) {
  ZH.l('SaveGCVSummaries: K: ' + ks.kqk);
  var arr = [];
  for (var gcv in gcsumms) {
    arr.push({gcv : Number(gcv), tentry : gcsumms[gcv]});
  }
  save_gcv_summaries(plugin, collections, ks, arr, next);
}

function get_gcv_summary(plugin, collections, ks, gcv, next) {
  var tkey = ZS.GetGCVSummary(ks.kqk, gcv);
  plugin.do_get(collections.gcdata_coll, tkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var gcsumm = gres[0];
        delete(gcsumm._id);
        next(null, gcsumm);
      }
    }
  });
}

function get_gcv_summaries(plugin, collections, ks, gcsumms, gcvs, next) {
  if (gcvs.length == 0) next(null, gcsumms)
  else {
    var gcv = gcvs.shift();
    get_gcv_summary(plugin, collections, ks, gcv, function(serr, gcsumm) {
      if (serr) next(serr, null);
      else {
        if (gcsumm) gcsumms[gcv] = gcsumm;
        setImmediate(get_gcv_summaries, plugin, collections,
                     ks, gcsumms, gcvs, next);
      }
    });
  }
}

function get_gcv_summary_range(plugin, collections, ks, bgcv, egcv, next) {
  ZH.l('get_gcv_summary_range: K: ' + ks.kqk + ' [' + bgcv + '-' + egcv + ']');
  var gcsumms = {};
  var gcvs    = [];
  for (var i = bgcv; i <= egcv; i++) { // NOTE: INCLUDING(egcv)
    gcvs.push(i);
  }
  get_gcv_summaries(plugin, collections, ks, gcsumms, gcvs, next);
}

function union_tombstone_summaries(dest, src) {
  for (var key in src) {
    var sp = src[key];
    var dp = dest[key];
    if (!dp) dest[key] = ZH.clone(sp);
    else {
      for (var skey in sp) {
        var sval = sp[skey];
        dp[skey] = ZH.clone(sval);
      }
    }
  }
}

function union_objects(dest, src) {
  for (var key in src) {
    dest[key] = src[key];
  }
}

function cmp_gcsumm_desc(a, b) {
  var a_gcv = a.gcv;
  var b_gcv = b.gcv;
  return (a_gcv === b_gcv) ? 0 : ((a_gcv > b_gcv) ? -1 : 1); // DESCENDING
}

exports.UnionGCVSummaries = function(gcsumms) {
  var agcsumms = [];
  for (var gcv in gcsumms) {
    agcsumms.push({gcv : Number(gcv), gcs : gcsumms[gcv]});
  }
  agcsumms.sort(cmp_gcsumm_desc); // DESENDING ORDER
  var stsumm = {};
  var slreo  = {};
  var sulreo = {};
  for (var i = 0; i < agcsumms.length; i++) {
    var gcs   = agcsumms[i].gcs;
    var tsumm = gcs.tombstone_summary;
    var lreo  = gcs.lhn_reorder;
    var ulreo = gcs.undo_lhn_reorder;
    union_tombstone_summaries(stsumm, tsumm); // EARLIEST GCV overwrites
    union_objects(slreo, lreo);
    union_objects(sulreo, ulreo);
  }
  var ugcsumms = {tombstone_summary : stsumm,
                  lhn_reorder       : slreo,
                  undo_lhn_reorder  : sulreo};
  return ugcsumms;
}

exports.GetGCVSummaryRangeCheck = function(plugin, collections,
                                           ks, bgcv, egcv, next) {
  get_gcv_summary_range(plugin, collections, ks, bgcv, egcv,
  function(gerr, gcsumms) {
    if (gerr) next(gerr, null);
    else {
      var range    = Number(egcv) - Number(bgcv);
      var ngcsumms = Object.keys(gcsumms).length;
      if (ngcsumms < range) {
        ZH.e('ERROR: ZGC.GetGCVSummaryRangeCheck: RANGE: ' + range +
             ' GOT: ' + ngcsumms);
        next(null, null);
      } else {
        next(null, gcsumms);
      }
    }
  });
}

exports.GetUnionedGCVSummary = function(plugin, collections,
                                        ks, bgcv, egcv, next) {
  exports.GetGCVSummaryRangeCheck(plugin, collections, ks, bgcv, egcv, 
  function(gerr, gcsumms) {
    if (gerr) next(gerr, null);
    else {
      if (!gcsumms) next(null, null);
      else {
        var ugcsumms = exports.UnionGCVSummaries(gcsumms);
        next(null, ugcsumms);
      }
    }
  });
}

exports.GetMinGcv = function(gcsumms) {
  var min_gcv = Number.MAX_VALUE;
  for (var sgcv in gcsumms) {
    var gcv = Number(sgcv);
    if (gcv < min_gcv) min_gcv = gcv;
  }
  return (min_gcv === Number.MAX_VALUE) ? 0 : min_gcv;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD REORDER TO GC-SUMMARIES -----------------------------------------------

function add_single_reorder_to_gc_summary(net, ks, reo, next) {
  if (reo.skip) next(null, null);
  else {
    var gcv = reo.gcv;
    ZH.l('add_single_reorder_to_gc_summary: K: ' + ks.kqk + ' GCV: ' + gcv);
    get_gcv_summary(net.plugin, net.collections, ks, gcv, function(serr, gcs) {
      if (serr) next(serr, null);
      else {
        if (!gcs) next(null, null);
        else {
          var rtsumm = reo.tombstone_summary;
          var rlreo  = reo.lhn_reorder;
          var rulreo = reo.undo_lhn_reorder;
          var gtsumm = gcs.tombstone_summary;
          var glreo  = gcs.lhn_reorder;
          var gulreo = gcs.undo_lhn_reorder;
          for (var pid in rtsumm) {
            if (!gtsumm[pid]) gtsumm[pid] = rtsumm[pid];
            else {
              var rtmbs = rtsumm[pid];
              var gtmbs = gtsumm[pid];
              for (var cid in rtmbs) {
                gtmbs[cid] = rtmbs[cid];
              }
            }
          }
          for (var rid in rlreo) {
            glreo[rid] = rlreo[rid];
          }
          for (var rid in rulreo) {
            gulreo[rid] = rulreo[rid];
          }
          save_gcv_summary(net.plugin, net.collections, ks, gcv, gcs, next);
        }
      }
    });
  }
}

function do_add_reorder_to_gc_summaries(net, ks, reorder, next) {
  if (reorder.length === 0) next(null, null);
  else {
    var reo = reorder.shift();
    add_single_reorder_to_gc_summary(net, ks, reo, function(aerr, ares) {
      if (aerr) next(aerr, null);
      else {
        setImmediate(do_add_reorder_to_gc_summaries, net, ks, reorder, next);
      }
    });
  }
}

exports.AddReorderToGCSummaries = function(net, ks, reorder, next) {
  ZH.l('ZGC.AddReorderToGCSummaries: K: ' + ks.kqk);
  var creorder = ZH.clone(reorder); // NOTE: gets modified
  do_add_reorder_to_gc_summaries(net, ks, creorder, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD OOO-GCW TO GC-SUMMARY -------------------------------------------------

function do_add_ooo_gcw_to_gc_summary(net, pc, gcv, rometa, reorder, next) {
  var ks = pc.ks;
  get_gcv_summary(net.plugin, net.collections, ks, gcv, function(gerr, gcs) {
    if (gerr) next(gerr, null);
    else {
      if (!gcs) next(null, null);
      else {
        if (!gcs.agent_reorder_deltas) gcs.agent_reorder_deltas = [];
        var ard = {rometa  : rometa,
                   reorder : reorder};
        gcs.agent_reorder_deltas.push(ard);
        save_gcv_summary(net.plugin, net.collections, ks, gcv, gcs, next);
      }
    }
  });
}

function apply_before_current_gcv(net, pc, breo, next) {
  var ks = pc.ks;
  ZH.e('apply_before_current_gcv: K: ' + ks.kqk + '  BREO[]'); ZH.e(breo);
  var md = pc.extra_data.md;
  if (!pc.ncrdt) ZH.SetNewCrdt(pc, md, md.ocrdt, false);
  var cbreo = ZH.clone(breo); // NOTE: gets modified
  backward_reorders(net, ks, pc.ncrdt, cbreo, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var cbreo = ZH.clone(breo); // NOTE: gets modified
      forward_reorders(net, ks, pc.ncrdt, cbreo, next);
    }
  });
}

// IF Reorder[GCV] is BEFORE OGCV apply_reorder(forwards & backwards)
function handle_before_current_gcv(net, pc, reorder, ogcv, next) {
  var breo = [];
  for (var i = 0; i < reorder.length; i++) {
    var reo  = reorder[i];
    var rgcv = reo.gcv;
    if (rgcv <= ogcv) breo.push(reo);
  }
  if (breo.length == 0) next(null, null);
  else                  apply_before_current_gcv(net, pc, breo, next);
}

exports.AddOOOGCWToGCSummary = function(net, pc, gcv, rometa, reorder, next) {
  var ks   = pc.ks;
  ZH.e('ZGC.AddOOOGCWToGCSummary: K: ' + ks.kqk + ' GCV: ' + gcv);
  var md   = pc.extra_data.md;
  var ogcv = md.ogcv
  handle_before_current_gcv(net, pc, reorder, ogcv, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else      do_add_ooo_gcw_to_gc_summary(net, pc, gcv, rometa, reorder, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT GC-WAIT -------------------------------------------------------------

exports.FinishAgentGCWait = function(net, pc, bgcv, egcv, next) {
  var ks = pc.ks;
  ZH.e('FINISH-AGENT-GC-WAIT: K: ' + ks.kqk + ' [' + bgcv + '-' + egcv + ']');
  get_gcv_summary_range(net.plugin, net.collections, ks, bgcv, egcv,
  function(gerr, gcsumms) {
    if (gerr) next(gerr, null);
    else      ZMerge.ForwardCrdtGCVersion(net, pc, gcsumms, next);
  });
}

exports.CancelAgentGCWait = function(net, ks, next) {
  ZH.e('ZGC.CancelAgentGCWait: (GC-WAIT) K: ' + ks.kqk);
  var gkey = ZS.GetAgentKeyGCVNeedsReorder(ks);
  net.plugin.do_remove(net.collections.global_coll, gkey, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPER DO_GARBAGE_COLLECTION ----------------------------------------------

function get_array_id(cval) {
  return cval["_"] + '|' + cval["#"] + '|' + cval["@"];
}

function get_tombstone_id(cval) {
  return cval["_"] + '|' + cval["#"];
}

// NOTE: currently only arg[3] ("X") is being used
function get_lhn_description(cval) {
  // NOTE: FINAL ARG: DEFAULT to TOMBSTONE: ["X"] (not ANCHOR: "A")
  return [cval["S"], cval["<"]["_"], cval["<"]["#"], "X"];
}

function build_gcm_from_tombstone_summary(tsumm) {
  var gcm = {};
  for (var par in tsumm) {
    var tmbs = tsumm[par];
    for (var tid in tmbs) {
      gcm[tid] = tmbs[tid];
    }
  }
  return gcm;
}

function convert_tombstone_to_anchor(tsumm, crdtd, child) {
  var pid            = get_array_id(crdtd);
  var cid            = get_tombstone_id(child);
  tsumm[pid][cid][3] = "A";
}

function get_tombstone_type(tmb) {
  return tmb[3];
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DO_GARBAGE_COLLECTION------------------------------------------------------

function debug_delete_has_dangling_lhn(cval, child, i,
                                       rchild, gcm, lhn_reorder) {
  ZH.e('GC-WAIT-INCOMPLETE: CHILD'); ZH.e(child);
  ZH.e('HIT: ' + i); ZH.e('RCHILD'); ZH.e(rchild); ZH.e('CVAL'); ZH.e(cval);
  ZH.e('GCM'); ZH.e(gcm); ZH.e('LHNREORDER'); ZH.e(lhn_reorder);
}

function debug_do_gc_pre_lhn_reorder(child, lhnr) {
  ZH.l('PRE-REORDER: CHILD'); ZH.p(child);
  ZH.l('PRE-REORDER: LHNR');  ZH.p(lhnr);
}

function debug_do_gc_post_lhn_reorder(child) {
  ZH.l('POST-REORDER: child'); ZH.p(child);
}

function delete_has_dangling_lhn(cval, spot, gcm, lhn_reorder) {
  var child = cval[spot];
  var hit   = false;
  for (var i = spot + 1; i < cval.length; i++) { // Search RIGHT: matching LHN
    var rchild = cval[i];
    var rcid   = get_tombstone_id(rchild);
    var rtodel = (rchild["X"] && ZH.IsDefined(gcm[rcid]));
    var rlhn   = rchild["<"];
    var rroot  = ((rlhn["_"] === 0) && (rlhn["#"] === 0));
    if (rtodel || rroot) continue; // SKIP TO-DELETE & ORIGINAL-ROOTS
    if (child["_"] === rlhn["_"] && child["#"] === rlhn["#"]) {
      var rlhnr = lhn_reorder[rcid];
      if (!rlhnr) {
        hit = true;
        debug_delete_has_dangling_lhn(cval, child, i, rchild, gcm, lhn_reorder);
        break;
      }
    }
  }
  if (!hit) return false; // NO HIT -> OK
  return true;
}

function do_garbage_collection(crdtd, gcm, lhn_reorder) {
  var cval  = crdtd.V;
  var ctype = crdtd.T;
  if (ctype === "A") {
    var toremove  = [];
    var reordered = false;
    for (var i = 0; i < cval.length; i++) {
      var child = cval[i];
      var cid   = get_tombstone_id(child);
      var todel = (child["X"] && ZH.IsDefined(gcm[cid]));
      var lhnr  = lhn_reorder[cid];
      if (lhnr) {
        debug_do_gc_pre_lhn_reorder(child, lhnr);
        child["S"]      = lhnr[0];
        child["<"]["_"] = lhnr[1];
        child["<"]["#"] = lhnr[2];
        debug_do_gc_post_lhn_reorder(child);
        delete(lhn_reorder[cid]);
        reordered = true;
      }
      if (todel) {
        var tmb      = gcm[cid];
        var ttype    = get_tombstone_type(tmb);
        var isanchor = (ttype === "A");
        var check    = (!ZH.AmCentral && !isanchor);
        if (check && delete_has_dangling_lhn(cval, i, gcm, lhn_reorder)) {
          return false;
        }
        if (isanchor) child["A"] = true; // MARK AS ANCHOR
        else          toremove.push(i);  // REMOVE TOMBSTONE
        delete(gcm[cid]);
      }
      var ok = do_garbage_collection(child, gcm, lhn_reorder);
      if (!ok) return false;
    }
    if (toremove.length) {
      ZH.l('GC: removing: ' + toremove.length);
    }
    for (var i = toremove.length - 1; i >= 0; i--) {
      cval.splice(toremove[i], 1);
    }
    if (reordered) {
      ZMerge.DoArrayMerge(crdtd, cval, false);
    }
    for (var i = 0; i < cval.length; i++) { // ORIGINAL-S/L no longer needed
      var child = cval[i];
      delete(child["OS"]);
      delete(child["OL"]);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      var child = cval[ckey];
      var ok    = do_garbage_collection(child, gcm, lhn_reorder);
      if (!ok) return false;
    }
  }
  return true;
}

exports.DoGarbageCollection = function(pc, gcv, tsumm, lhnr) {
  var ks   = pc.ks;
  var md   = pc.extra_data.md;
  var crdt = md.ocrdt;
  ZH.l('ZGC.DoGarbageCollection: K: ' + ks.kqk + ' GCV: ' + gcv);
  var gcm  = build_gcm_from_tombstone_summary(tsumm);
  //NOTE: no ZDT.RunDatatypeFunctions() needed when removing tombstones
  var ok   = do_garbage_collection(crdt._data, gcm, lhnr);
  if (!ok) return false;
  md.gcv                = gcv; // NOTE: SET MD.GCV
  crdt._meta.GC_version = gcv; // Used in ZH.SetNewCrdt()
  ZMerge.DeepDebugMergeCrdt(crdt._meta, crdt._data.V, false);
  return true;
}

// NOTE calls "next(null, CRDT)"
function run_gc(net, pc, dentry, next) {
  var ks     = pc.ks;
  var md     = pc.extra_data.md;
  var crdt   = md.ocrdt;
  delete(dentry.delta._meta.DO_GC); // Dont rerun ZRAD.ProcessAutoDelta()
  crdt._meta = ZH.clone(dentry.delta._meta);
  var gcv    = crdt._meta.GC_version;
  var dgc    = ZH.clone(dentry.delta.gc); // NOTE: gets modified
  var tsumm  = dgc.tombstone_summary;
  var lhnr   = dgc.lhn_reorder;
  ZH.l('ZGC.GarbageCollect: K: ' + ks.kqk + ' SET-GCV: ' + gcv);
  var ok     = exports.DoGarbageCollection(pc, gcv, tsumm, lhnr);
  if (ok) next(null, md.ocrdt);
  else { // NOTE: ONLY HAPPENS ON AGENT
    ZOOR.SetGCWaitIncomplete(net, ks, function(serr, sres) {
      next(serr, null); // NULL CRDT -> GC-WAIT-INCOMPLETE -> DONT STORE CRDT
    });
  }
}

function debug_diff_dependencies(deps, mdeps, diffs) {
  ZH.l('diff_dependencies: DEPS: ');  ZH.p(deps);
  ZH.l('diff_dependencies: MDEPS: '); ZH.p(mdeps);
  ZH.l('diff_dependencies: DIFFS: '); ZH.p(diffs);
}

function diff_dependencies(net, ks, deps, next) {
  var diffs = {};
  ZDelt.GetAgentAgentVersions(net, ks, function(ferr, mdeps) {
    if (ferr) next(ferr, null);
    else {
      for (var suuid in mdeps) {
        var isuuid = Number(suuid);
        // DIFF ONLY AGENT-DELTAS (NOT from_central)
        if (isuuid <= ZH.MaxDataCenterUUID) continue;
        var mavrsn = mdeps[suuid];
        var davrsn = deps[suuid];
        var mavnum = ZH.GetAvnum(mavrsn);
        var davnum = ZH.GetAvnum(davrsn);
        if (mavnum > davnum) {
          diffs[suuid] = {mavrsn : mavrsn, davrsn : davrsn};
        }
      }
      debug_diff_dependencies(deps, mdeps, diffs);
      next(null, diffs);
    }
  });
}
 
// NOTE calls "next(null, CRDT)"
function analyze_agent_gc_wait(net, pc, dentry, diffs, next) {
  var ks = pc.ks;
  var md = pc.extra_data.md;
  ZOOR.AnalyzeKeyGCVNeedsReorderRange(net, ks, md.gcv, diffs,
  function(serr, ok) {
    if (serr) next(serr, null);
    else {
      if (ok) run_gc(net, pc, dentry, next);
      else    next(null, null); // NULL CRDT -> GC-WAIT -> DONT STORE CRDT
    }
  });
}

function agent_run_gc(net, pc, dentry, next) {
  var ks  = pc.ks;
  var gcv = ZH.GetDentryGCVersion(dentry);
  ZOOR.AgentInGCWait(net, ks, function(gerr, gwait) {
    if (gerr) next(gerr, null);
    else {
      ZH.l('GC: (GC-WAIT) GET-KEY-GCV-NREO: K: ' + ks.kqk + ' GW: ' + gwait);
      if (gwait) {
        ZH.e('DO_GC: K: ' + ks.kqk + ' DURING AGENT-GC-WAIT -> NO-OP');
        next(null, null); // NULL CRDT -> GC-WAIT -> DONT STORE CRDT
      } else {
        var deps = dentry.delta._meta.dependencies;
        diff_dependencies(net, ks, deps, function(serr, diffs) {
          if (serr) next(serr, null);
          else {
            var ok = (Object.keys(diffs).length === 0);
            ZH.l('agent_run_gc: ok: ' + ok);
            if (ok) run_gc(net, pc, dentry, next);
            else    analyze_agent_gc_wait(net, pc, dentry, diffs, next);
          }
        });
      }
    }
  });
}

// NOTE calls "next(null, CRDT)"
exports.GarbageCollect = function(net, pc, dentry, next) {
  var ks     = pc.ks;
  var gcv    = ZH.GetDentryGCVersion(dentry);
  var tentry = dentry.delta.gc;
  save_gcv_summary(net.plugin, net.collections, ks, gcv, tentry,
  function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      if (ZH.AmCentral) run_gc      (net, pc, dentry, next);
      else              agent_run_gc(net, pc, dentry, next);
    }
  });
}

// NOTE: SecondaryDataCenters should persist a flag [delta->DO_GC]
//       that gets removed when the DO_GC Delta is processed
//       |-> On Geo-election, new primary should redo any outstanding DO_GC
// NOTE calls "next(null, CRDT)"
function set_DO_GC(ks, crdt, next) {
  var ncrdt         = ZH.clone(crdt); // Not shared w/ Dentry.Meta
  ncrdt._meta.DO_GC = true;
  ZH.l('ZGC.CheckGarbageCollect: SET DO_GC: K: ' + ks.kqk);
  next(null, ncrdt);
}

// NOTE calls "next(null, CRDT)"
exports.CheckGarbageCollect = function(ks, crdt, ntmbs, next) {
  ZH.l('CheckGarbageCollect: ntmbs: ' + ntmbs);
  if (!ZH.AmCentral                  ||
      !ZCLS.AmPrimaryDataCenter()    ||
      ZH.ClusterNetworkPartitionMode ||
      ZH.ClusterVoteInProgress       ||
      ZH.GeoVoteInProgress) {
    next(null, crdt);
  } else {
    delete(crdt._meta.DO_GC);
    if (ntmbs <= TombstoneThreshold) next(null, crdt);
    else                             set_DO_GC(ks, crdt, next);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// UPDATE LEFT HAND NEIGHBOR -------------------------------------------------

exports.AgentPostMergeUpdateLhn = function(aentry, clm) {
  ZH.l('ZGC.AgentPostMergeUpdateLhn');
  var aval    = aentry.V;
  var op_path = aentry.op_path;
  var lkey    = ZMerge.GetLastKeyByName(clm, aval, op_path);
  if (!lkey) throw(new Error('PROGRAM ERROR(agent_post_merge_update_lhn)'));
  var lval    = aval;
  var hit     = -1;
  var s       = [aval["S"]];
  for (var i = (lkey - 1); i >= 0; i--) { // SEARCH BACKWARDS
    var cval = clm[i];
    if (lval["<"]["_"] === cval["_"] &&
        lval["<"]["#"] === cval["#"]) {   // Find LHN (CHAIN)
      hit      = i;
      var dhit = (cval["A"] || !cval["X"]);
      if (dhit) break; // DIRECT-HIT -> BREAK
      else {           // INDIRECT HIT (TOMBSTONE) -> CONTINUE
        s.unshift(cval["S"]);
        lval = cval;
      }
    }
  }
  if (hit === -1) {
    ZH.l('NO LHN FOUND: lkey: ' + lkey + ' -> NO-OP');
    return false;
  } else {
    var nlhn       = clm[hit];
    aval["S"]      = s;
    aval["<"]["_"] = nlhn["_"];
    aval["<"]["#"] = nlhn["#"];
    ZH.l('END: update_lhn: aval'); ZH.p(aval);
    return true;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ANCHORS -------------------------------------------------------------------

var ToBeGCState = {ANCHOR : 1,
                   CHILD  : 2};

function create_anchor(crdtd, tsumm, ochild, rchild,
                       lhn_reorder, undo_lhn_reorder, s) {
  ZH.l('CREATE_ANCHOR (ochild, rchild, s)');
  ZH.p(ochild); ZH.p(rchild); ZH.p(s);
  var rid               = get_tombstone_id(rchild);
  undo_lhn_reorder[rid] = [rchild["OS"], rchild["OL"]["_"], rchild["OL"]["#"]];
  lhn_reorder[rid]      = [s,            ochild["_"],       ochild["#"]];
  convert_tombstone_to_anchor(tsumm, crdtd, ochild);
}

// NOTE: create_anchor() may be called more than once on chains -> OK
function do_find_anchor(crdtd, tsumm, beg, spot, child, ochild,
                        lhn_reorder, undo_lhn_reorder, s) {
  var cval     = crdtd.V;
  var assigned = false;
  for (var j = (spot + 1); j < cval.length; j++) { // SEARCH RIGHT UNTIL END
    var rchild = cval[j];
    var rlhn   = rchild["<"];
    if (child["_"] === rlhn["_"] && child["#"] === rlhn["#"]) {
      s.push(rchild["S"]);
      if (rchild["Y"] === ToBeGCState.ANCHOR) {
        var hit = find_anchor_chain(crdtd, tsumm, beg, j, 
                                    lhn_reorder, undo_lhn_reorder, s);
        if (hit) assigned = true;
      } else {
        assigned = true;
        create_anchor(crdtd, tsumm, ochild, rchild,
                      lhn_reorder, undo_lhn_reorder, s);
      }
    }
  }
  return assigned;
}

function find_anchor_chain(crdtd, tsumm, beg, spot,
                           lhn_reorder, undo_lhn_reorder, s) {
  var cval   = crdtd.V;
  var ochild = cval[beg];
  var child  = cval[spot];
  child["Y"] = ToBeGCState.CHILD; // FIRST LINK IS ANCHOR
  return do_find_anchor(crdtd, tsumm, beg, spot, 
                        child, ochild, lhn_reorder, undo_lhn_reorder, s);
}

function assign_anchors(crdtd, tsumm, lhn_reorder, undo_lhn_reorder) {
  var cval     = crdtd.V;
  var assigned = false;
  for (var i = 0; i < cval.length; i++) {
    var child = cval[i];
    if (child["Y"] === ToBeGCState.ANCHOR) {
      var s   = [];
      var hit = find_anchor_chain(crdtd, tsumm, i, i,
                                  lhn_reorder, undo_lhn_reorder, s);
      if (hit) assigned = true;
    }
  }
  return assigned;
}

function remove_dangler(crdtd, tsumm, beg) {
  var cval  = crdtd.V;
  var child = cval[beg];
  ZH.l('DANGLING_ANCHOR: BEG: ' + beg); ZH.p(child);
  create_tombstone_summary(tsumm, crdtd, child);
}

function do_find_dangling_anchor(cval, tsumm, beg, spot, child) {
  for (var j = (spot + 1); j < cval.length; j++) { // SEARCH RIGHT UNTIL END
    var rchild = cval[j];
    var rlhn   = rchild["<"];
    if (child["_"] === rlhn["_"] && child["#"] === rlhn["#"]) {
      if (rchild["Y"]) { // NOTE: EITHER: [ANCHOR,CHILD] are OK
        var hit = find_dangling_anchor_chain(cval, tsumm, beg, j);
        if (hit) return true;
      } else {
        return true;
      }
    }
  }
  return false;
}

function find_dangling_anchor_chain(cval, tsumm, beg, spot) {
  var child = cval[spot];
  return do_find_dangling_anchor(cval, tsumm, beg, spot, child);
}

function remove_dangling_anchors(crdtd, tsumm) {
  var cval = crdtd.V;
  for (var i = 0; i < cval.length; i++) {
    var child = cval[i];
    if (child["A"]) {
      var hit = find_dangling_anchor_chain(cval, tsumm, i, i);
      if (!hit) {
        remove_dangler(crdtd, tsumm, i);
      }
    }
  }
}

function find_anchors(crdtd, tsumm, lhn_reorder, undo_lhn_reorder) {
  var cval  = crdtd.V;
  var ctype = crdtd.T;
  if (ctype === "A") {
    var has_tombstones = false;
    for (var i = 0; i < cval.length; i++) {
      var child = cval[i];
      if (child["X"]) {
        has_tombstones = true;
        child["Y"]     = ToBeGCState.ANCHOR;
      }
      find_anchors(child, tsumm, lhn_reorder, undo_lhn_reorder);
    }
    if (has_tombstones) {
      var hit = assign_anchors(crdtd, tsumm, lhn_reorder, undo_lhn_reorder);
      if (hit) {
        remove_dangling_anchors(crdtd, tsumm);
      }
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      var child = cval[ckey];
      find_anchors(child, tsumm, lhn_reorder, undo_lhn_reorder);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMPACT ANCHORS -----------------------------------------------------------

// NOTE: compacting anchors is a simple trick, that works because a
//       Post-GC CRDT has zero tombstones -> therefore immutable order
//       Sort the array and then compact S[] to 2 entries ->
//         1st entry is S[0],
//         2nd entry is the array index of the entry (switched to DESC)

function cmp_lhn_fs(lfs_a, lfs_b) {
  var fsa = lfs_a.fs;
  var fsb = lfs_b.fs;
  return (fsa === fsb) ? 0 : ((fsa > fsb) ? 1 : -1);
}

function compact_s_group(fs, sg, lhn_reorder) {
  ZH.l('compact_s_group'); ZH.p(sg);
  var slen = sg.length;
  for (var i = 0; i < slen; i++) {
    var rid             = sg[i];
    var order           = ((slen - i) - 1); // DESC (start at 0)
    var ns              = [fs, order];
    ZH.l('COMPACT: lhn_reorder: rid: ' + rid); ZH.p(ns);
    lhn_reorder[rid][0] = ns;
  }
}

function compact_lhn_reorder(lhn_reorder) {
  var lhn_fs = [];
  for (var rid in lhn_reorder) {
    var lhnr = lhn_reorder[rid];
    var s    = lhnr[0];
    var us   = ZMerge.UnrollS(s);
    var fs   = us[0];
    lhn_fs.push({fs : fs, rid : rid, s : s});
  }
  lhn_fs.sort(cmp_lhn_fs);
  for (var i = 0; i < lhn_fs.length; i++) {
    var lfs = lhn_fs[i];
    var s   = lfs.s;
    if (s.length > 1) { // S[] w/ more than 1 entry -> compacted to 2 entries
      var fs = lfs.fs;
      var sg = [lfs.rid];
      var j  = (i + 1);
      for (; j < lhn_fs.length; j++) { // SEARCH RIGHT
        var rlfs = lhn_fs[j];
        var frs  = rlfs.fs;
        if (fs !== frs) break;
        sg.push(rlfs.rid);
      }
      compact_s_group(fs, sg, lhn_reorder);
      i = (j - 1); // SKIP compacted
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// TRIM REORDER --------------------------------------------------------------

function debug_create_trim_reorder(desc, rid, lhn_reorder) {
  ZH.e('create_trim_reorder: ADD(' + desc + '): rid: ' + rid +
       ' lhn_reorder" ' + lhn_reorder[rid]);
}

function add_trim_reorder_entries(cval, desc, undo_lhn_reorder, lhn_reorder) {
  var rid               = get_tombstone_id(cval);
  undo_lhn_reorder[rid] = [cval["OS"], cval["OL"]["_"], cval["OL"]["#"]];
  lhn_reorder[rid]      = [cval["S"],  cval["<"]["_"],  cval["<"]["#"]];
  debug_create_trim_reorder(desc, rid, lhn_reorder);
}

function create_trim_reorder(clm, spot, undo_lhn_reorder, lhn_reorder) {
  var child = clm[spot];
  var crid  = get_tombstone_id(child);
  if (lhn_reorder[crid]) return; // TRIM-REORDER is LOWEST REORDER-PRIORITY
  var s     = child["S"];
  ZH.e('create_trim_reorder: CHILD'); ZH.e(child);
  var us    = ZMerge.UnrollS(s);
  var fs    = us[0];
  for (var i = spot - 1; i >= 0; i--) { // Search LEFT
    var cval = clm[i];
    var cs   = cval["S"];
    var cus  = ZMerge.UnrollS(cs);
    var fcs  = cus[0];
    if (fs !== fcs) break;
    add_trim_reorder_entries(cval, "LEFT", undo_lhn_reorder, lhn_reorder);
  }
  // ADD SELF
  add_trim_reorder_entries(child, "SELF", undo_lhn_reorder, lhn_reorder);
  for (var i = spot + 1; i < clm.length; i++) { // SEARCH RIGHT
    var cval = clm[i];
    var cs   = cval["S"];
    var cus  = ZMerge.UnrollS(cs);
    var fcs  = cus[0];
    if (fs !== fcs) break;
    add_trim_reorder_entries(cval, "RIGHT", undo_lhn_reorder, lhn_reorder);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE-CRDT DO GARBAGE COLLECTION ------------------------------------------

function is_head_of_array(child, clm, spot) {
  if (spot === 0) return true;
  for (var i = spot - 1; i >= 0; i--) { // get child's first NON-tombstoned LHN
    var lhn = clm[i];
    if (child["<"]["_"] === lhn["_"] && child["<"]["#"] === lhn["#"]) {
      if (lhn["X"]) child = lhn;
      else          return false; // LHN IS NOT A TOMBSTONE -> NOT HEAD
    }
  }
  return true;
}

function create_gc_reorder(clm, child, hit, s, undo_lhn_reorder, lhn_reorder) {
  var rhn               = clm[hit];
  ZH.l('PRE: create_gc_reorder'); ZH.p(rhn);
  var rid               = get_tombstone_id(rhn);
  // undo_lhn_reorder[] allows us to reverse LHN inheritance
  undo_lhn_reorder[rid] = [rhn["OS"], rhn["OL"]["_"],  rhn["OL"]["#"]];
  // lhn_reorder describes which lhns to reorder on DO_GC
  lhn_reorder[rid]      = [s,         child["<"]["_"], child["<"]["#"]];
  // RightHandNeighbor inherits Child's [S, LHN]
  rhn["S"]              = s;
  rhn["<"]["_"]         = child["<"]["_"];
  rhn["<"]["#"]         = child["<"]["#"];
  ZH.l('POST: create_gc_reorder'); ZH.p(rhn);
}

function find_gc_reorder_tree(clm, spot, s, undo_lhn_reorder, lhn_reorder) {
  var child = clm[spot];
  for (var i = spot + 1; i < clm.length; i++) { // SEARCH RIGHT
    var cval = clm[i];
    if (cval["<"]["_"] === child["_"] &&
        cval["<"]["#"] === child["#"]) { // Find LHN (CHAIN)
      var dhit = (cval["A"] || !cval["X"]);
      if (dhit) {
        var ns = s;
        ns.push(cval["S"]);
        create_gc_reorder(clm, child, i, ns, undo_lhn_reorder, lhn_reorder);
      } else {
        s.push(cval["S"]);
        find_gc_reorder_tree(clm, i, s,  undo_lhn_reorder, lhn_reorder);
        return; // NOTE: RECURSE
      }
    }
  }
}

function do_get_gc_reorder(clm, spot, undo_lhn_reorder, lhn_reorder) {
  var aval = clm[spot];
  var s    = [aval["S"]];
  find_gc_reorder_tree(clm, spot, s, undo_lhn_reorder, lhn_reorder);
}

function get_gc_reorder(clm, spot, undo_lhn_reorder, lhn_reorder) {
  var child = clm[spot];
  // NOTE: ZERO-ing out LHNs deletes positional metadata
  if (is_head_of_array(child, clm, spot)) return;
  do_get_gc_reorder(clm, spot, undo_lhn_reorder, lhn_reorder);
}

function get_garbage_collection(crdtd, undo_lhn_reorder, lhn_reorder) {
  var cval  = crdtd.V;
  var ctype = crdtd.T;
  if (ctype === "A") {
    // OS & OL used in create_gc_reorder() & & create_anchor()
    for (var i = 0; i < cval.length; i++) {
      var child   = cval[i];
      child["OS"] = ZH.clone(child["S"]);
      child["OL"] = ZH.clone(child["<"]);
    }
    for (var i = 0; i < cval.length; i++) {
      var child = cval[i];
      var s     = child["S"];
      var hit   = (child["X"] && !child["A"]); // TOMBSTONE & NOT ANCHOR
      var trim  = (s.length > 2);
      if (hit) {
        get_gc_reorder(cval, i, undo_lhn_reorder, lhn_reorder);
      } else if (trim) {
        create_trim_reorder(cval, i, undo_lhn_reorder, lhn_reorder);
      }
      get_garbage_collection(child, undo_lhn_reorder, lhn_reorder);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      var child = cval[ckey];
      get_garbage_collection(child, undo_lhn_reorder, lhn_reorder);
    }
  }
}

function create_tombstone_summary(tsumm, crdtd, child) {
  var pid  = get_array_id(crdtd)
  var cid  = get_tombstone_id(child)
  var desc = get_lhn_description(child);
  if (!tsumm[pid]) tsumm[pid] = {};
  tsumm[pid][cid] = desc;
}

function do_summarize_tombstones(crdtd, tsumm, is_oa) {
  var cval  = crdtd.V;
  var ctype = crdtd.T;
  if (ctype === "A") {
    var oa = ZH.IsOrderedArray(crdtd);
    for (var i = 0; i < cval.length; i++) {
      var child        = cval[i];
      var do_tombstone = child["X"] && !is_oa;
      if (do_tombstone) {
        create_tombstone_summary(tsumm, crdtd, child);
      }
      do_summarize_tombstones(child, tsumm, oa);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      var child = cval[ckey];
      do_summarize_tombstones(child, tsumm, false);
    }
  }
}

function summarize_tombstones(crdt) {
  var tsumm = {};
  do_summarize_tombstones(crdt._data, tsumm, false);
  return tsumm;
}

exports.DoGetGarbageCollection = function(gcv, crdt) {
  ZH.l('ZGC.DoGetGarbageCollection');
  var undo_lhn_reorder = {};
  var lhn_reorder      = {};
  get_garbage_collection(crdt._data, undo_lhn_reorder, lhn_reorder);
  var tsumm            = summarize_tombstones(crdt);
  var gcm              = build_gcm_from_tombstone_summary(tsumm);
  find_anchors(crdt._data, tsumm, lhn_reorder, undo_lhn_reorder);
  compact_lhn_reorder(lhn_reorder);
  var ntmbs            = Object.keys(gcm).length;
  var now              = ZH.GetMsTime();
  var gc               = {gcv               : gcv,
                          when              : now,
                          num_tombstones    : ntmbs,
                          tombstone_summary : ZH.clone(tsumm),
                          undo_lhn_reorder  : ZH.clone(undo_lhn_reorder),
                          lhn_reorder       : ZH.clone(lhn_reorder)};
  return gc;
}

// NOTE calls "next(null, CRDT)"
exports.SendGCdelta = function(net, ks, md, crdt, next) {
  if (!ZCLS.AmPrimaryDataCenter()) return next(null, null);
  md.ocrdt = ZH.clone(crdt); // ZRAD.ProcessAutoDelta() on THIS CRDT
  ZMDC.IncrementGCVersion(net.plugin, net.collections, ks, 1,
  function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var gcv       = Number(gres.gc_version);
      var new_count = 1; // DO_GC consists of ONE OP
      var cmeta     = crdt._meta;
      ZXact.CreateAutoDentry(net, ks, cmeta, new_count, false,
      function(aerr, dentry) {
        if (aerr) next(aerr, null);
        else {
          var meta        = dentry.delta._meta;
          meta.GC_version = gcv;
          meta.DO_GC      = true;
          dentry.delta.gc = exports.DoGetGarbageCollection(gcv, crdt);
          var avrsn       = meta.author.agent_version;
          ZH.e('GC-DELTA: K: ' + ks.kqk + ' AV: ' + avrsn + ' GCV: ' + gcv);
          var net         = ZH.CreateNetPerRequest(ZH.Central);
          ZRAD.ProcessAutoDelta(net, ks, md, dentry, next);
        }
      });
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REORDER DELTA -------------------------------------------------------------

function apply_backward_undo_lhn_reorder(crdtd, undo_lhn_reorder) {
  var cval  = crdtd.V;
  var ctype = crdtd.T;
  if (ctype === "A") {
    for (var i = 0; i < cval.length; i++) {
      var child = cval[i];
      ZMerge.CheckApplyReorderElement(child, undo_lhn_reorder);
      apply_backward_undo_lhn_reorder(child, undo_lhn_reorder);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      var child = cval[ckey];
      apply_backward_undo_lhn_reorder(child, undo_lhn_reorder);
    }
  }
}

function forward_reorder(net, ks, crdt, reo, next) {
  if (reo.skip) next(null, null);
  else {
    var gcv = reo.gcv;
    ZH.l('forward_reorder: K: ' + ks.kqk + ' GCV: ' + gcv);
    // NOTE: ReorderDelta can NOT FAIL due to GC-WAIT-INCOMPLETE
    do_garbage_collection(crdt._data, {}, reo.lhn_reorder);
    crdt._meta.GC_version = gcv; // Used in ZH.SetNewCrdt()
    next(null, null);
  }
}

function forward_reorders(net, ks, crdt, reorder, next) {
  if (reorder.length === 0) next(null, null);
  else {
    var reo = reorder.shift();
    forward_reorder(net, ks, crdt, reo, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(forward_reorders, net, ks, crdt, reorder, next);
    });
  }
}

function backward_reorder(net, ks, crdt, reo, next) {
  if (reo.skip) next(null, null);
  else {
    var gcv = reo.gcv;
    ZH.l('backward_reorder: K: ' + ks.kqk + ' GCV: ' + gcv);
    apply_backward_undo_lhn_reorder(crdt._data, reo.undo_lhn_reorder);
    next(null, null);
  }
}

function backward_reorders(net, ks, crdt, reorder, next) {
  if (reorder.length === 0) next(null, null);
  else {
    var reo = reorder.pop(); // NOTE: POP() START AT END
    backward_reorder(net, ks, crdt, reo, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(backward_reorders, net, ks, crdt, reorder, next);
    });
  }
}

// NOTE calls "next(null, CRDT)"
exports.Reorder = function(net, ks, crdt, dentry, next) {
  ZH.l('ZGC.Reorder: K: ' + ks.kqk);
  if (!crdt) next(null, null); // NOTE: CRDT is NULL (REMOVE)
  else {
    var gcv          = crdt._meta.GC_version;
    crdt._meta       = ZH.clone(dentry.delta._meta);
    var cmeta        = crdt._meta;
    cmeta.GC_version = gcv; // OCRDT GCV is correct
    var reorder      = ZH.clone(dentry.delta.reorder); // NOTE: gets modified
    backward_reorders(net, ks, crdt, reorder, function(serr, sres) {
      if (serr) next(serr, null);
      else {
        var reorder = ZH.clone(dentry.delta.reorder); // NOTE: gets modified
        forward_reorders(net, ks, crdt, reorder, function(aerr, ares) {
          if (aerr) next(aerr, null);
          else {
            ZMerge.DeepDebugMergeCrdt(crdt._meta, crdt._data.V, false);
            next(null, crdt);
          }
        });
      }
    });
  }
}

// NOTE: ReorderDelta DEPS[] ONLY for RUUID -> Prevent AGENT-GC-WAIT DEADLOCK
function trim_reorder_dependencies(ndentry, ruuid) {
  var meta = ndentry.delta._meta;
  var deps = meta.dependencies;
  for (var suuid in deps) {
    suuid = Number(suuid);
    if (suuid !== ruuid) delete(deps[suuid]);
  }
}

// NOTE calls "next(null, CRDT)"
exports.SendReorderDelta = function(net, ks, md, dgcs,
                                    ruuid, ravrsn, rgcv, next) {
  var new_count = 1;
  var cmeta     = md.ocrdt._meta;
  ZXact.CreateAutoDentry(net, ks, cmeta, new_count, false,
  function(aerr, ndentry) {
    if (aerr) next(aerr, null);
    else {
      var meta                  = ndentry.delta._meta;
      var avrsn                 = meta.author.agent_version;
      meta.GC_version           = md.gcv;
      meta.DO_REORDER           = true;
      meta.reference_uuid       = ruuid;
      meta.reference_version    = ravrsn;
      meta.reference_GC_version = rgcv;
      if (!dgcs) { // GCV was STALE -> IGNORE REFERENCE_DELTA
        ZH.l('SET REFERENCE_IGNORE: K: ' + ks.kqk + ' AV: ' + avrsn);
        meta.reference_ignore = true;
      } else {
        ndentry.delta.reorder = dgcs;
      }
      trim_reorder_dependencies(ndentry, ruuid);
      ZH.l('ZGC.SendReorderDelta: K: ' + ks.kqk + ' AV: ' + avrsn +
           ' GCV: ' + md.gcv);
      ZRAD.ProcessAutoDelta(net, ks, md, ndentry, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REAP DELTA ----------------------------------------------------------------

function get_reap_key_crdt(net, ks, next) {
  ZDS.ForceRetrieveCrdt(net.plugin, net.collections, ks, function(ferr, crdt) {
    if (ferr) next(ferr, null);
    else {
      if (crdt) next(null, crdt);
      else {
        var lkey = ZS.GetLastRemoveDelta(ks);
        net.plugin.do_get_field(net.collections.ldr_coll, lkey, "meta",
        function(gerr, cmeta) {
          if (gerr) next(gerr, null);
          else {
            var crdt = ZH.FormatCrdtForStorage(ks.key, cmeta, null);
            next(null, crdt);
          }
        });
      }
    }
  });
}

exports.SendReapDelta = function(net, ks, gcv, next) {
  var new_count = 1;
  get_reap_key_crdt(net, ks, function(gerr, crdt) {
    if (gerr) next(gerr, null);
    else {
      var rchans = crdt._meta.replication_channels;
      ZPub.GetSubs(net.plugin, net.collections, ks, rchans,
      function(ferr, subs) {
        if (ferr) next(ferr, null);
        else {
          var md = {ocrdt : crdt, subs : subs};
          ZXact.CreateAutoDentry(net, ks, crdt._meta, new_count, false,
          function(aerr, ndentry) {
            if (aerr) next(aerr, null);
            else {
              var meta             = ndentry.delta._meta;
              meta.DO_REAP         = true;
              meta.reap_gc_version = gcv;
              var avrsn            = meta.author.agent_version;
              ZH.l('ZGC.SendReapDelta: K: ' + ks.kqk + ' AV: ' + avrsn);
              ZRAD.ProcessAutoDelta(net, ks, md, ndentry, next);
            }
          });
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZMerge.update_lhn()
exports.GetTombstoneID = function(cval) {
  return get_tombstone_id(cval);
}

// NOTE: Used by ZMerge.freshen_central_delta/update_agent_delta_lhn,
//               ZMerge.DoRewindCrdtGCVersion
exports.GetArrayID = function(crdtd) {
  return get_array_id(crdtd);
}

// NOTE: Used by ZNM.get_geo_need_merge_body()
exports.GetGCVSummaryRange = function(plugin, collections,
                                      ks, bgcv, egcv, next) {
  get_gcv_summary_range(plugin, collections, ks, bgcv, egcv, next);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZGC']={} : exports);

