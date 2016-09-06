"use strict";

var ZRAD, ZGack, ZMerge, ZDelt, ZSD, ZAD, ZAS, ZGC, ZDS, ZMDC, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');;
  ZRAD   = require('./zremote_apply_delta');
  ZGack  = require('./zgack');
  ZMerge = require('./zmerge');
  ZDelt  = require('./zdeltas');
  ZSD    = require('./zsubscriber_delta');
  ZAD    = require('./zapply_delta');
  ZAS    = require('./zactivesync');
  ZGC    = require('./zgc');
  ZDS    = require('./zdatastore');
  ZMDC   = require('./zmemory_data_cache');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
} 

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

function debug_dentries_agent_versions(dentries) {
  var ret = '';
  for (var i = 0; i < dentries.length; i++)  {
    var dentry = dentries[i];
    var author = dentry.delta._meta.author;
    var avrsn  = author.agent_version;
    if (ret.length) ret += ", ";
    ret        += avrsn;
  }
  return ret;
}

function debug_replay_merge_dentries(ks, rdentries) {
  if (rdentries.length) {
    ZH.e('ZOOR.ReplayMergeDeltas: K: ' + ks.kqk + ' rdentries[AV]: [' +
         debug_dentries_agent_versions(rdentries) + ']');
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

var DebugCmpDeltaAgentVersion = false;

function cmp_delta_agent_version(avrsn1, avrsn2) {
  var res1   = avrsn1.split('|');
  var auuid1 = Number(res1[0]);
  var avnum1 = Number(res1[1]);
  var res2   = avrsn2.split('|');
  var auuid2 = Number(res2[0]);
  var avnum2 = Number(res2[1]);
  if (DebugCmpDeltaAgentVersion && (auuid1 === auuid2) && (avnum1 === avnum2)) {
    ZH.e('cmp_delta_agent_version: REPEAT AV: AV1: ' + avrsn1 +
         ' AV2: ' + avrsn2);
    process.exit(-1);
    return 0;
  }
  if (auuid1 !== auuid2) return (auuid1 > auuid2) ? 1 : -1;
  else { // (auuid1 === auuid2)
    return (avnum1 === avnum2) ? 0 : ((avnum1 > avnum2) ? 1 : -1);
  }
}

function create_reference_author(dentry) {
  var rfauuid  = dentry.delta._meta.reference_uuid;
  var rfavrsn  = dentry.delta._meta.reference_version;
  var rfauthor = {agent_uuid : rfauuid, agent_version : rfavrsn};
  return rfauthor;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GC-WAIT_INCOMPLETE --------------------------------------------------------

exports.SetGCWaitIncomplete = function(net, ks, next) {
  ZH.l('START: GC-WAIT_INCOMPLETE: K: ' + ks.kqk);
  var wkey = ZS.GetAgentKeyGCWaitIncomplete(ks);
  net.plugin.do_set_field(net.collections.global_coll,
                          wkey, "value", true, next);
}

exports.EndGCWaitIncomplete = function(net, ks, next) {
  ZH.l('END: GC-WAIT_INCOMPLETE: K: ' + ks.kqk);
  var wkey = ZS.GetAgentKeyGCWaitIncomplete(ks);
  net.plugin.do_remove(net.collections.global_coll, wkey, next);
}

exports.AgentInGCWait = function(net, ks, next) {
  var gkey = ZS.GetAgentKeyGCVNeedsReorder(ks);
  net.plugin.do_get(net.collections.global_coll, gkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var empty = (gres.length === 0);
      var gwait = !empty;
      ZH.l('REO: (GC-WAIT) K: ' + ks.kqk + ' GCW(NREO): ' + gwait);
      if (gwait) next(null, true);
      else {
        var wkey = ZS.GetAgentKeyGCWaitIncomplete(ks);
        net.plugin.do_get_field(net.collections.global_coll, wkey, "value",
        function(ferr, gcwi) {
          if (ferr) next(ferr, null);
          else {
            ZH.l('REO: (GC-WAIT) K: ' + ks.kqk + ' GCW(INCOMPLETE): ' + gcwi);
            if (gcwi) next(null, true); // POSSIBLE END-GC-WAIT-INCOMPLETE
            else      next(null, false);
          }
        });
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET/SET/DECREMENT KEY-GCV-NREO --------------------------------------------

function get_num_agent_key_gcv_needs_reorder(net, ks, gcv, next) {
  var gkey = ZS.GetAgentKeyGCVNeedsReorder(ks);
  net.plugin.do_get_field(net.collections.global_coll, gkey, gcv,
  function(gerr, avrsns) {
    if (gerr) next(gerr, null);
    else {
      var nreo = avrsns ? Object.keys(avrsns).length : 0;
      next(null, nreo);
    }
  });
}

function do_remove_agent_key_gcv_needs_reorder(net, ks, gcv, next) {
  ZH.l('REMOVE-KEY-GCV-NREO: (GC-WAIT): K: ' + ks.kqk + ' GCV: ' + gcv);
  var gkey = ZS.GetAgentKeyGCVNeedsReorder(ks);
  net.plugin.do_get(net.collections.global_coll, gkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var gavrsns = gres[0];
      delete(gavrsns._id);
      var empty   = true;
      for (var agcv in gavrsns) {
        var avrsns = gavrsns[agcv];
        var nreo   = Object.keys(avrsns).length;
        if (nreo > 0) {
          empty = false;
          break;
        }
      }
      ZH.l('do_remove_agent_key_gcv_needs_reorder: empty: ' + empty);
      var gkey = ZS.GetAgentKeyGCVNeedsReorder(ks);
      if (empty) {
        net.plugin.do_remove(net.collections.global_coll, gkey, next);
      } else {
        net.plugin.do_unset_field(net.collections.global_coll, gkey, gcv, next);
      }
    }
  });
}

function debug_remove_agent_version_agent_key_gcv_nreo(avrsn, avrsns, nreo) {
  ZH.l('REMOVE-AV-KGNREO: AV: ' + avrsn + ' NREO: ' + nreo + ' AVS[];');
  ZH.p(avrsns);
}

function remove_agent_version_agent_key_gcv_needs_reorder(net, ks, gcv, avrsn,
                                                          next) {
  var gkey = ZS.GetAgentKeyGCVNeedsReorder(ks);
  net.plugin.do_get_field(net.collections.global_coll, gkey, gcv,
  function(gerr, avrsns) {
    if (gerr) next(gerr, null);
    else {
      delete(avrsns[avrsn]); // REMOVE SINGLE AGENT-VERSION
      var nreo = Object.keys(avrsns).length;
      debug_remove_agent_version_agent_key_gcv_nreo(avrsn, avrsns, nreo);
      var gkey = ZS.GetAgentKeyGCVNeedsReorder(ks);
      net.plugin.do_set_field(net.collections.global_coll, gkey, gcv, avrsns,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          var dkey = ZS.GetAgentDeltaGCVNeedsReorder(ks);
          net.plugin.do_unset_field(net.collections.global_coll, dkey, avrsn,
          function(rerr, rres) {
            next(rerr, nreo);
          });
        }
      });
    }
  });
}

function decrement_agent_key_gcv_needs_reorder(net, ks, gcv, avrsn, next) {
  ZH.l('decrement_agent_key_gcv_needs_reorder: K: ' + ks.kqk + ' GCV: ' + gcv);
  get_num_agent_key_gcv_needs_reorder(net, ks, gcv, function(gerr, nreo) {
    if (gerr) next(gerr, null);
    else {
      ZH.l('REO: (GC-WAIT) GET-KEY-GCV-NREO: K: ' + ks.kqk + ' GCV: ' + gcv +
           ' N: ' + nreo);
      if (!nreo) next(null, false);
      else {
        remove_agent_version_agent_key_gcv_needs_reorder(net, ks, gcv, avrsn,
        function(serr, nnreo) {
          if (serr) next(serr, null);
          else {
            ZH.l('DECREMENT-KEY-GCV-NREO: (GC-WAIT) K: ' + ks.kqk +
                 ' GCV: ' + gcv + ' (N)NREO: ' + nnreo);
            if (nnreo) next(null, false);
            else {
              do_remove_agent_key_gcv_needs_reorder(net, ks, gcv,
              function(rerr, rres) {
                next(rerr, true);
              });
            }
          }
        });
      }
    }
  });
}

function get_per_gcv_number_key_gcv_needs_reorder(ks, xgcv, dentries) {
  var gavrsns = {}
  for (var i = 0; i < dentries.length; i++) {
    var dentry    = dentries[i];
    var dgcv      = ZH.GetDentryGCVersion(dentry);
    var meta      = dentry.delta._meta;
    var avrsn     = meta.author.agent_version;
    var fcentral  = meta.from_central;
    var gmismatch = (xgcv !== dgcv);
    var do_ignore = meta.DO_IGNORE;
    // (AGENT-DELTA) AND (DELTA(GCV) mismatch XCRDT) AND (NOT AN IGNORE)
    if (!fcentral && gmismatch && !do_ignore) {
      var gc_gcv = dgcv + 1; // NEXT GC_version will be GC-WAIT
      if (!gavrsns[gc_gcv]) gavrsns[gc_gcv] = {};
      gavrsns[gc_gcv][avrsn] = true;
    }
  }
  return gavrsns;
}

function get_total_number_key_gcv_needs_reorder(ks, xgcv, dentries) {
  var gavrsns = get_per_gcv_number_key_gcv_needs_reorder(ks, xgcv, dentries);
  var tot     = 0;
  for (var gcv in gavrsns) {
    var avrsns  = gavrsns[gcv];
    var nreo    = Object.keys(avrsns).length;
    tot        += nreo;
  }
  return tot;
}

function set_delta_gcv_needs_reorder(net, ks, aavrsns, next) {
  if (aavrsns.length === 0) next(null, null);
  else {
    var avrsn = aavrsns.shift();
    var dkey  = ZS.GetAgentDeltaGCVNeedsReorder(ks);
    net.plugin.do_set_field(net.collections.global_coll, dkey, avrsn, true,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        setImmediate(set_delta_gcv_needs_reorder, net, ks, aavrsns, next);
      }
    });
  }
}

function debug_set_per_gcv_key_gcv_needs_reorder(ks, gcv, avrsns, nreo) {
  ZH.e('(GC-WAIT) SET-KEY-GCV-NREO: K: ' + ks.kqk + ' GCV: ' + gcv +
       ' N: ' + nreo);
  ZH.l('(GC-WAIT) SET-KEY-GCV-NREO: K: ' + ks.kqk + ' AVS[]'); ZH.p(avrsns);
}

function set_single_per_gcv_key_gcv_needs_reorder(net, ks, gcv, avrsns, next) {
  var nreo = Object.keys(avrsns).length;
  debug_set_per_gcv_key_gcv_needs_reorder(ks, gcv, avrsns, nreo);
  var gkey = ZS.GetAgentKeyGCVNeedsReorder(ks);
  net.plugin.do_set_field(net.collections.global_coll, gkey, gcv, avrsns,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var aavrsns = [];
      for (var avrsn in avrsns) aavrsns.push(avrsn);
      set_delta_gcv_needs_reorder(net, ks, aavrsns, next);
    }
  });
}

function set_per_gcv_key_gcv_needs_reorder(net, ks, agavrsns, next) {
  if (agavrsns.length === 0) next(null, null);
  else {
    var agavrsn = agavrsns.shift();
    var gcv     = agavrsn.gcv;
    var avrsns  = agavrsn.avrsns;
    set_single_per_gcv_key_gcv_needs_reorder(net, ks, gcv, avrsns,
    function(aerr, ares) {
      if (aerr) next(aerr, null);
      else {
        setImmediate(set_per_gcv_key_gcv_needs_reorder,
                     net, ks, agavrsns, next);
      }
    });
  }
}

function set_key_gcv_needs_reorder(net, ks, xgcv, dentries, next) {
  var gavrsns  = get_per_gcv_number_key_gcv_needs_reorder(ks, xgcv, dentries);
  var agavrsns = [];
  for (var gcv in gavrsns) {
    agavrsns.push({gcv : gcv, avrsns : gavrsns[gcv]});
  }
  set_per_gcv_key_gcv_needs_reorder(net, ks, agavrsns, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ANALYZE KEY-GCV-NREO ------------------------------------------------------

function fetch_range_deltas(net, ks, authors, mdentries, next) {
  if (authors.length === 0) next(null, null);
  else {
    var author = authors.shift();
    ZSD.FetchSubscriberDelta(net.plugin, net.collections, ks, author,
    function(gerr, dentry) {
      if (gerr) next(gerr, null);
      else {
        if (dentry) mdentries.push(dentry);
        setImmediate(fetch_range_deltas, net, ks, authors, mdentries, next);
      }
    });
  }
}

//  DELTAS PRE-CURRENT-GCV are IGNORED on DO_GC
function analyze_pending_deltas_on_do_gc(cgcv, dentries) {
  var tor = [];
  for (var i = 0; i < dentries.length; i++) {
    var dentry = dentries[i];
    var dgcv   = ZH.GetDentryGCVersion(dentry);
    if (dgcv < cgcv) {
      var avrsn  = dentry.delta._meta.author.agent_version;
      ZH.l('analyze_pending_deltas_on_do_gc: IGNORE: AV: ' + avrsn +
           ' CGCV: ' + cgcv + ' DGCV: ' + dgcv);
      tor.push(i);
    }
  }
  for (var i = tor.length - 1; i >= 0; i--) {
    dentries.splice(tor[i], 1);
  }
  var ok = (dentries.length === 0); // IGNORE ALL DELTAS -> DO RUN-GC
  ZH.l('analyze_pending_deltas_on_do_gc: OK: ' + ok);
  return ok;
}

exports.AnalyzeKeyGCVNeedsReorderRange = function(net, ks, cgcv, diffs, next) {
  ZH.l('ZOOR.AnalyzeKeyGCVNeedsReorderRange: (C)GCV: ' + cgcv); ZH.p(diffs);
  var xgcv    = -1; // NOTE: JERRY-RIG to IGNORE gmismatch check
  var authors = [];
  for (var suuid in diffs) {
    var diff   = diffs[suuid];
    var mavrsn = diff.mavrsn;
    var davrsn = diff.davrsn;
    var mavnum = ZH.GetAvnum(mavrsn);
    var davnum = ZH.GetAvnum(davrsn);
    // DAVNUM is OK(SKIP), MAVNUM NEEDS REORDER(INCLUDE)
    for (var avnum = (davnum + 1); avnum <= mavnum; avnum++) {
      var avrsn  = ZH.CreateAvrsn(suuid, avnum);
      var author = {agent_uuid : suuid, agent_version : avrsn};
      authors.push(author);
    }
  }
  var mdentries = [];
  fetch_range_deltas(net, ks, authors, mdentries, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var ok = analyze_pending_deltas_on_do_gc(cgcv, mdentries);
      if (ok) next(null, true); // OK -> DO RUN-GC
      else {
        set_key_gcv_needs_reorder(net, ks, xgcv, mdentries,
        function(serr, sres) {
          next(null, false);    // NOT OK -> DO NOT RUN-GC -> GC-WAIT
        });
      }
    }
  });
}

function get_max_zero_level_needs_reorder(net, ks, gcv, rogcv, next) {
  ZH.l('get_max_zero_level_needs_reorder: K: ' + ks.kqk +
       ' GCV: ' + gcv + ' ROGCV: ' + rogcv);
  get_num_agent_key_gcv_needs_reorder(net, ks, gcv, function(gerr, nreo) {
    if (gerr) next(gerr, null);
    else {
      ZH.l('GET_ZERO_LEVEL: (GC-WAIT) GET-KEY-GCV-NREO: K: ' + ks.kqk +
           ' GCV: ' + gcv + ' N: ' + nreo);
      if (nreo) { // NOT ZERO LEVEL, RETURN PREVIOUS GCV
        next(null, (gcv - 1));
      } else {
        if (gcv === rogcv) next(null, gcv); // MAX POSSIBLE GCV
        else {
          gcv = gcv + 1; // CHECK NEXT GCV
          setImmediate(get_max_zero_level_needs_reorder,
                       net, ks, gcv, rogcv, next);
        }
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOCAL (STORAGE/SUBSCRIBER) ACCESSORS --------------------------------------

function create_local_delta_PC(net, ks, dentry, md, is_sub, next) {
  if (is_sub) {
    ZSD.CreateSubscriberDeltaPC(net, ks, dentry, md, next);
  } else {
    ZRAD.CreateStorageDeltaPC  (net, ks, dentry, md, next);
  }
}

function get_local_agent_versions(net, ks, is_sub, next) {
  if (is_sub) ZDelt.GetAgentAgentVersions (net, ks, next);
  else        ZRAD.GetCentralAgentVersions(net, ks, next);
}

function fetch_local_delta(net, ks, author, is_sub, next) {
  if (is_sub) {
    ZSD.FetchSubscriberDelta(net.plugin, net.collections, ks, author, next);
  } else {
    ZRAD.FetchStorageDelta  (net.plugin, net.collections, ks, author, next);
  }
}

// NOTE: next(serr, ok, ncrdt) - 3 ARGUMENTS
function handle_local_internal_delta(net, ks, dentry, md,
                                     is_ref, is_sub, next) {
  if (is_sub) {
    ZSD.InternalHandleSubscriberDelta(net, ks, dentry, md, next);
  } else {
    ZRAD.InternalHandleStorageDelta(net, ks, dentry, md, is_ref, next);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REORDER SUBSCRIBER DELTA --------------------------------------------------

function do_process_reorder_delta(net, pc, is_sub, next) {
  var ks     = pc.ks;
  var dentry = pc.dentry;
  var md     = pc.extra_data.md;
  var author = dentry.delta._meta.author;
  var avrsn  = author.agent_version;
  ZH.e('ZOOR.REORDER: APPLY: K: ' + ks.kqk + ' RF-AV: ' + avrsn);
  handle_local_internal_delta(net, ks, dentry, md, true, is_sub,
  function(serr, ok, ncrdt) {
    if (serr) next(serr, null);
    else {
      ZH.l('ZOOR.do_process_reorder_delta: OK: ' + ok);
      if (!ok) next(null, null); // INTERNAL NO-OP
      else {                     // SUCCESFFULY APPLIED -> NO LONGER OOO
        if (ncrdt) ZH.SetNewCrdt(pc, md, ncrdt, false);
        exports.UnsetOOOGCVDelta(net, ks, author, function(rerr, rres) {
          if (rerr) next(rerr, null);
          else {
            exports.DoRemoveOOODelta(net.plugin, net.collections,
                                    ks, author, next);
          }
        });
      }
    }
  });
}

function process_reorder_delta(net, pc, rfdentry, is_sub, next) {
  var ks       = pc.ks;
  var rodentry = pc.dentry;     // FROM SUBSCRIBER-DELTA
  var auuid    = rodentry.delta._meta.author.agent_uuid;
  var rrdentry = ZMerge.AddReorderToReference(rodentry, rfdentry);
  pc.dentry    = rrdentry;      // [REORDER + REFERENCE] DELTA
  do_process_reorder_delta(net, pc, is_sub, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      exports.CheckReplayOOODeltas(net, pc, true, is_sub, function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          pc.dentry = rodentry; // FROM SUBSCRIBER-DELTA
          next(null, null);
        }
      });
    }
  });
}

function debug_post_check_agent_gc_wait(fgcw, gcwi, zreo, rfgcv, ogcv) {
  ZH.l('post_check_agent_gc_wait: (GC-WAIT): FIN-GCW: ' + fgcw +
       ' GCWI: '  + gcwi  + ' ZREO: ' + zreo +
       ' RFGCV: ' + rfgcv + ' OGCV: ' + ogcv);
}

function post_check_agent_gc_wait(net, pc, zreo, next) {
  var ks       = pc.ks;
  var rodentry = pc.dentry;
  var md       = pc.extra_data.md;
  var ogcv     = md.ogcv;
  var rogcv    = ZH.GetDentryGCVersion(rodentry);
  var rfgcv    = rodentry.delta._meta.reference_GC_version;
  var wkey     = ZS.GetAgentKeyGCWaitIncomplete(ks);
  net.plugin.do_get_field(net.collections.global_coll, wkey, "value",
  function(ferr, gcwi) {
    if (ferr) next(ferr, null);
    else {
      var fgcw = gcwi || (zreo && (rfgcv === ogcv)); // OGCV governs FIN-GCW
      debug_post_check_agent_gc_wait(fgcw, gcwi, zreo, rfgcv, ogcv);
      if (!fgcw) next(null, null);
      else {
        var bgcv;
        if (gcwi) bgcv = ogcv  + 1; // OGCV already applied
        else      bgcv = rfgcv + 1; // REFERENCE-DELTA-GCV already applied
        if (!pc.ncrdt) ZH.SetNewCrdt(pc, md, md.ocrdt, false);
        get_max_zero_level_needs_reorder(net, ks, bgcv, rogcv,
        function(gerr, egcv) {
          if (gerr) next(gerr, null);
          else {
            if (egcv >= bgcv) ZGC.FinishAgentGCWait(net, pc, bgcv, egcv, next);
            else {
              ZH.e('FINISH-AGENT-GC-WAIT: NON-MINIMUM-GCV: BGCV: ' + bgcv +
                   ' EGCV: ' + egcv + '-> NO-OP');
              next(null, null);
            }
          }
        });
      }
    }
  });
}

exports.DecrementReorderDelta = function(net, pc, next) {
  var ks       = pc.ks;
  var rodentry = pc.dentry;
  var rfavrsn  = rodentry.delta._meta.reference_version;
  var rfgcv    = rodentry.delta._meta.reference_GC_version;
  var gc_gcv   = rfgcv + 1; // NEXT GC_version will be GC-WAIT
  ZH.l('ZOOR.DecrementReorderDelta: K: ' + ks.kqk + ' GCV: ' + gc_gcv);
  decrement_agent_key_gcv_needs_reorder(net, ks, gc_gcv, rfavrsn,
  function(serr, zreo) {
    if (serr) next(serr, null);
    else      post_check_agent_gc_wait(net, pc, zreo, next);
  });
}

function agent_post_reorder_delta(net, pc, next) {
  var ks = pc.ks;
  ZH.l('agent_post_reorder_delta: K: ' + ks.kqk);
  exports.DecrementReorderDelta(net, pc, next);
}

function set_wait_on_reference_delta(net, ks, roauthor, rfauthor, next) {
  ZH.e('set_wait_on_reference_delta: REFERENCE-DELTA NOT YET ARRIVED');
  // PERSIST BOTH NEED_REFERENCE & NEED_REORDER
  var fkey   = ZS.GetDeltaNeedReference(ks, roauthor);
  var fentry = {ks : ks, reference_author : rfauthor};
  net.plugin.do_set(net.collections.oood_coll, fkey, fentry,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var okey   = ZS.GetDeltaNeedReorder(ks, rfauthor);
      var oentry = {ks : ks, reorder_author : roauthor};
      net.plugin.do_set(net.collections.oood_coll, okey, oentry, next);
    }
  });
}

function get_need_apply_reorder_delta(net, ks, rfauthor, is_sub, next) {
  var rauuid  = rfauthor.agent_uuid;
  var ravrsn  = rfauthor.agent_version;
  var rselfie = (rauuid === ZH.MyUUID);
  if (rselfie) {
    ZH.l('get_need_apply_reorder_delta: RSELFIE: NEED: true');
    next(null, true);
  } else {
    get_local_agent_versions(net, ks, is_sub, function(gerr, mavrsns) {
      if (gerr) next(gerr, null);
      else {
        if (!mavrsns) {
          ZH.l('get_need_apply_reorder_delta: NO SAVRSNS[]: NEED: false');
          next(null, false);
        } else {
          var mavrsn  = mavrsns[rauuid];
          var mavnum  = ZH.GetAvnum(mavrsn);
          var ravnum  = ZH.GetAvnum(ravrsn);
          var need    = (mavnum && mavnum < ravnum);
          ZH.l('get_need_apply_reorder_delta: RUUID: ' + rauuid +
               ' MAVRSN: ' + mavrsn + ' RAVRSN: ' + ravrsn + ' NEED: ' + need);
          next(null, need);
        }
      }
    });
  }
}

function persist_set_wait_on_reference_delta(net, ks, rodentry, rfauthor,
                                             next) {
  var is_sub = true;
  ZH.e("FINISH-GC-WAIT-WAIT-ON-REFERENCE");
  conditional_persist_delta(net, ks, rodentry, is_sub, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var roauthor = rodentry.delta._meta.author;
      set_wait_on_reference_delta(net, ks, roauthor, rfauthor, next);
    }
  });
}

function do_finish_gc_wait_apply_reorder_delta(net, pc, rodentry,
                                               rfdentry, next) {
  var is_sub  = true;
  var odentry = pc.dentry; // ORIGINAL DENTRY that CAUSED ForwardGCV
  pc.dentry   = rodentry;
  ZH.e("FINISH-GC-WAIT-APPLY-REORDER-DELTA");
  // NOTE: APPLY [RODENTRY & RFDENTRY] to CRDT
  process_reorder_delta(net, pc, rfdentry, is_sub, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      pc.dentry = odentry; // REPLACE WITH ORIGINAL DENTRY
      next(null, null);
    }
  });
}

exports.ForwardCrdtGCVersionApplyReferenceDelta = function(net, pc, 
                                                           rodentry, next) {
  var is_sub   = true;
  var ks       = pc.ks;
  var rfauthor = create_reference_author(rodentry);
  get_need_apply_reorder_delta(net, ks, rfauthor, is_sub, function(gerr, need) {
    if (gerr) next(gerr, null);
    else {
      if (!need) next(null, null);
      else {
        ZSD.FetchSubscriberDelta(net.plugin, net.collections, ks, rfauthor,
        function(ferr, rfdentry) {
          if (ferr) next(ferr, null);
          else {
            if (rfdentry) {
              do_finish_gc_wait_apply_reorder_delta(net, pc, rodentry,
                                                    rfdentry, next);
            } else { // REFERENCE-DELTA NOT YET ARRIVED
              persist_set_wait_on_reference_delta(net, ks, rodentry,
                                                  rfauthor, next);
            }
          }
        });
      }
    }
  });
}

function apply_reference_delta(net, pc, rfdentry, is_sub, next){
  if (rfdentry) {
    process_reorder_delta(net, pc, rfdentry, is_sub, next);
  } else { // REFERENCE-DELTA NOT YET ARRIVED
    var ks       = pc.ks;
    var rodentry = pc.dentry;
    var roauthor = rodentry.delta._meta.author;
    var rfauthor = create_reference_author(rodentry);
    set_wait_on_reference_delta(net, ks, roauthor, rfauthor, next);
  }
}

function do_apply_reorder_delta(net, pc, is_sub, next) {
  var afunc    = is_sub ? ZAD.ApplySubscriberDelta : ZAD.CentralApplyDelta;
  var ks       = pc.ks;
  var rodentry = pc.dentry;
  var rometa   = rodentry.delta._meta;
  var rfauthor = create_reference_author(rodentry);
  get_need_apply_reorder_delta(net, ks, rfauthor, is_sub, function(gerr, need) {
    if (gerr) next(gerr, null);
    else {
      if (!need) { // APPLY REORDER[] but NOT REFERENCE_DELTA
        if (rometa.reference_ignore) {
          rometa.DO_IGNORE = true; // Used in ZAD.do_apply_delta
        } else {
          rometa.REORDERED = true; // Used in ZAD.do_apply_delta
        }
        afunc(net, pc, next);
      } else {
        fetch_local_delta(net, ks, rfauthor, is_sub, function (ferr, rfdentry) {
          if (ferr) next(ferr, null);
          else      apply_reference_delta(net, pc, rfdentry, is_sub, next);
        });
      }
    }
  });
}

function normal_agent_apply_reorder_delta(net, pc, next) {
  var is_sub   = true;
  var ks       = pc.ks;
  ZH.l('normal_agent_apply_reorder_delta: K: ' + ks.kqk);
  var rodentry = pc.dentry;
  var reorder  = rodentry.delta.reorder;
  ZGC.AddReorderToGCSummaries(net, ks, reorder, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
     do_apply_reorder_delta(net, pc, is_sub, function(serr, sres) {
        if (serr) next(serr, null);
        else      agent_post_reorder_delta(net, pc, next);
      });
    }
  });
}

function gc_wait_agent_apply_reorder_delta(net, pc, next) {
  var ks       = pc.ks;
  var rodentry = pc.dentry;
  var rogcv    = ZH.GetDentryGCVersion(rodentry);
  var rometa   = rodentry.delta._meta;
  var reorder  = rodentry.delta.reorder;
  ZH.e('DO_REORDER: OOOGCW: K: ' + ks.kqk + ' DURING AGENT-GC-WAIT');
  ZGC.AddReorderToGCSummaries(net, ks, reorder, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      ZGC.AddOOOGCWToGCSummary(net, pc, rogcv, rometa, reorder,
      function(serr, sres) {
        if (serr) next(serr, null);
        else      agent_post_reorder_delta(net, pc, next);
      });
    }
  });
}

function agent_apply_reorder_delta(net, pc, next) {
  var ks = pc.ks;
  exports.AgentInGCWait(net, ks, function(gerr, gwait) {
    if (gerr) next(gerr, null);
    else {
      ZH.l('REO: (GC-WAIT) GET-KEY-GCV-NREO: K: ' + ks.kqk + ' GW: ' + gwait);
      if (!gwait) normal_agent_apply_reorder_delta (net, pc, next);
      else        gc_wait_agent_apply_reorder_delta(net, pc, next);
    }
  });
}

function central_apply_reorder_delta(net, pc, next) {
  var is_sub   = false;
  var ks       = pc.ks;
  var rodentry = pc.dentry;
  var reorder  = rodentry.delta.reorder;
  ZGC.AddReorderToGCSummaries(net, ks, reorder, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else      do_apply_reorder_delta(net, pc, is_sub, next);
  });
}

exports.ApplyReorderDelta = function(net, pc, is_sub, next) {
  if (is_sub) agent_apply_reorder_delta  (net, pc, next);
  else        central_apply_reorder_delta(net, pc, next);
}

// NOTE: next(CRDT)
function internal_apply_reorder_delta(net, ks, dentry, md, is_sub, next) {
  var avrsn = dentry.delta._meta.author.agent_version;
  ZH.e('internal_apply_reorder_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  create_local_delta_PC(net, ks, dentry, md, is_sub, function(cerr, pc) {
    if (cerr) next(cerr, null);
    else {
      exports.ApplyReorderDelta(net, pc, is_sub, function(serr, sres) {
        next(serr, pc.ncrdt); // NOTE: next(CRDT)
      });
    }
  });
}

function save_ooo_gcv_delta(net, ks, dentry, is_sub, next) {
  var auuid = dentry.delta._meta.author.agent_uuid;
  var akey  = ZS.GetAgentKeyOOOGCV(ks, auuid);
  net.plugin.do_set_field(net.collections.global_coll, akey, "value", true,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var einfo = {is_sub  : is_sub,
                   kosync  : false,
                   knreo   : true,
                   central : dentry.delta._meta.from_central};
      exports.PersistOOODelta(net, ks, dentry, einfo, next);
    }
  });
}

// NOTE: next(CRDT)
exports.HandleOOOGCVDelta = function(net, ks, md, dentry, is_sub, next) {
  ZH.l('ZOOR.HandleOOOGCVDelta: K: ' + ks.kqk);
  var author = dentry.delta._meta.author;
  var auuid  = author.agent_uuid;
  save_ooo_gcv_delta(net, ks, dentry, is_sub, function(perr, pres) {
    if (perr) next(perr, null);
    else {
      var okey = ZS.GetDeltaNeedReorder(ks, author);
      net.plugin.do_get_field(net.collections.oood_coll, okey, "reorder_author",
      function(gerr, roauthor) {
        if (gerr) next(gerr, null);
        else {
          if (!roauthor) next(null, null);
          else {
            fetch_local_delta(net, ks, roauthor, is_sub,
            function(ferr, rodentry) {
              if (ferr) next(ferr, null);
              else {
                if (!rodentry) next(null, null);
                else {
                  internal_apply_reorder_delta(net, ks, rodentry,
                                               md, is_sub, next);
                }
              }
            });
          }
        }
      });
    }
  });
}

exports.UnsetOOOGCVDelta = function(net, ks, author, next) {
  ZH.l('ZOOR.UnsetOOOGCVDelta: K: ' + ks.kqk);
  var auuid = author.agent_uuid;
  var akey  = ZS.GetAgentKeyOOOGCV(ks, auuid);
  net.plugin.do_remove(net.collections.global_coll, akey, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      var okey = ZS.GetDeltaNeedReorder(ks, author);
      net.plugin.do_get_field(net.collections.oood_coll, okey, "reorder_author",
      function(gerr, roauthor) {
        if (gerr) next(gerr, null);
        else {
          if (!roauthor) next(null, null);
          else { // REMOVE BOTH NEED_REFERENCE & NEED_REORDER
            var fkey = ZS.GetDeltaNeedReference(ks, roauthor);
            net.plugin.do_remove(net.collections.oood_coll, fkey,
            function(uerr, ures) {
              if (uerr) next(uerr, null);
              else {
                var okey = ZS.GetDeltaNeedReorder(ks, author);
                net.plugin.do_remove(net.collections.oood_coll, okey, next);
              }
            });
          }
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE OOO SUBSCRIBER/STORAG DELTAS ----------------------------------------

var CENTRAL_THRESHOLD_UNEXPECTED_DELTA_TIMEOUT = 25000; /* 11 seconds */
var CENTRAL_UNEXPECTED_DELTA_TIMEOUT           = 10000; /* 10 seconds */

var AGENT_THRESHOLD_UNEXPECTED_DELTA_TIMEOUT   = 5000;  /* 5 seconds */
var AGENT_UNEXPECTED_DELTA_TIMEOUT             = 2000;  /* 2 seconds */

function get_unexpected_delta_timeout() {
  return ZH.AmCentral ? CENTRAL_UNEXPECTED_DELTA_TIMEOUT :
                        AGENT_UNEXPECTED_DELTA_TIMEOUT;
}

function get_threshold_unexpected_delta_timeout() {
  return ZH.AmCentral ? CENTRAL_THRESHOLD_UNEXPECTED_DELTA_TIMEOUT :
                        AGENT_THRESHOLD_UNEXPECTED_DELTA_TIMEOUT;
}

var UnexpectedDeltaTimer    = {}; // PER KQK
var UnexpectedDeltaTS       = {}; // PER KQK
var UnexpectedDeltaPerAgent = {}; // PER [KQK,AUUID}

function cancel_unexpected_delta_timer(ks) {
  ZH.e('cancel_unexpected_delta_timer (UNEXSD) K: ' + ks.kqk);
  if (UnexpectedDeltaTimer[ks.kqk]) {
    clearTimeout(UnexpectedDeltaTimer[ks.kqk]);
    UnexpectedDeltaTimer   [ks.kqk] = null;
    UnexpectedDeltaPerAgent[ks.kqk] = null;
  }
  UnexpectedDeltaTS[ks.kqk] = 0;
}

function do_handle_unexpected_delta(net, ks, auuid, einfo, next) {
  ZH.e('do_handle_unexpected_delta (UNEXSD) K: ' + ks.kqk + ' AU: ' + auuid);
  cancel_unexpected_delta_timer(ks);
  var hfunc = einfo.is_sub ? ZSD.HandleUnexpectedOOODelta :
                             ZRAD.HandleUnexpectedOOODelta;
  hfunc(net, ks, einfo, next);
}

function set_timer_unexpected_subscriber_delta(net, ks, auuid,
                                               einfo, now, next) {
  UnexpectedDeltaTS[ks.kqk]              = now;
  if (!UnexpectedDeltaPerAgent[ks.kqk]) UnexpectedDeltaPerAgent[ks.kqk] = {};
  UnexpectedDeltaPerAgent[ks.kqk][auuid] = true;
  var to                                 = get_unexpected_delta_timeout();
  ZH.e('UNEXPECTED OOO-SD K: ' + ks.kqk + ' -> SET-TIMER: (UNEXSD): ' + to);
  UnexpectedDeltaTimer[ks.kqk]           = setTimeout(function() {
    do_handle_unexpected_delta(net, ks, auuid, einfo, ZH.OnErrLog);
  }, to);
  next(null, null);
}

function action_ooo_delta(net, ks, auuid, einfo, next) {
  var ok = einfo.kosync;
  if (!ok) ok = (einfo.ooodep && !einfo.oooav);
  if (!ok) ok = (einfo.knreo  && !einfo.central);
  ZH.e('action_ooo_delta: K: ' + ks.kqk + ' kosync: ' + einfo.kosync + 
       ' ooodep: ' + einfo.ooodep + ' oooav: '   + einfo.oooav +
       ' knreo: '  + einfo.knreo  + ' central: ' + einfo.central +
       ' -> OK: '  + ok);
  if (ok) next(null, null);
  else {
    var now       = ZH.GetMsTime();
    var ts        = UnexpectedDeltaTS[ks.kqk];
    if (ZH.IsUndefined(ts)) ts = 0;
    var diff      = (now - ts);
    var sane      = (diff > 0);
    var threshold = get_threshold_unexpected_delta_timeout();
    ZH.e('action_ooo_delta: TS: ' + ts + ' NOW: ' + now + ' DIFF: ' + diff +
         ' THRESHOLD: ' + threshold);
    if (ts && sane && (diff < threshold)) {
      ZH.e('UNEXPECTED OOO-SD K: ' + ks.kqk + ' (TIMER-SET) -> NO-OP (UNEXSD)');
      next(null, null);
    } else {
      set_timer_unexpected_subscriber_delta(net, ks, auuid, einfo, now, next);
    }
  }
}

function conditional_persist_delta(net, ks, dentry, is_sub, next) {
  if (is_sub) ZSD.ConditionalPersistSubscriberDelta(net, ks, dentry, next);
  else        ZRAD.ConditionalPersistStorageDelta(net, ks, dentry, next);
}

function do_persist_ooo_delta(net, ks, dentry, einfo, next) {
  var author = dentry.delta._meta.author;
  var auuid  = author.agent_uuid;
  var avrsn  = author.agent_version;
  ZH.l('ZOOR.PersistOOODelta: K: ' + ks.kqk + ' AV: ' + avrsn);
  var kkey   = ZS.GetOOOKey(ks.kqk);
  net.plugin.do_increment(net.collections.delta_coll, kkey, "value", 1,
  function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      var okey = ZS.GetOOODelta(ks, author);
      net.plugin.do_set_field(net.collections.delta_coll, okey, "value", true,
      function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          var akey = ZS.GetOOOKeyDeltaVersions(ks.kqk);
          net.plugin.do_push(net.collections.delta_coll,
                             akey, "aversions", avrsn,
          function(verr, vres) {
            if (verr) next(verr, null);
            else {
              conditional_persist_delta(net, ks, dentry, einfo.is_sub,
              function(serr, sres) {
                if (serr) next(serr, null);
                else      action_ooo_delta(net, ks, auuid, einfo, next);
              });
            }
          });
        }
      });
    }
  });
}

exports.PersistOOODelta = function(net, ks, dentry, einfo, next) {
  var author = dentry.delta._meta.author;
  var okey   = ZS.GetOOODelta(ks, author);
  net.plugin.do_get_field(net.collections.delta_coll, okey, "value",
  function(oerr, oooav) {
    if (oerr) next(oerr, null);
    else {
      if (!oooav) do_persist_ooo_delta(net, ks, dentry, einfo, next);
      else {
        var avrsn = author.agent_version;
        ZH.e('ZOOR.PersistOOODelta: K: ' + ks.kqk +
             ' AV: ' + avrsn + ' DELTA ALREADY PERSISTED -> NO-OP');
        next(null, null);
      }
    }
  });
}

function do_remove_ooo_delta_metadata(plugin, collections, ks, next) {
  ZH.l('do_remove_ooo_delta_metadata: K : ' + ks.kqk);
  var kkey = ZS.GetOOOKey(ks.kqk);
  plugin.do_remove(collections.delta_coll, kkey, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      var akey = ZS.GetOOOKeyDeltaVersions(ks.kqk);
      plugin.do_remove(collections.delta_coll, akey, next);
    }
  });
}

function do_remove_ooo_delta(plugin, collections, ks, author, next) {
  var avrsn = author.agent_version;
  ZH.l('ZOOR.DoRemoveOOODelta: K: ' + ks.kqk + ' AV: ' + avrsn);
  var okey = ZS.GetOOODelta(ks, author);
  plugin.do_remove(collections.delta_coll, okey, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      var avrsn = author.agent_version;
      var akey  = ZS.GetOOOKeyDeltaVersions(ks.kqk);
      plugin.do_pull(collections.delta_coll, akey, "aversions", avrsn,
      function (xerr, xres) {
        if (xerr) next(xerr, null); // PULL SHOULD GET AN ELEMENT
        else {
          var kkey = ZS.GetOOOKey(ks.kqk);
          plugin.do_increment(collections.delta_coll, kkey, "value", -1,
          function(uerr, ures) {
            if (uerr) next(uerr, null);
            else {
              var noooav = Number(ures.value);
              if (noooav) next(null, null);
              else {
                do_remove_ooo_delta_metadata(plugin, collections, ks, next);
              }
            }
          });
        }
      });
    }
  });
}

exports.DoRemoveOOODelta = function(plugin, collections, ks, author, next) {
  var avrsn = author.agent_version;
  var okey  = ZS.GetOOODelta(ks, author);
  plugin.do_get_field(collections.delta_coll, okey, "value",
  function(oerr, oooav) {
    if (oerr) next(oerr, null);
    else {
      if (!oooav) next(null, null);
      else        do_remove_ooo_delta(plugin, collections, ks, author, next)
    }
  });
}

exports.RemoveSubscriberDelta = function(plugin, collections,
                                         ks, author, next) {
  ZDS.RemoveSubscriberDelta(plugin, collections, ks, author,
  function(rerr, rres) {
    if (rerr) next(rerr, null);
    else      exports.DoRemoveOOODelta(plugin, collections, ks, author, next);
  });
}

exports.RemoveStorageDelta = function(plugin, collections, ks, author, next) {
  ZDS.CentralRemoveDelta(plugin, collections, ks, author, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else      exports.DoRemoveOOODelta(plugin, collections, ks, author, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// IRRELEVANT DELTAS (ON MERGE) ----------------------------------------------

function local_remove_irrelevant_delta(net, ks, author, is_sub, next) {
  var avrsn = author.agent_version;
  ZH.l('local_remove_irrelevant_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  if (is_sub) ZAS.DoCommitDelta(net, ks, author, next);
  else        ZGack.DoStorageCommitDelta(net, ks, author, next);
}

function remove_irrelevant_deltas(net, ks, authors, is_sub, next) {
  if (authors.length === 0) next(null, null);
  else {
    var author = authors.shift();
    local_remove_irrelevant_delta(net, ks, author, is_sub,
    function(rerr, rres) {
      if (rerr) next(rerr, null);
      else {
        setImmediate(remove_irrelevant_deltas, net, ks, authors, is_sub, next);
      }
    });
  }
}

function add_irrelevant_agent_version(authors, cavrsns, avrsns) {
  if (!avrsns) return;
  for (var i = 0; i < avrsns.length; i++) {
    var avrsn  = avrsns[i];
    var res    = avrsn.split('|');
    var auuid  = Number(res[0]);
    var avnum  = Number(res[1]);
    var cavrsn = cavrsns[auuid];
    var cavnum = ZH.GetAvnum(cavrsn);
    if (avnum <= cavnum) {
      var author = {agent_uuid : auuid, agent_version : avrsn};
      authors.push(author);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SORT OOO DELTAS (DEPENDENCIES) --------------------------------------------

function add_contiguous_agent_version(authors, cavrsns, avrsns) {
  if (!avrsns) return;
  for (var i = 0; i < avrsns.length; i++) {
    var avrsn  = avrsns[i];
    var res    = avrsn.split('|');
    var auuid  = Number(res[0]);
    var avnum  = Number(res[1]);
    var cavrsn = cavrsns[auuid];
    var cavnum = ZH.GetAvnum(cavrsn);
    var eavnum = cavnum + 1;
    if (avnum === eavnum) {
      cavrsns[auuid] = avrsn; // NOTE: modifies cavrsns[]
      var author     = {agent_uuid : auuid, agent_version : avrsn};
      authors.push(author);
    }
  }
}

function fetch_author_deltas(net, ks, is_sub, authors, adentries, next) {
  if (authors.length === 0) next(null, null);
  else {
    var author = authors.shift();
    var auuid  = author.agent_uuid;
    fetch_local_delta(net, ks, author, is_sub, function (gerr, odentry) {
      if (gerr) next(gerr, null);
      else {
        if (odentry) { //NOTE: odentry missing on RECURSIVE CORNER CASES
          if (!adentries[auuid]) adentries[auuid] = [];
          adentries[auuid].push(odentry);
        }
        setImmediate(fetch_author_deltas, net,
                     ks, is_sub, authors, adentries, next);
      }
    });
  }
}

function cmp_dependency(dentry1, dentry2) {
  var meta1  = dentry1.delta._meta;
  var auuid1 = meta1.author.agent_uuid;
  var avrsn1 = meta1.author.agent_version;
  var dep1   = meta1.dependencies;
  var meta2  = dentry2.delta._meta;
  var auuid2 = meta2.author.agent_uuid;
  var avrsn2 = meta2.author.agent_version;
  var dep2   = meta2.dependencies;
  var rdep   = dep2 ? dep2[auuid1] : undefined;
  var ldep   = dep1 ? dep1[auuid2] : undefined;
  var ret;
  if (!rdep && !ldep) {
    ret        = 0;
  } else if (rdep) {
    var avnum1 = ZH.GetAvnum(avrsn1);
    var ravnum = ZH.GetAvnum(rdep);
    ret        = (avnum1 === ravnum) ? 0 : ((avnum1 < ravnum) ? -1 : 1);
  } else { // LDEP
    var avnum2 = ZH.GetAvnum(avrsn2);
    var lavnum = ZH.GetAvnum(ldep);
    ret        = (avnum2 === lavnum) ? 0 : ((avnum2 < lavnum) ? 1 : -1);
  }
  //ZH.l('cmp_dependency: avrsn1: ' + avrsn1 + ' avrsn2: ' + avrsn2 +
       //' rdep: ' + rdep + ' ldep: ' + ldep + ' ret: ' + ret);
  return ret;
}

function init_dependency_sort_authors(adentries) {
  var fdentries = [];
  for (var auuid in adentries) {
    var dentries = adentries[auuid];
    var dentry   = dentries.shift(); // TAKE FIRST FROM EACH AUUID
    fdentries.push(dentry);
  }
  return fdentries;
}

function get_next_agent_dentry(adentries, auuid) {
  var dentries = adentries[auuid];
  if (!dentries) return null;
  var dentry   = dentries.shift(); // TAKE FIRST FROM AUUID
  return dentry;
}

function do_dependency_sort_authors(net, ks, adentries, rdentries, next) {
  var fdentries = init_dependency_sort_authors(adentries);
  while (fdentries.length) {
    fdentries.sort(cmp_dependency);
    var rdentry   = fdentries.shift(); // TAKE LOWEST DEPENDENCY
    rdentries.push(rdentry);
    var auuid     = rdentry.delta._meta.author.agent_uuid;
    var fdentry   = get_next_agent_dentry(adentries, auuid);
    if (fdentry) fdentries.push(fdentry);
  }
  next(null, rdentries);
}

// NOTE: DependencySort does not work perfectly yet -> still imperfect sorting
//         luckily the versioning engine deals with the imperfections :)
exports.DependencySortAuthors = function(net, ks, is_sub, authors, next) {
  var cauthors  = ZH.clone(authors); // NOTE: gets modified
  var adentries = {};
  fetch_author_deltas(net, ks, is_sub, cauthors, adentries,
  function(ferr, fres) {
    if (ferr) next(ferr, null);
    else {
      var rdentries = [];
      do_dependency_sort_authors(net, ks, adentries, rdentries, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REPLAY APPLY DELTAS -------------------------------------------------------

function replay_delta_version(net, pc, dentry, is_sub, next) {
  var ks = pc.ks;
  var md = pc.extra_data.md;
  handle_local_internal_delta(net, ks, dentry, md, false, is_sub,
  function(serr, ok, ncrdt) {
    if (serr) next(serr, null);
    else {
      ZH.l('ZOOR.replay_delta_version: OK: ' + ok);
      if (!ok) next(null, null); // INTERNAL NO-OP
      else {                     // SUCCESFFULY APPLIED -> NO LONGER OOO
        if (ncrdt) ZH.SetNewCrdt(pc, md, ncrdt, false);
        var author = dentry.delta._meta.author;
        exports.DoRemoveOOODelta(net.plugin, net.collections, ks, author, next);
      }
    }
  });
}

function do_replay_delta_versions(net, pc, dentries, is_sub, next) {
  if (dentries.length === 0) next(null, null);
  else {
    var dentry = dentries.shift();
    replay_delta_version(net, pc, dentry, is_sub, function(serr, sres) {
      if (serr) next(serr, null);
      else {
        setImmediate(do_replay_delta_versions, net, pc, dentries, is_sub, next);
      }
    });
  }
}

function replay_delta_versions(net, pc, xcrdt, dentries, is_sub, next) {
  var md   = pc.extra_data.md;
  md.ocrdt = ZH.clone(xcrdt); // do_replay_delta_versions() on THIS CRDT
  do_replay_delta_versions(net, pc, dentries, is_sub, next);
}

function get_merge_replay_deltas(net, ks, is_sub, cavrsns, next) {
  var ccavrsns = ZH.clone(cavrsns); // NOTE: gets modified
  var gkey     = is_sub ? ZS.GetAgentKeyDeltaVersions(ks.kqk) :
                          ZS.GetCentralKeyAgentVersions(ks.kqk);
  net.plugin.do_get_array_values(net.collections.delta_coll, gkey, "aversions",
  function(gerr, avrsns) {
    if (gerr) next(gerr, null);
    else {
      var akey = ZS.GetOOOKeyDeltaVersions(ks.kqk);
      net.plugin.do_get_array_values(net.collections.delta_coll,
                                     akey, "aversions",
      function(ferr, oooavrsns) {
        if (ferr) next(ferr, null);
        else {
          var iauthors = [];
          var authors  = [];
          if (!avrsns) avrsns = [];
          if (oooavrsns) {
            for (var i = 0; i < oooavrsns.length; i++) {
              avrsns.push(oooavrsns[i]); // APPEND OOOAVRSNS[] TO AVRNS[]
            }
          }
          avrsns.sort(cmp_delta_agent_version);
          add_irrelevant_agent_version(iauthors, cavrsns,  avrsns);
          add_contiguous_agent_version(authors,  ccavrsns, avrsns);
          remove_irrelevant_deltas(net, ks, iauthors, is_sub,
          function(serr, sres) {
            if (serr) next(serr, null);
            else {
              exports.DependencySortAuthors(net, ks, is_sub, authors, next);
            }
          });
        }
      });
    }
  });
}

function adjust_replay_deltas_to_gc_summaries(ks, dentries, min_gcv) {
  var dmin_gcv = min_gcv ? (min_gcv - 1) : 0; // PREVIOUS GCV DELTAS ARE OK
  ZH.l('adjust_replay_deltas_to_gc_summaries: (D)MIN-GCV: ' + dmin_gcv);
  for (var i = 0; i < dentries.length; i++) {
    var dentry = dentries[i];
    var dgcv   = ZH.GetDentryGCVersion(dentry);
    if (dgcv < dmin_gcv) {
      var author = dentry.delta._meta.author;
      var avrsn  = author.agent_version;
      ZH.e('SET DO_IGNORE: K: ' + ks.kqk + ' AV: ' + avrsn);
      dentry.delta._meta.DO_IGNORE = true;
    }
  }
}

function adjust_gc_summaries_to_replay_deltas(gcsumms, dentries) {
  var min_gcv = Number.MAX_VALUE;
  for (var i = 0; i < dentries.length; i++) {
    var dentry = dentries[i];
    var ignore = dentry.delta._meta.DO_IGNORE;
    if (!ignore) {
      var dgcv = ZH.GetDentryGCVersion(dentry);
      if (dgcv < min_gcv) min_gcv = dgcv;
    }
  }
  ZH.l('adjust_gc_summaries_to_replay_deltas: MIN-GCV: ' + min_gcv);
  for (var sgcv in gcsumms) {
    var gcv = Number(sgcv);
    if (gcv <= min_gcv) { // LESS THAN OR EQUAL -> ALREADY PROCESSED MIN-GCV
      ZH.l('adjust_gc_summaries_to_replay_deltas: DELETE: GCV: ' + gcv);
      delete(gcsumms[gcv]);
    }
  }
}

// NOTE: ZSD.InternalHandleSubscriberDelta skips OOOGCV check
//       |-> AUTO-DTS can incorrectly advance ncrdt.GCV
function finalize_rewound_subscriber_merge(net, pc, next) {
  var md = pc.extra_data.md;
  ZH.l('ZOOR.replay_subscriber_merge_deltas: (F)GCV: ' + md.gcv);
  pc.ncrdt._meta.GC_version = md.gcv;
  next(null, null);
}

function replay_subscriber_merge_deltas(net, ks, pc, next) {
  var is_sub  = true;
  var xgcv    = pc.xcrdt._meta.GC_version;
  var cavrsns = pc.extra_data.cavrsns;
  var gcsumms = ZH.clone(pc.extra_data.gcsumms); // NOTE: GETS MODIFIED
  var md      = pc.extra_data.md;
  ZH.SetNewCrdt(pc, md, pc.xcrdt, true);
  ZH.l('ZOOR.replay_subscriber_merge_deltas: cavrsns'); ZH.p(cavrsns);
  get_merge_replay_deltas(net, ks, is_sub, cavrsns, function(gerr, rdentries) {
    if (gerr) next(gerr, null);
    else {
      var tot = get_total_number_key_gcv_needs_reorder(ks, xgcv, rdentries);
      ZH.l('ZOOR.replay_subscriber_merge_deltas: TOTAL(NUM): ' + tot);
      if (tot == 0) {                            // NO REWIND
        if (!rdentries.length) next(null, null); // NO-OP
        else {                                   // REPLAY
          debug_replay_merge_dentries(ks, rdentries);
          replay_delta_versions(net, pc, pc.ncrdt, rdentries, is_sub, next);
        }
      } else {                                    // REWIND AND REPLAY
        debug_replay_merge_dentries(ks, rdentries);
        var min_gcv = ZGC.GetMinGcv(gcsumms);
        adjust_replay_deltas_to_gc_summaries(ks, rdentries, min_gcv);
        ZH.l('ZOOR.replay_subscriber_merge_deltas: MIN-GCV: ' + min_gcv);
        adjust_gc_summaries_to_replay_deltas(gcsumms, rdentries);
        var ncrdt   = ZMerge.RewindCrdtGCVersion(ks, pc.xcrdt, gcsumms);
        ZH.SetNewCrdt(pc, md, ncrdt, true);
        md.ogcv     = md.gcv; // NOTE: SET MD.OGCV
        ZH.l('ZOOR.replay_subscriber_merge_deltas: (N)GCV: ' + md.gcv);
        set_key_gcv_needs_reorder(net, ks, xgcv, rdentries,
        function(serr, sres) {
          if (serr) next(serr, null);
          else {
            replay_delta_versions(net, pc, pc.ncrdt, rdentries, is_sub,
            function(aerr, ares) {
              if (aerr) next(aerr, null);
              else      finalize_rewound_subscriber_merge(net, pc, next)
            });
          }
        });
      }
    }
  });
}

exports.PrepareLocalDeltasPostSubscriberMerge = function(net, pc, next) {
  var is_sub  = true;
  var ks      = pc.ks;
  var cavrsns = pc.extra_data.cavrsns;
  ZH.l('ZOOR.PrepareLocalDeltasPostSubscriberMerge: cavrsns'); ZH.p(cavrsns);
  get_merge_replay_deltas(net, ks, is_sub, cavrsns, function(gerr, rdentries) {
    if (gerr) next(gerr, null);
    else {
      var xgcv = pc.xcrdt._meta.GC_version;
      set_key_gcv_needs_reorder(net, ks, xgcv, rdentries, next);
    }
  });
}

function replay_central_merge_deltas(net, ks, pc, next) {
  var is_sub  = false;
  var md      = pc.extra_data.md;
  var cavrsns = pc.extra_data.cavrsns;
  // CENTRAL does not need to REWIND-GCV
  ZH.SetNewCrdt(pc, md, pc.xcrdt, false);
  ZH.l('ZOOR.replay_central_merge_deltas: cavrsns'); ZH.p(cavrsns);
  get_merge_replay_deltas(net, ks, is_sub, cavrsns, function(gerr, rdentries) {
    if (gerr) next(gerr, null);
    else {
      debug_replay_merge_dentries(ks, rdentries);
      replay_delta_versions(net, pc, pc.ncrdt, rdentries, is_sub, next);
    }
  });
}

exports.ReplayMergeDeltas = function(net, ks, pc, is_sub, next) {
  if (is_sub) replay_subscriber_merge_deltas(net, ks, pc, next);
  else        replay_central_merge_deltas   (net, ks, pc, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE OOO SUBSCRIBER DELTAS ----------------------------------------------

function do_replay_ooo_deltas(net, pc, rdentries, is_sub, next) {
  var ks = pc.ks;
  var md = pc.extra_data.md;
  ZH.e('ZOOR.do_replay_ooo_deltas: K: ' + ks.kqk + ' rdentries[AV]: [' +
       debug_dentries_agent_versions(rdentries) + ']');
  replay_delta_versions(net, pc, md.ocrdt, rdentries, is_sub, next);
}

function get_replay_ooo_deltas(net, ks, is_sub, next) {
  get_local_agent_versions(net, ks, is_sub, function(gerr, mavrsns) {
    if (gerr) next(gerr, null);
    else {
      var akey = ZS.GetOOOKeyDeltaVersions(ks.kqk);
      net.plugin.do_get_array_values(net.collections.delta_coll,
                                     akey, "aversions",
      function(aerr, oooavrsns) {
        if (aerr) next(aerr, null);
        else {
          if (oooavrsns) oooavrsns.sort(cmp_delta_agent_version);
          var authors  = [];
          var cmavrsns = ZH.clone(mavrsns); // NOTE: gets modified 
          add_contiguous_agent_version(authors, cmavrsns, oooavrsns);
          exports.DependencySortAuthors(net, ks, is_sub, authors, next);
        }
      });
    }
  });
}

function replay_ooo_deltas(net, pc, is_sub, next) {
  var ks = pc.ks;
  get_replay_ooo_deltas(net, ks, is_sub, function(ferr, rdentries) {
    if (ferr) next(ferr, null);
    else      do_replay_ooo_deltas(net, pc, rdentries, is_sub, next);
  });
}

function check_agent_uuid_not_present(auuid, dentries) {
  for (var i = 0; i < dentries.length; i++) {
    var dentry = dentries[i];
    var dauuid = dentry.delta._meta.author.agent_uuid;
    if (auuid === dauuid) return false;
  }
  return true;
}

function check_finish_unexpected_delta(net, ks, auuid, is_sub, next) {
  var afire = UnexpectedDeltaPerAgent[ks.kqk] &&
              UnexpectedDeltaPerAgent[ks.kqk][auuid];
  if (!afire) next(null, null);
  else {
    get_replay_ooo_deltas(net, ks, is_sub, function(ferr, rdentries) {
      if (ferr) next(ferr, null);
      else {
        ZH.l('check_finish_unexpected_delta: (UNEXSD)')
        var ok = check_agent_uuid_not_present(auuid, rdentries);
        if (ok) {
          delete(UnexpectedDeltaPerAgent[ks.kqk][auuid]);
          var n = Object.keys(UnexpectedDeltaPerAgent[ks.kqk]).length;
          ZH.l('NUM UNEXSD PER KQK: ' + n);
          if (n === 0) {
            ZH.e('FINISH-UNEXSD: K: ' + ks.kqk + ' U: ' + auuid);
            cancel_unexpected_delta_timer(ks);
          }
        }
        next(null, null);
      }
    });
  }
}

function conditional_replay_ooo_deltas(net, pc, noooav, is_sub, next) {
  if (!noooav) next(null, null);
  else {
    var md = pc.extra_data.md;
    if (pc.ncrdt) { // replay_ooo_deltas() on THIS CRDT
      md.ocrdt = ZH.clone(pc.ncrdt);
    }
    replay_ooo_deltas(net, pc, is_sub, next);
  }
}

exports.CheckReplayOOODeltas = function(net, pc, do_check, is_sub, next) {
  var ks     = pc.ks;
  ZH.l('ZOOR.CheckReplayOOODeltas: K: ' + ks.kqk);
  var dentry = pc.dentry;
  var md     = pc.extra_data.md;
  var auuid  = dentry.delta._meta.author.agent_uuid;
  var kkey   = ZS.GetOOOKey(ks.kqk);
  net.plugin.do_get_field(net.collections.delta_coll, kkey, "value",
  function(gerr, noooav) {
    if (gerr) next(gerr, null);
    else {
      conditional_replay_ooo_deltas(net, pc, noooav, is_sub,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          if (!do_check) next(null, null);
          else {
            check_finish_unexpected_delta(net, ks, auuid, is_sub, next);
          }
        }
      });
    }
  });
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZOOR']={} : exports);

