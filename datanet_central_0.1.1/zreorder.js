
var ZMerge = require('./zmerge');
var ZGC    = require('./zgc');
var ZH     = require('./zhelper');

function remove_tombstones(cval) {
  for (var i = 0; i < cval.length; i++) {
    var child = cval[i];
    delete(child.X);
    delete(child.A);
  }
}

function do_apply_gc_as_tombstones(cval, tmbs) {
  ZH.l('do_apply_gc_as_tombstones');
  for (var i = 0; i < cval.length; i++) {
    var child = cval[i];
    var cid   = ZGC.GetTombstoneID(child);
    if (ZH.IsDefined(tmbs[cid])) {
      var tmb    = tmbs[cid];
      var ttype  = tmb[3];
      child["X"] = true;
      if (ttype === "A") child["A"] = true;
    }
  }
}

function apply_gc_as_tombstones(crdtd, gcs) {
  var cval  = crdtd.V;
  var ctype = crdtd.T;
  if (ctype === "A") {
    var pid  = ZGC.GetArrayID(crdtd);
    var tmbs = gcs.tombstone_summary[pid];
    if (tmbs) {
      do_apply_gc_as_tombstones(cval, tmbs);
    }
    for (var i = 0; i < cval.length; i++) {
      var child = cval[i];
      apply_gc_as_tombstones(child, gcs);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      var child = cval[ckey];
      apply_gc_as_tombstones(child, gcs);
    }
  }
}

function do_add_new_elements_and_remove_tombstones(crdtd, cval, aels) {
  ZH.l('do_add_new_elements_and_remove_tombstones');
  for (var i = 0; i < aels.length; i++) {
    var ael = aels[i];
    cval.push(ael);
  }
  ZMerge.DoArrayMerge(crdtd, cval, false);
  remove_tombstones(cval);
}

function add_new_elements_and_remove_tombstones(crdtd, nels) {
  var cval  = crdtd.V;
  var ctype = crdtd.T;
  if (ctype === "A") {
    var pid  = ZGC.GetArrayID(crdtd);
    var aels = nels[pid];
    if (aels) {
      do_add_new_elements_and_remove_tombstones(crdtd, cval, aels);
    }
    for (var i = 0; i < cval.length; i++) {
      var child = cval[i];
      add_new_elements_and_remove_tombstones(child, nels);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      var child = cval[ckey];
      add_new_elements_and_remove_tombstones(child, nels);
    }
  }
}

function cmp_array(oa, na) {
  if (oa.length !== na.length) return 1;
  for (var i = 0; i < oa.length; i++) {
    if (oa[i] !== na[i]) return 1;
  }
  return 0;
}

function cmp_reorder(oreo, nreo) {
  if (cmp_array(oreo[0], nreo[0])) return 1; // S
  if (oreo[1] !== nreo[1])         return 1; // LHN._
  if (oreo[2] !== nreo[2])         return 1; // LHN.#
}

function cmp_tombstone(otmb, ntmb) {
  if (cmp_reorder(otmb, ntmb)) return 1;
  if (otmb[3] !== ntmb[3])     return 1; // TOMBSTONE-TYPE
}

function diff_gc_summary(ogcs, ngcs) {
  var d      = {gcv : ogcs.gcv};
  var ngcs   = ZH.clone(ngcs); // NOTE: gets modified
  var otsumm = ogcs.tombstone_summary;
  var ntsumm = ngcs.tombstone_summary;
  for (var pid in otsumm) {
    var otmbs = otsumm[pid];
    var ntmbs = ntsumm[pid];
    for (var tid in otmbs) {
      var otmb = otmbs[tid];
      var ntmb = ntmbs[tid];
      if (!cmp_tombstone(otmb, ntmb)) delete(ntmbs[tid]);
    }
    var cnt = Object.keys(ntmbs).length;
    if (cnt === 0) delete(ntsumm[pid]);
  }
  d.tombstone_summary = ntsumm;

  var olhnr = ogcs.lhn_reorder;
  var nlhnr = ngcs.lhn_reorder;
  for (var tid in olhnr) {
    var oreo = olhnr[tid];
    var nreo = nlhnr[tid];
    if (!cmp_reorder(oreo, nreo)) delete(nlhnr[tid]);
  }
  d.lhn_reorder = nlhnr;

  var oulhnr = ogcs.undo_lhn_reorder;
  var nulhnr = ngcs.undo_lhn_reorder;
  for (var tid in oulhnr) {
    var oureo = oulhnr[tid];
    var nureo = nulhnr[tid];
    if (!cmp_reorder(oureo, nureo)) delete(nulhnr[tid]);
  }
  d.undo_lhn_reorder = nulhnr;
  var ntsumm         = Object.keys(d.tombstone_summary).length;
  var nlreo          = Object.keys(d.lhn_reorder).length;
  var nulreo         = Object.keys(d.undo_lhn_reorder).length;
  d.skip             = (ntsumm === 0) && (nlreo === 0) && (nulreo === 0);
  return d;
}

exports.CreateCentralReorderGCSummaries = function(ks, crdt, agcsumms, nels) {
  ZH.l('ZREO.CreateCentralReorderGCSummaries: K: ' + ks.kqk);
  var min_gcv = agcsumms[0].gcv;
  var md      = {gcv : min_gcv};
  var op      = "UpdateLeftHandNeighbor";
  var edata   = {md : md};
  var pc      = ZH.InitPreCommitInfo(op, ks, null, null, edata);
  pc.ncrdt    = crdt;

  var crdtd   = crdt._data;
  // STEP ONE: REWIND CRDT TO LOWEST GCV
  for (var i = (agcsumms.length - 1); i >= 0; i--) {
    var gcs = agcsumms[i];
    ZMerge.DoRewindCrdtGCVersion(crdtd, gcs);
  }

  // STEP TWO: ADD NEW ELEMENTS AND REMOVE TOMBSTONES
  add_new_elements_and_remove_tombstones(crdtd, nels);

  var ngcsumms = [];
  for (var i = 0; i < agcsumms.length; i++) {
    var gcs   = agcsumms[i];
    var gcv   = gcs.gcv;
    // STEP THREE: FOR EACH GC_SUMMARY: APPLY GCS.T_SUMMARY AS TOMBSTONES
    apply_gc_as_tombstones(crdtd, gcs);

    // STEP FOUR: GET GARBAGE COLLECTION (WITH NEW ELEMENTS PRESENT)
    var ngcs  = ZGC.DoGetGarbageCollection(gcv, crdt);
    ngcsumms.push(ZH.clone(ngcs));
    
    // STEP FIVE: DO GARBAGE COLLECTION (CLEAN UP FOR NEXT LOOP)
    var tsumm = ngcs.tombstone_summary;
    var lhnr  = ngcs.lhn_reorder;
    md.ocrdt  = pc.ncrdt; // ZGC.DoGarbageCollection on THIS CRDT
    ZGC.DoGarbageCollection(pc, gcv, tsumm, lhnr); // NOTE: can NOT FAI
  }

  // STEP SIX: DIFF OLD & NEW GC-SUMMARIES
  var dgcs = [];
  for (var i = 0; i < agcsumms.length; i++) {
    var ogcs = agcsumms[i];
    var ngcs = ngcsumms[i];
    var d    = diff_gc_summary(ogcs, ngcs);
    dgcs.push(d);
  }
  return dgcs;
}

