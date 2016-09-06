"use strict";

var ZRAD, ZPio, ZMerge, ZConv, ZXact, ZDoc, ZOplog, ZDelt, ZCLS, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZRAD   = require('./zremote_apply_delta');
  ZPio   = require('./zpio');
  ZMerge = require('./zmerge');
  ZConv  = require('./zconvert');
  ZXact  = require('./zxaction');
  ZDoc   = require('./zdoc');
  ZOplog = require('./zoplog');
  ZDelt  = require('./zdeltas');
  ZCLS   = require('./zcluster');
  ZQ     = require('./zqueue');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var DefaultTrimSize = 1;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function get_crdt_array_len(clm) {
  var len = 0;
  for (var i = 0; i < clm.length; i++) {
    if (!clm[i]["X"]) len += 1;
  }
  return len;
}

function get_clm_min_json(clm) {
  for (var i = 0; i < clm.length; i++) {
    if (!clm[i]["X"] && !clm[i]["Z"]) return ZConv.CrdtElementToJson(clm[i]);
  }
  return null;
}

function get_clm_max_json(clm) {
  for (var i = clm.length - 1; i >= 0; i--) {
    if (!clm[i]["X"] && !clm[i]["Z"]) return ZConv.CrdtElementToJson(clm[i]);
  }
  return null;
}

function get_post_trim_min_json(clm, nbeg) {
  for (var i = nbeg; i < clm.length; i++) {
    if (!clm[i]["X"] && !clm[i]["Z"]) return ZConv.CrdtElementToJson(clm[i]);
  }
  return null;
}

function add_tombstone(tombstoned, op_path, clm, i) {
  var coppath = ZH.clone(op_path);
  coppath.push(ZOplog.CreateOpPathEntry(i, clm[i]));
  var entry   = ZOplog.CreateDeltaEntry(coppath, clm[i]);
  tombstoned.push(entry);
}

function create_delta_entry(val, op_path, fpath) {
  var member  = ZConv.ConvertJsonToCrdtType(val);
  member["_"] = fpath["_"];
  member["#"] = fpath["#"];
  member["@"] = fpath["@"];
  member["M"] = fpath["@"];
  member["S"] = fpath["S"];
  member["E"] = fpath["E"];
  var entry   = ZOplog.CreateDeltaEntry(op_path, member);
  return entry;
}

function get_metadata_field(f, crdt, op_path) {
  var crdtd = crdt._data.V;
  var clm   = ZMerge.FindNestedElementExact(crdtd, op_path);
  var lkey  = op_path[(op_path.length - 1)].K;
  var pno   = clm[lkey].E[f];
  return pno;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA-TYPE SORTING ---------------------------------------------------------

// NOTE: SecondaryDataCenters should persist a flag [delta->DO_DS]
//       that gets removed when the DO_DS Delta is processed
//       |-> On Geo-election, new primary should redo any outstanding DO_DS
function set_DO_DS(meta, dtd) {
  meta.DO_DS = true;
  if (!meta.DS_DTD) meta.DS_DTD = [];
  meta.DS_DTD.push(dtd);
}

function check_large_list(meta, clm, e, dtd) {
  if (!ZH.AmCentral)               return;
  if (!ZCLS.AmPrimaryDataCenter()) return;
  var psize = Number(e['PAGE-SIZE']) * 2; // NOTE: 2X -> SPLITS in 2
  var llen  = get_crdt_array_len(clm);
  ZH.l('ZTD.check_large_list: llen: ' + llen); ZH.p(e);
  if (llen >= psize) {
    ZH.l('ZTD.check_large_list: SET meta.DO_DS -> TRUE')
    set_DO_DS(meta, dtd);
  }
}

function check_list_max_size(meta, clm, e, dtd) {
  if (!ZH.AmCentral)               return;
  if (!ZCLS.AmPrimaryDataCenter()) return;
  ZH.l('ZTD.check_list_max_size'); //ZH.p(e);
  var max_size = Number(e['MAX-SIZE']);
  var trim     = Number(e["TRIM"]);
  if (!trim) trim = DefaultTrimSize;
  var llen     = get_crdt_array_len(clm);
  var msize    = Math.abs(max_size);
  if (llen > msize) {
    ZH.l('ZTD.check_list_max_size: SET meta.DO_DS -> TRUE')
    set_DO_DS(meta, dtd);
  }
}

function cmp_string_or_number(aval, bval) {
  var ajst = typeof(aval);
  var bjst = typeof(bval);
  if (ajst === bjst) {
    return (aval === bval) ? 0 : ((aval > bval) ? 1 : -1);
  } else {
    if (ajst === "string") return -1; // Strings BEFORE Numbers
    else                   return 1; 
  }
}

function cmp_key(a, b) {
  var akey = a.key;
  var bkey = b.key;
  return (akey === bkey) ? 0 : ((akey > bkey) ? 1 : -1);
}

function cmp_crdt_ordered_object(ace, bce) {
  if (ace["X"]) return -1;
  if (bce["X"]) return  1;
  var alen = Object.keys(ace.V).length;
  var blen = Object.keys(bce.V).length;
  if (alen !== blen) return ((alen > blen) ? 1 : -1);
  else {
    var as = [];
    for (var akey in ace.V) {
      as.push({key : akey, val : ace.V[akey]});
    }
    as.sort(cmp_key);
    var bs = [];
    for (var bkey in bce.V) {
      bs.push({key : bkey, val : bce.V[bkey]});
    }
    bs.sort(cmp_key);
    for (var i = 0; i < alen; i++) {
      var akey = as[i].key;
      var bkey = bs[i].key;
      if (akey !== bkey) return (akey > bkey) ? 1 : -1;
      var vace = as[i].val;
      var vbce = bs[i].val;
      var ret  = cmp_crdt_ordered_list(vace, vbce);
      if (ret !== 0) return ret;
    }
    return 0;
  }
}

function cmp_crdt_ordered_array(ace, bce) {
  if (ace["X"]) return -1;
  if (bce["X"]) return  1;
  var alen = ace.V.length;
  var blen = bce.V.length;
  if (alen !== blen) return ((alen > blen) ? 1 : -1);
  else {
    for (var i = 0; i < alen; i++) {
      var vace = ace.V[i];
      var vbce = bce.V[i];
      var ret  = cmp_crdt_ordered_list(vace, vbce);
      if (ret !== 0) return ret;
    }
    return 0;
  }
}

function cmp_crdt_ordered_list(ace, bce) {
  if (ace["X"]) return -1;
  if (bce["X"]) return  1;
  var atype = ace.T;
  var btype = bce.T;
  if (atype === "O") {
    if (btype === "O") return cmp_crdt_ordered_object(ace, bce);
    if (btype === "A") return -1; // Object BEFORE Array
    else               return 1;  // Objects & Arrays LAST
  }
  if (atype === "A") {
    if (btype === "A") return cmp_crdt_ordered_array(ace, bce);
    if (btype === "O") return 1;  // Object BEFORE Array
    else               return 1;  // Objects & Arrays LAST
  }
  if (btype === "O") return -1;   // Objects & Arrays LAST
  if (btype === "A") return -1;   // Objects & Arrays LAST
  var aval  = ZConv.CrdtElementToJson(ace);
  var bval  = ZConv.CrdtElementToJson(bce);
  return cmp_string_or_number(aval, bval);
}

function update_ordered_list_min_max(crdtd, op_path, clm) {
  var lkey     = op_path[(op_path.length - 1)].K;
  var cop_path = ZH.clone(op_path);
  cop_path.pop(); // FIND PARENT
  var pclm     = ZMerge.FindNestedElement(crdtd, cop_path);
  pclm[lkey].E["min"] = get_clm_min_json(clm);
  pclm[lkey].E["max"] = get_clm_max_json(clm);
}

exports.RunDatatypeFunctions = function(crdtd, meta, merge, dts) {
  ZH.l('RunDatatypeFunctions: dts'); //ZH.p(dts);
  for (var vname in dts) {
    var dtd     = dts[vname];
    var e       = dtd.E;
    var op_path = dtd.op_path;
    var clm     = ZMerge.FindNestedElement(crdtd, op_path);
    if (clm) {
      if      (e["MAX-SIZE"])    check_list_max_size(meta, clm, e, dtd);
      else if (e.LARGE === true) check_large_list(meta, clm, e, dtd);
      if (e.ORDERED) {
        clm.sort(cmp_crdt_ordered_list);
        update_ordered_list_min_max(crdtd, op_path, clm)
      }
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA-TYPE DELTA HELPERS ---------------------------------------------------

function do_trim_list(max_size, diff, clm, op_path, dfield) {
  ZH.l('do_trim_list: clen: ' + clm.length +
       ' max_size: ' + max_size + ' diff: ' + diff);
  if (max_size > 0) {
    for (var i = clm.length - 1; i >= 0; i--) {
      if (!clm[i]["X"]) {
        add_tombstone(dfield, op_path, clm, i);
        diff -= 1;
        if (diff === 0) return i - 1;
      }
    }
    return 0;
  } else {
    for (var i = 0; i < clm.length; i++) {
      if (!clm[i]["X"]) {
        add_tombstone(dfield, op_path, clm, i);
        diff -= 1;
        if (diff === 0) return i + 1;
      }
    }
    return 0;
  }
}

function trim_capped_list(clm, e, op_path, tombstoned, deleted) {
  var max_size = Number(e['MAX-SIZE']);
  var trim     = e["TRIM"] ? Number(e["TRIM"]) : DefaultTrimSize;
  var msize    = Math.abs(max_size);
  var llen     = get_crdt_array_len(clm);
  var diff     = llen - (msize - trim);
  if (diff <= 0) return; // NOT THIS LIST
  var oa       = e["ORDERED"] ? true : false;
  var dfield   = oa ? deleted : tombstoned
  do_trim_list(max_size, diff, clm, op_path, dfield);
}

function add_split_datatype(datatyped, op_path, nmin) {
  var vals = [];
  vals.push({"field" : "page_number", "operation" : "increment"});
  if (nmin) {
    vals.push({"field" : "min", "operation" : "set", "value" : nmin});
  }
  var entry = ZOplog.CreateDeltaEntry(op_path, vals);
  datatyped.push(entry);
}

function split_large_list(clm, e, f, op_path, splitted, datatyped, metadatas) {
  var trim  = Number(e['PAGE-SIZE']);
  var psize = (trim * 2) * -1; // TRIM FROM TOP OF ARRAY
  var llen  = get_crdt_array_len(clm);
  var msize = Math.abs(psize);
  var diff  = llen - (msize - trim);
  if (llen < psize) return; // NOT THIS LIST
  var nbeg  = do_trim_list(psize, diff, clm, op_path, splitted);
  var nmin  = get_post_trim_min_json(clm, nbeg);
  add_split_datatype(datatyped, op_path, nmin);
  metadatas.push({F : f, E : e});
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MAX-SIZE DELTA ------------------------------------------------------------

// NOTE calls "next(null, CRDT)"
function do_send_ds_delta(net, ks, md, crdt, dfields, next) {
  for (var i = 0; i < dfields.splitted.length; i++) {
    dfields.deleted.push(dfields.splitted[i]); // COMBINE DELETED & SPLITTED
  }
  //ZH.l('DS_DELTA TOMBSTONEDS'); ZH.p(tombstoned);
  var ncnt  = dfields.tombstoned.length + dfields.deleted.length;
  var cmeta = crdt._meta;
  ZXact.CreateAutoDentry(net, ks, cmeta, ncnt, false,
  function(aerr, dentry) {
    if (aerr) next(aerr, null);
    else {
      dentry.delta.tombstoned = dfields.tombstoned;
      dentry.delta.deleted    = dfields.deleted;
      dentry.delta.datatyped  = dfields.datatyped;
      dentry.delta._meta.AUTO = true;
      var avrsn               = dentry.delta._meta.author.agent_version;
      ZH.l('do_send_ds_delta: K: ' + ks.kqk + ' AV(C): ' + avrsn);
      //ZH.e('AUTO-DENTRY'); ZH.e(dentry);
      var net   = ZH.CreateNetPerRequest(ZH.Central);
      ZRAD.ProcessAutoDelta(net, ks, md, dentry, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LARGE-LIST DELTA ----------------------------------------------------------

function add_added_object(full_splitted, op_path, j) {
  var cop_path = [];
  for (var i = 0; i <= j; i++) {
    cop_path.push(op_path[i]);
  }
  var val   = {}; // EMPTY OBJECT
  var fel   = cop_path.length - 1;
  var fpath = cop_path[fel];
  var entry = create_delta_entry(val, cop_path, fpath);
  full_splitted.push(entry);
}

function add_added_array(full_splitted, op_path, metadata) {
  var val   = {"_data" : [],
               "_type" : metadata.F, "_metadata" : {}}; // EMPTY DT
  for (var k in metadata.E) val._metadata[k] = metadata.E[k];
  val._metadata.internal = true; // INTERNAL: unsupported "page_number" field
  var fel   = op_path.length - 1;
  var fpath = op_path[fel];
  var entry = create_delta_entry(val, op_path, fpath);
  full_splitted.push(entry);
}

// NOTE: ideally new page's id would INSURE it was on same machine main-page
function create_sub_page_id(meta, op_path, pno) {
  var oid    = meta._id;
  var pmpath = ZH.PathGetDotNotation(op_path);
  var nid    =  oid + "|" + pmpath + "|" + pno;
  return nid;
}

function do_single_split_delta(net, md, crdt, splitted,
                               mentry, metadata, next) {
  for (var i = 0; i < splitted.length; i++) {
    delete(splitted[i]["V"]["Z"]); // NO SLEEPERS IN SPLIT-DELTAS
  }
  metadata.E.min = get_clm_min_json(splitted); // SLEEPERS can change MIN/MAX
  metadata.E.max = get_clm_max_json(splitted);
  var full_splitted = [];
  var op_path       = mentry.op_path;
  if (op_path.length > 1) { // Sub-path -> nested OBJECTs
    for (var j = 0; j < (op_path.length - 1); j++) {
      add_added_object(full_splitted, op_path, j);
    }
  }
  add_added_array(full_splitted, op_path, metadata);
  for (var i = 0; i < splitted.length; i++) { // APPEND splitted
    full_splitted.push(splitted[i]);
  }
  var cmeta = crdt._meta;
  var f     = "page_number";
  var pno   = get_metadata_field(f, crdt, op_path);
  var nid   = create_sub_page_id(cmeta, op_path, pno);       // NEW ID
  var nks   = ZH.CompositeQueueKey(cmeta.ns, cmeta.cn, nid); // NEW KS
  var nmd   = {subs : md.subs};
  var ncnt  = full_splitted.length;
  ZXact.CreateAutoDentry(net, nks, cmeta, ncnt, false,
  function(aerr, dentry) {
    if (aerr) next(aerr, null);
    else {
      var delta          = dentry.delta;
      var meta           = delta._meta;
      meta._id           = nid;           // NEW ID
      meta.initial_delta = true;          // INITIAL DELTA
      meta.GC_version    = 0;             // INITIAL DELTA
      delta.added        = full_splitted; // ADD SPLITTED
      var avrsn          = meta.author.agent_version;
      ZH.l('do_split_delta: K: ' + nks.kqk + ' AV(C): ' + avrsn);
      //ZH.e('SPLIT-DENTRY'); ZH.e(dentry);
      var net = ZH.CreateNetPerRequest(ZH.Central);
      // NOTE: SAFE to process INTIAL(AUTO)DELTA on NOT KEY-MASTER
      ZRAD.ProcessAutoDelta(net, nks, nmd, dentry, next);
    }
  });
}

function loop_do_split_delta(net, md, crdt, splitted,
                             datatyped, metadatas, next) {
  if (datatyped.length === 0) next(null, null);
  else {
    var mentry   = datatyped.shift();
    var metadata = metadatas.shift();
    do_single_split_delta(net, md, crdt, splitted, mentry, metadata,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        setImmediate(loop_do_split_delta, net,
                     md, crdt, splitted, datatyped, metadatas, next);
      }
    });
  }
}

// NOTE: no 'ks' argument -> 'nks' gets created from crdt
function do_split_delta(net, md, crdt, splitted, datatyped, metadatas, next) {
  var cdatatyped = ZH.clone(datatyped);
  loop_do_split_delta(net, md, crdt, splitted, cdatatyped, metadatas, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DO_DS DELTA ---------------------------------------------------------------

// NOTE calls "next(null, CRDT)"
exports.SendDSdelta = function(net, ks, md, crdt, ds_dtd, next) {
  if (!ZCLS.AmPrimaryDataCenter()) return next(null, null);
  ZH.l('ZDT.SendDSdelta: K: ' + ks.kqk);
  md.ocrdt       = ZH.clone(crdt); // ZRAD.ProcessAutoDelta() on THIS CRDT
  var ccrdt      = ZH.clone(crdt);
  //ZH.l('DTD'); ZH.p(ccrdt._meta.DS_DTD);
  var crdtd      = ZH.clone(ccrdt._data.V);
  var dfields    = {tombstoned : [], deleted   : [],
                    splitted   : [], datatyped : []};
  var metadatas  = [];
  for (var i = 0; i < ds_dtd.length; i++) {
    var dtd     = ds_dtd[i];
    var f       = dtd.F;
    var e       = dtd.E;
    var op_path = dtd.op_path;
    var clm     = ZMerge.FindNestedElement(crdtd, op_path);
    if (e["MAX-SIZE"]) {
      trim_capped_list(clm, e, op_path,
                       dfields.tombstoned, dfields.deleted);
    } else if (e["LARGE"]) {
      split_large_list(clm, e, f, op_path,
                       dfields.splitted, dfields.datatyped, metadatas);
    }
  }
  if (dfields.splitted.length === 0) { // ONLY e['MAX-SIZE'] TRIMs
    do_send_ds_delta(net, ks, md, crdt, dfields, next);
  } else {                     // e['LARGE'] TRIMS (also)
    do_split_delta(net, md, crdt, dfields.splitted,
                   dfields.datatyped, metadatas,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        do_send_ds_delta(net, ks, md, crdt, dfields, next);
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZMerge.apply_delta
exports.CmpCrdtOrderedList = function(ace, bce) {
  return cmp_crdt_ordered_list(ace, bce);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZDT']={} : exports);

