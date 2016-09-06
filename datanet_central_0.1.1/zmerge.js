"use strict";

var ZConv, ZDoc, ZDelt, ZOplog, ZAD, ZREO, ZOOR, ZDT, ZGC, ZDS, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZConv  = require('./zconvert');
  ZDoc   = require('./zdoc');
  ZDelt  = require('./zdeltas');
  ZOplog = require('./zoplog');
  ZAD    = require('./zapply_delta');
  ZREO   = require('./zreorder');
  ZOOR   = require('./zooo_replay');
  ZDT    = require('./zdatatypes');
  ZGC    = require('./zgc');
  ZDS    = require('./zdatastore');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

var DeepDebugContent        = true; //TODO FIXME HACK
var DeepDebugArrayHeartbeat = true; //TODO FIXME HACK
var DebugUpdateLHN          = true; //TODO FIXME HACK

/*
  TODO Handle clock-skew issues (need GlobalTime protocol)
  E.g.: If a delta comes in with dates very far in the past,
        cmp_child_member() will push them to the front of a CRDT_ARRAY
  SOLUTION: Central should baseline "S"s and propogate any "S" changes
*/

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function debug_element_value(field, el, ndelt, prfx) {
  ZH.l(prfx + ' NAME: ' + field +
       ' [D:' + ndelt + ',P:' + el.P + ',N:' + el.N + ']');
}

function add_merge_overwritten_error(merge, dsection, pmpath) {
  var merr = 'Section: ' + dsection + ' Field: ' + pmpath +
             ' has been overwritten';
  ZH.l('MERGE_CRDT: ' + merr);
  merge.errors.push(merr);
}
function add_merge_not_found_error(merge, dsection, pmpath) {
  var merr = 'Section: ' + dsection + ' Path: ' + pmpath +
             ' key not found - possible overwritten';
  ZH.l('MERGE_CRDT: ' + merr);
  merge.errors.push(merr);
}

function full_member_match(ma, mb) {
  return (ma["_"] === mb["_"] &&
          ma["#"] === mb["#"] &&
          ma["@"] === mb["@"]);
}

function insert_overwrite_element_in_array(clm, el) {
  for (var j = 0; j < clm.length; j++) {
    if (full_member_match(el, clm[j])) {
      clm[j] = el;
      return;
    }
  }
  clm.push(el);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MERGE ARRAY INSERTS -------------------------------------------------------

var DebugArrayMerge = false;

function tie_breaker_child_member(da, db) {
  if (da["_"] !== db["_"]) return (da["_"] > db["_"]) ?  1 : -1;
  else                     return (da["#"] > db["#"]) ?  1 : -1;
  throw(new Error("LOGIC(tie_breaker_child_member)"));
}

function unroll_s(s) {
  var u = [];
  for (var i = 0; i < s.length; i++) {
    var e = s[i];
    if (typeof(e) === 'number') {
      u.push(e);
    } else { //  NEXTED
      var iu = unroll_s(e);
      for (var j = 0; j < iu.length; j++) {
        u.push(iu[j]);
      }
    }
  }
  return u;
}

// DESC for S[] values, ASC for array-length
function cmp_s(usa, usb) {
  for (var i = 0; i < usa.length; i++) {
    if (i === usb.length) return 1; // ASC
    var sa = usa[i];
    var sb = usb[i];
    if (sa !== sb) return (sa > sb) ? -1 : 1; // DESC
  }
  var susa = usa.length;
  var susb = usb.length;
  return (susa === susb) ? 0 : ((susa > susb) ? 1 : -1); // ASC
}

// Sort to CreationTimestamp[S](DESC), AUUID[_], DELTA_VERSION[#]
function cmp_child_member(ca, cb) {
  var da  = ca.data;
  var db  = cb.data;
  var usa = unroll_s(da["S"]);
  var usb = unroll_s(db["S"]);
  var rs  = cmp_s(usa, usb);
  if (rs) return rs;
  else    return tie_breaker_child_member(da, db);
}

// LHN[0,0] is sorted BEFORE other roots
function cmp_root_child_member(ca, cb) {
  var da    = ca.data;
  var db    = cb.data;
  var roota = ((da["<"]["_"] === 0) && (da["<"]["#"] === 0));
  var rootb = ((db["<"]["_"] === 0) && (db["<"]["#"] === 0));
  if ((!roota && !rootb) || (roota && rootb)) { // NEITHER OR BOTH ROOT
    return cmp_child_member(ca, cb);
  } else {
    return rootb ? 1 : -1;
  }
}

function create_parent(data) {
  return {data : data, child : null};
}

function create_ll_node(mbrs) {
  return {members : mbrs};
} 

function get_root_ll_node(carr) {
  var roots = []
  var tor   = [];
  for (var i = 0; i < carr.length; i++) {
    var ma  = carr[i];
    var lhn = ma["<"];
    var hit = false;
    if (lhn["_"] === 0 && lhn["#"] === 0) hit = false;
    else {
      for (var j = 0; j < carr.length; j++) {
        if (i == j) continue;
        var mb = carr[j];
        if (lhn["_"] === mb["_"] && lhn["#"] === mb["#"]) {
          hit = true;
          break;
        }
      }
    }
    if (!hit) {
      roots.push(create_parent(ma));
      tor.push(i);
    }
  }
  for (var i = tor.length - 1; i >= 0; i--) { 
    carr.splice(tor[i], 1);
  }
  return create_ll_node(roots);
}

function assign_children(par, carr) {
  var children = [];
  var tor      = [];
  var pdata    = par.data;
  for (var i = 0; i < carr.length; i++) {
    var ma  = carr[i];
    var lhn = ma["<"];
    if (pdata["_"] === lhn["_"] && pdata["#"] === lhn["#"]) {
      children.push(create_parent(ma));
      tor.push(i);
    }
  }
  for (var i = tor.length - 1; i >= 0; i--) {
    carr.splice(tor[i], 1);
  }
  if (children.length) {
    children.sort(cmp_child_member);
    par.child = create_ll_node(children);
    for (var i = 0; i < par.child.members.length; i++) {
      var npar = par.child.members[i];
      assign_children(npar, carr);
    }
  }
}

function create_ll_from_crdt_array(carr) {
  var roots = get_root_ll_node(carr);
  roots.members.sort(cmp_root_child_member);
  if (DebugArrayMerge) { ZH.l('roots'); ZH.p(roots); }
  for (var i = 0; i < roots.members.length; i++) {
    var par = roots.members[i];
    assign_children(par, carr);
  }
  return roots;
}

function flatten_llca(sarr, child) {
  for (var i = 0; i < child.members.length; i++) {
    var par = child.members[i];
    sarr.push(par.data);
    if (par.child) {
      flatten_llca(sarr, par.child);
    }
  }
}

function do_array_merge(pclm, carr) {
  var llca      = create_ll_from_crdt_array(carr);
  pclm.V.length = 0;
  flatten_llca(pclm.V, llca);
}

function array_merge(pclm, carr, aval) {
  var oa = ZH.IsOrderedArray(pclm);
  if (oa) {
    pclm.V.push(aval);
    return 0;
  } else {
    carr.push(aval);
    do_array_merge(pclm, carr)
    var hit = 0;
    for (var i = 0; i < pclm.V.length; i++) {
      var ma = pclm.V[i];
      if (aval["_"] === ma["_"] && aval["#"] === ma["#"]) return hit;
      if (!ma["X"]) hit += 1; // Ignore Tombstones
    }
    throw(new Error('LOGIC(array_merge)'));
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD DATATYPE OPERATION METADATA -------------------------------------------

function add_dts(dts, pclm, entry, op_path) {
  var f = pclm.F;
  if (f) {
    var coppath = ZH.clone(op_path);
    coppath.pop();
    var ppath   = ZH.PathGetDotNotation(coppath);
    if (!dts[ppath]) {
      dts[ppath] = {F: f, E : pclm.E, op_path : coppath};
    }
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIND ELEMENTS -------------------------------------------------------------

function get_element_real_path(clm, op_path) {
  var ctype = "O"; // base CRDT data-type always an object
  var path  = '';
  for (var i = 0; i < op_path.length; i++) {
    var hit;
    if (ctype === "A") { // need to search array for op_path[_,#,@] match
      for (var j = 0; j < clm.length; j++) {
        if (full_member_match(op_path[i], clm[j])) {
          hit   = j;
          ctype = clm[j].T;
          clm   = clm[j].V;
          break;
        }
      }
    } else {
      hit       = op_path[i].K;
      var nextc = clm[hit];
      ctype     = nextc.T;
      clm       = nextc.V;
    }
    if (path.length) path += '.';
    path += String(hit);
  }
  return path;
}

function __find_nested_element(clm, op_path, match_full_path,
                               need_path, need_parent) {
  var ctype = "O"; // base CRDT data-type always an object
  var par   = clm;
  var lastc = clm;
  var oplen = match_full_path ? op_path.length : (op_path.length - 1); 
  for (var i = 0; i < oplen; i++) {
    lastc = clm;
    if (ctype === "A") { // need to search array for op_path[_,#,@] match
      var match = false;
      for (var j = 0; j < clm.length; j++) {
        if (full_member_match(op_path[i], clm[j])) {
          par   = clm[j];
          ctype = clm[j].T;
          clm   = clm[j].V;
          match = true;
          break;
        }
      }
      if (!match) return null; // Branch containing member is gone -> NO-OP
    } else {
      var nextc = clm[op_path[i].K];
      if (ZH.IsUndefined(nextc)) return null; // Member gone -> NO-OP
      // Check that nextc matches not just by name, but by [_,#,@] (LWW)
      if (!full_member_match(op_path[i], nextc)) return null;
      par   = nextc;
      ctype = nextc.T;
      clm   = nextc.V;
    }
  }
  return need_parent ? par   :
         need_path   ? lastc :
                       clm;
}

function find_nested_element_exact(clm, op_path) {
  return __find_nested_element(clm, op_path, true, true, false);
}

function find_nested_element_base(clm, op_path) {
  return __find_nested_element(clm, op_path, false, false, false);
}

function find_parent_element(clm, op_path) {
  return __find_nested_element(clm, op_path, false, false, true);
}

function find_nested_element_default(clm, op_path) {
  return __find_nested_element(clm, op_path, true, false, false);
}

function __get_last_key(clm, val, op_path, lww) {
  if (op_path.length === 1) { // Top-level is Object
    return op_path[0].K;
  }
  if (!Array.isArray(clm)) { // Non-array key match is Object
    return op_path[op_path.length - 1].K;
  }

  // modding Array: Find [_,#,@] match (not LHN match)
  for (var i = 0; i < clm.length; i++) {
    var lval = clm[i];
    if (full_member_match(val, lval)) return i;
  }
  return null;
}

function get_last_key_overwrite(clm, val, op_path) {
  return __get_last_key(clm, val, op_path, true);
}

function get_last_key_exact(clm, val, op_path) {
  return __get_last_key(clm, val, op_path, false);
}

// PURPOSE: get key respecting array tombstones
function get_last_key_by_name(clm, val, op_path) {
  if (op_path.length === 1) return op_path[0].K; // Top-level is Object
  if (!Array.isArray(clm))  return op_path[op_path.length - 1].K; // Object

  // Scan clm(array) if (clm[i][_,#]==val[<][_,#]) -> LeftHandNeighbor match
  for (var i = 0; i < clm.length; i++) {
    if (val["<"]["_"] === clm[i]["_"] && val["<"]["#"] === clm[i]["#"]) {
      // NOTE: "@"s do not have to match (array adding is about position)
      return i + 1; // add one to go the left of the match
    }
  }
  return 0; // NOTE: a HEAD insert ("<";[0,0]) never matches clm[] entries
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEEP DEBUG MERGE CRDT -----------------------------------------------------

function deep_debug_array_heartbeat_error(cval, ks, nvals, name, ts) {
  ZH.e('ERROR: (EDEEP) DeepDebugArrayHeartbeat: K: ' + ks.kqk +
       ' VAL: ' + ts + '-' + name);
  ZH.e('nvals'); ZH.e(nvals);
  ZH.e('cval');  ZH.e(cval);
}

function do_deep_debug_array_heartbeat(ks, crdtd) {
  var nvals = {};
  var cval  = crdtd.CONTENTS.V;
  for (var i = 0; i < cval.length; i++) {
    var child = cval[i];
    if (child.T != "S") continue; // ZMerge.RewindCrdtGCVersion() adds "N"s
    var val   = child.V;
    var res   = val.split('-');
    var ts    = res[0];
    var name  = res[1];
    if (!nvals[name]) nvals[name] = [];
    nvals[name].push(ts);
  }
  var ok = true;
  for (name in nvals) {
    var vals = nvals[name];
    var max  = 0;
    for (var i = 0; i < vals.length; i++) {
      var ts = vals[i];
      if (ts > max) max = ts;
      else {
        ok = false;
        deep_debug_array_heartbeat_error(cval, ks, nvals, name, ts);
      }
    }
  }
  if (ok) ZH.l('OK: DeepDebugArrayHeartbeat: K: '   + ks.kqk);
  else    ZH.l('FAIL: DeepDebugArrayHeartbeat: K: ' + ks.kqk);
}

function print_unrolled_s(us) {
  if (us.length === 1) return us[0];
  else {
    var ret = '';
    for (var i = 0; i < us.length; i++) {
      if (ret.length) ret += ','
      ret += us[i];
    }
    return '[' + ret + ']';
  }
}

function do_full_debug_array_heartbeat(cval) {
  for (var i = 0; i < cval.length; i++) {
    var child = cval[i];
    var v     = child["V"];
    var x     = child["X"] ? (child["A"] ? " A" : " X") : "";
    var d     = child["#"];
    var us    = unroll_s(child["S"]);
    var s     = print_unrolled_s(us);
    var lhnd  = child["<"]["#"];
    ZH.l("\tV: " + v + "\t(D: " + d + " S: " + s + " L: " + lhnd + ")" + x);
  }
}

function full_debug_array_heartbeat(crdtd) {
  var cval = crdtd.CONTENTS.V;
  do_full_debug_array_heartbeat(cval);
}

exports.DeepDebugMergeCrdt = function(meta, crdtd, is_reo) {
  var ks     = ZH.CompositeQueueKey(meta.ns, meta.cn, meta._id);
  var is_ahb = (ks.kqk === "production|statistics|ARRAY_HEARTBEAT") &&
               (crdtd && crdtd.CONTENTS && crdtd.CONTENTS.V);
  if (DeepDebugContent) {
    ZH.l('DeepDebugContent');
    if (is_ahb) {
      full_debug_array_heartbeat(crdtd);
    } else {
      var json = ZConv.ConvertCrdtDataValue(crdtd);
      ZH.p(json);
    }
  }
  if (DeepDebugArrayHeartbeat && is_ahb) {
    if (!is_reo) { // REORDER_DELTAS are TEMPORARILY NOT-YET-ORDERED
      do_deep_debug_array_heartbeat(ks, crdtd);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MERGE DELTA INTO CRDT -----------------------------------------------------

// NOTE: equivalent CREATION-TIME is rare
function get_tiebraker_add_operation(oval, aval) {
  var ok = true;
  if      (oval["@"] >   aval["@"]) ok = false;
  else if (oval["@"] === aval["@"]) {      // EQUIVALENT CREATION-TIME
    if (oval["_"] > aval["_"]) ok = false; // TIE-BREAKER (NOTE: EQUAL IS OK)
  }
  return ok;
}

function emaciate_tombstone(clm, lkey) { //TODO -> makes debugging harder
  // NOTE: ONLY: [_,#,S,@,<] are needed
  //delete(clm[lkey].V); delete(clm[lkey].T);
}

exports.ApplyDeltas = function(ocrdt, delta, is_reo) {
  ZH.l('ZMerge.ApplyDeltas: IS_REO: ' + is_reo);
  var crdtd = ocrdt._data ? ocrdt._data.V : null;
  var cmeta = ocrdt._meta;
  var merge = {post_merge_deltas : [],
               crdtd             : ZH.clone(crdtd),
               errors            : []};
  var dts   = {};

  // 1.) Modified[]
  for (var i = 0; i < delta.modified.length; i++) {
    var mentry  = delta.modified[i];
    var mval    = mentry.V;
    var mtype   = mval.T;
    var op_path = mentry.op_path;
    var pmpath  = ZH.PathGetDotNotation(op_path);
    var is_iop  = ZH.IsDefined(mval.V.D);
    var clm     = find_nested_element_base(merge.crdtd, op_path);
    if (clm === null) {
      add_merge_not_found_error(merge, 'modified', pmpath);
      continue; // Key not found & LWW fail -> NO-OP
    }
    var pclm    = find_parent_element(merge.crdtd, op_path);
    var lkey    = is_iop ? get_last_key_exact    (clm, mval, op_path) :
                           get_last_key_overwrite(clm, mval, op_path);
    if (lkey === null) {
      add_merge_overwritten_error(merge, 'modified', pmpath);
      continue;
    }
    add_dts(dts, pclm, mentry, op_path);
    var rpath      = get_element_real_path(merge.crdtd, op_path);
    var c_isnum    = ZH.IsDefined(clm[lkey].V.P);
    if (c_isnum && is_iop && mtype === "N") {
      // Number modifications are done via deltas (INCR or DECR)
      var ndelt      = mval.V.D;
      var positive   = clm[lkey].V.P;             // old-value.P
      var negative   = clm[lkey].V.N;             // old-value.N
      if (ndelt > 0) positive = positive + ndelt; // add delta to old-value.P
      else           negative = negative - ndelt; // add delta to old-value.N
      debug_element_value(lkey, clm[lkey].V, ndelt, 'PRE INCR: ');
      clm[lkey].V.P  = positive;                  // set new-value.P
      clm[lkey].V.N  = negative;                  // set new-value.N
      debug_element_value(lkey, clm[lkey].V, ndelt, 'POST INCR: ');
      var result     = (positive - negative);
      var pme        = {op     : 'increment', path : rpath, value : ndelt,
                        result : result};
      merge.post_merge_deltas.push(pme);
    } else { // All non-numeric modifications OVERWRITE the previous value
      clm[lkey] = mval; // SET NEW VALUE
      var val   = ZConv.CrdtElementToJson(mval);
      var pme   = {op : 'set', path : rpath, value : val};
      merge.post_merge_deltas.push(pme);
    }
  }

  // 2.) Tombstoned[]
  for (var i = 0; i < delta.tombstoned.length; i++) {
    var tentry   = delta.tombstoned[i];
    var tval     = tentry.V;
    var op_path  = tentry.op_path;
    var pmpath   = ZH.PathGetDotNotation(op_path);
    var clm      = find_nested_element_base(merge.crdtd, op_path);
    if (clm === null) {
      add_merge_not_found_error(merge, 'tombstoned', pmpath);
      continue;
    }
    var lkey     = get_last_key_exact(clm, tval, op_path);
    if (lkey === null) {
      add_merge_overwritten_error(merge, 'tombstoned', pmpath);
      continue;
    }
    clm[lkey]["X"] = true;
    emaciate_tombstone(clm, lkey);
    var rpath      = get_element_real_path(merge.crdtd, op_path);
    var pme        = {op : 'delete', path : rpath};
    merge.post_merge_deltas.push(pme);
  }

  // 3.) Added[]
  for (var i = 0; i < delta.added.length; i++) {
    var aentry   = delta.added[i];
    var aval     = aentry.V;
    var atype    = aval.T;
    var op_path  = aentry.op_path;
    var pmpath   = ZH.PathGetDotNotation(op_path);
    var clm      = find_nested_element_base(merge.crdtd, op_path);
    if (clm === null) {
      add_merge_not_found_error(merge, 'added', pmpath);
      continue; // Key not found & LWW fail -> NO-OP
    }
    var pclm     = find_parent_element(merge.crdtd, op_path);
    var ctype    = Array.isArray(clm) ? "A" : "O";
    var isa      = (ctype === "A");
    var oa       = ZH.IsOrderedArray(pclm);
    var lkey     = oa ? 0 : get_last_key_by_name(clm, aval, op_path);
    if (!isa) { // SET [N,S,O] -> NOT INSERT [A]
      var oval = clm[lkey];
      if (oval) { // Added value was concurrently ADDED
        var ok = get_tiebraker_add_operation(oval, aval);
        if (!ok) {
          add_merge_overwritten_error(merge, 'added', pmpath);
          continue;
        }
      }
    }
    var pme = {};
    if (oa) { // CHECK FOR LARGE-LIST SLEEPER
      var oamin = ZH.GetOrderedArrayMin(pclm);
      if (oamin) { // not defined when empty
        var coamin = ZConv.ConvertJsonToCrdtType(oamin);
        if (ZDT.CmpCrdtOrderedList(aval, coamin) < 0) { // SLEEPER
          pme.late  = true;
          aval["Z"] = true;
        }
      }
    }
    add_dts(dts, pclm, aentry, op_path);
    if (isa) {
      var hit   = array_merge(pclm, clm, aval);
      clm       = pclm.V; // do_array_merge() rewrites pclm.V[]
      pme.op    = 'insert';
      pme.index = hit;
    } else {
      clm[lkey] = aval;
      pme.op    = 'set';
    }
    pme.path  = get_element_real_path(merge.crdtd, op_path);
    pme.value = ZConv.CrdtElementToJson(aval);
    merge.post_merge_deltas.push(pme);
  }

  // 4.) Deleted[]
  for (var i = 0; i < delta.deleted.length; i++) {
    var rentry  = delta.deleted[i];
    var rval    = rentry.V;
    var op_path = rentry.op_path;
    var pmpath  = ZH.PathGetDotNotation(op_path);
    var clm     = find_nested_element_base(merge.crdtd, op_path);
    if (clm === null) {
      add_merge_not_found_error(merge, 'deleted', pmpath);
      continue; // Key not found & LWW fail -> NO-OP
    }
    var lkey    = get_last_key_exact(clm, rval, op_path);
    if (lkey === null) {
      add_merge_overwritten_error(merge, 'deleted', pmpath);
      continue;
    }
    var rpath   = get_element_real_path(merge.crdtd, op_path);
    var ctype   = Array.isArray(clm) ? "A" : "O";
    if (ctype === "A") clm.splice(lkey, 1);
    else               delete(clm[lkey]);
    var pme     = {op : 'delete', path : rpath};
    merge.post_merge_deltas.push(pme);
  }

  // 5.) Datatyped[]
  for (var i = 0; i < delta.datatyped.length; i++) {
    var mentry  = delta.datatyped[i];
    var mval    = mentry.V;
    if (mval.length === 0) continue; // NO-OP
    var op_path = mentry.op_path;
    var clm     = find_nested_element_exact(merge.crdtd, op_path);
    var lkey    = op_path[(op_path.length - 1)].K;
    for (var j = 0; j < mval.length; j++) {
      var m     = mval[j];
      var fname = m.field;
      var dop   = m.operation;
      if (dop === "increment") {
        clm[lkey].E[fname] += 1;
      } else if (dop === "set") {
        var val = m.value;
        clm[lkey].E[fname] = val;
      }
    }
  }

  var ndts = Object.keys(dts).length;
  if (ndts !== 0) {
    ZDT.RunDatatypeFunctions(merge.crdtd, delta._meta, merge, dts)
  }

  exports.DeepDebugMergeCrdt(cmeta, merge.crdtd, is_reo);

  return ZH.clone(merge);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMPARE ZDOC AUTHORS ------------------------------------------------------

exports.CompareZDocAuthors = function(m1, m2) {
  var mdc1 = m1.document_creation;
  var mdc2 = m2.document_creation;
  ZH.l('CompareZDocAuthors: [' + mdc1["_"] + ', ' + mdc1["#"] + '] to' +
                          ' [' + mdc2["_"] + ', ' + mdc2["#"] + ']');
  if (mdc1["_"] === mdc2["_"] && mdc1["#"] === mdc2["#"]) return 0;
  else                                                    return 1;
}


//TODO START move to zgc.js

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FRESHEN HELPERS -----------------------------------------------------------

function create_array_element_from_tombstone(tid, tmb) {
  var s     = tmb[0];
  var us    = unroll_s(s);
  var ts    = us[0];
  var res   = tid.split('|');
  var duuid = Number(res[0]);
  var step  = Number(res[1]);
  var el    = {"T" : "N",                // DUMMY VALUE
               "V" : {"P" : 0, "N" : 0}, // DUMMY VALUE
               "_" : duuid,
               "#" : step,
               "@" : ts,
               "S" : s,
               "X" : true,
               "<" : {
                 "_" : tmb[1],
                 "#" : tmb[2]
               }
              };
  return el; // PSEUDO TOMBSTONE
}

exports.CheckApplyReorderElement = function(cval, reo) {
  var cid    = ZGC.GetTombstoneID(cval);
  var do_reo = reo[cid];
  if (do_reo) {
    cval["S"]      = do_reo[0];
    cval["<"]["_"] = do_reo[1];
    cval["<"]["#"] = do_reo[2];
  }
}

function apply_reorder(clm, reo) {
  for (var i = 0; i < clm.length; i++) {
    var cval = clm[i];
    exports.CheckApplyReorderElement(cval, reo);
  }
}

function apply_lhn_reorder(clm, ugcsumms) {
  var reo = ugcsumms.lhn_reorder;
  apply_reorder(clm, reo);
}

function apply_undo_lhn_reorder(clm, ugcsumms) {
  var ureo = ugcsumms.undo_lhn_reorder;
  apply_reorder(clm, ureo);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REWIND/FORWARD CRDT -------------------------------------------------------

function rewind_add_tombstones(clm, tmbs) {
  for (var tid in tmbs) {
    var tmb = tmbs[tid];
    var el  = create_array_element_from_tombstone(tid, tmb);
    insert_overwrite_element_in_array(clm, el);
  }
}

exports.DoRewindCrdtGCVersion = function(crdtd, ugcsumms) {
  var cval  = crdtd.V;
  var ctype = crdtd.T;
  if (ctype === "A") {
    var pid  = ZGC.GetArrayID(crdtd);
    var tmbs = ugcsumms.tombstone_summary[pid];
    if (tmbs) {
      rewind_add_tombstones(cval, tmbs);
      apply_undo_lhn_reorder(cval, ugcsumms);
      do_array_merge(crdtd, cval);
    }
    for (var i = 0; i < cval.length; i++) {
      var child = cval[i];
      exports.DoRewindCrdtGCVersion(child, ugcsumms);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      var child = cval[ckey];
      exports.DoRewindCrdtGCVersion(child, ugcsumms);
    }
  }
}

// NOTE: returns CRDT
exports.RewindCrdtGCVersion = function(ks, ocrdt, gcsumms) {
  var ngcs = Object.keys(gcsumms).length;
  if (ngcs === 0) return ocrdt;
  else {
    var min_gcv     = ZGC.GetMinGcv(gcsumms);
    ZH.l('ZMerge.RewindCrdtGCVersion: K: ' + ks.kqk + ' (MIN)GCV: ' + min_gcv);
    var ugcsumms    = ZGC.UnionGCVSummaries(gcsumms);
    var ncrdt       = ZH.clone(ocrdt);
    var crdtd       = ncrdt._data;
    exports.DoRewindCrdtGCVersion(crdtd, ugcsumms);
    var ngcv        = (min_gcv - 1); // REWIND -> BEFORE MIN_GCV
    var meta        = ncrdt._meta;
    meta.GC_version = ngcv;
    ZH.l('ZMerge.RewindCrdtGCVersion: (N)GCV: ' + ngcv);
    return ncrdt;
  }
}

function apply_agent_reorder_deltas(net, pc, ards, next) {
  if (ards.length === 0) next(null, null);
  else {
    var ard      = ards.shift();
    var rometa   = ard.rometa;
    var reorder  = ard.reorder;
    var rodentry = create_provisional_reorder_delta(rometa, reorder);
    ZOOR.ForwardCrdtGCVersionApplyReferenceDelta(net, pc, rodentry,
    function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(apply_agent_reorder_deltas, net, pc, ards, next);
    });
  }
}

// NOTE: next(null, OK);
function do_forward_crdt_gc_version(net, pc, gcs, next) {
  var ks    = pc.ks;
  var md    = pc.extra_data.md;
  var gcv   = gcs.gcv;
  var tsumm = gcs.tombstone_summary;
  var lhnr  = gcs.lhn_reorder;
  var ards  = gcs.agent_reorder_deltas ? gcs.agent_reorder_deltas : [];
  ZH.l('do_forward_crdt_gc_version: K: ' + ks.kqk + ' GCV: ' + gcv +
       ' #ARDS: ' + ards.length);
  md.ocrdt  = pc.ncrdt; // FORWARD-GC-VERSION on THIS CRDT
  var ok    = ZGC.DoGarbageCollection(pc, gcv, tsumm, lhnr);
  if (!ok) {
    ZH.e('FAIL: do_forward_crdt_gc_version: K: ' + ks.kqk + ' GCV: ' + gcv);
    ZOOR.SetGCWaitIncomplete(net, ks, function(serr, sres) {
      next(serr, false); // BAD -> STOP
    });
  } else {
    ZH.SetNewCrdt(pc, md, md.ocrdt, true);
    apply_agent_reorder_deltas(net, pc, ards, function(serr, sres) {
      if (serr) next(serr, null);
      else {
        ZOOR.CheckReplayOOODeltas(net, pc, false, true, function(serr, sres) {
          next(serr, true); // OK -> CONTINUE
        });
      }
    });
  }
}

function forward_crdt_gc_version(net, pc, agcsumms, next) {
  if (agcsumms.length === 0) next(null, true);
  else {
    var gcs = agcsumms.shift();
    do_forward_crdt_gc_version(net, pc, gcs, function(aerr, ok) {
      if (aerr) next(aerr, null);
      else {
        if (ok) setImmediate(forward_crdt_gc_version, net, pc, agcsumms, next);
        else    next(null, false);
      }
    });
  }
}

exports.ForwardCrdtGCVersion = function(net, pc, gcsumms, next) {
  var ks       = pc.ks;
  ZH.l('ZMerge.ForwardCrdtGCVersion: K: ' + ks.kqk);
  var agcsumms = [];
  for (var sgcv in gcsumms) {
    var gcs = gcsumms[sgcv];
    agcsumms.push(gcs);
  }
  forward_crdt_gc_version(net, pc, agcsumms, function(serr, ok) {
    if (serr) next(serr, null);
    else {
      if (!ok) next(null, null); // GC-WAIT_INCOMPLETE continues
      else     ZOOR.EndGCWaitIncomplete(net, ks, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FRESHEN CENTRAL DELTA -----------------------------------------------------

function cmp_gcsumm(a, b) {
  var a_gcv = a.gcv;
  var b_gcv = b.gcv;
  return (a_gcv === b_gcv) ? 0 : ((a_gcv > b_gcv) ? 1 : -1);
}

function freshen_central_delta(ks, dentry, ocrdt, gcsumms, gcv) {
  ZH.l('freshen_central_delta');
  var initial = dentry.delta._meta.initial_delta;
  var remove  = dentry.delta._meta.remove;
  if (initial || remove) return []; // NOTE: EMPTY REORDER[]
  var odentry = ZH.clone(dentry); // CLONE -> gets modified
  var odelta  = odentry.delta;
  var ocrdtd  = ZH.clone(ocrdt._data.V); // CLONE -> gets modified
  var nels    = {};
  for (var i = 0; i < odelta.added.length; i++) {
    var aentry  = odelta.added[i];
    var aval    = aentry.V;
    var oaval   = ZH.clone(aval);
    var lhn     = aval["<"];
    // NOT_ARRAY OR LHN[0,0] -> NO-OP
    if (!lhn || ((lhn["_"] === 0) && (lhn["#"] === 0))) continue;
    var op_path = aentry.op_path;
    var clm     = find_nested_element_base(ocrdtd, op_path);
    if (!clm) continue; // overwritten
    var pclm    = find_parent_element(ocrdtd, op_path);
    var lkey    = get_last_key_by_name(clm, aval, op_path);
    if (!lkey) {
      var pid = ZGC.GetArrayID(pclm);
      if (!nels[pid]) nels[pid] = [];
      nels[pid].push(aval);
    }
  }
  var cnt = Object.keys(nels).length;
  if (!cnt) return []; // NOTE: EMPTY REORDER[]
  else {
    var agcsumms = [];
    for (var gcv in gcsumms) {
      agcsumms.push(gcsumms[gcv]);
    }
    agcsumms.sort(cmp_gcsumm);
    return ZREO.CreateCentralReorderGCSummaries(ks, ocrdt, agcsumms, nels);
  }
}

exports.FreshenCentralDelta = function(net, ks, dentry, md, next) {
  var bgcv = ZH.GetDentryGCVersion(dentry);
  var egcv = md.gcv;
  ZH.l('ZMerge.FreshenCentralDelta: K: ' + ks.kqk +
       ' GCV:[' + bgcv + '-' + egcv + ']');
  ZGC.GetGCVSummaryRangeCheck(net.plugin, net.collections, ks, bgcv, egcv,
  function(gerr, gcsumms) {
    if (gerr) next(gerr, null);
    else {
      if (!gcsumms) { // GCV was STALE -> IGNORE REFERENCE_DELTA
        next(null, []); // NOTE: EMPTY REORDER[]
      } else {
        var ocrdt = ZH.clone(md.ocrdt); // NOTE: gets modified
        var dgcs  = freshen_central_delta(ks, dentry, ocrdt, gcsumms, egcv);
        next(null, dgcs);
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FRESHEN AGENT DELTA -------------------------------------------------------

function update_agent_delta_lhn(ugcsumms, aentry, pclm, clm) {
  var aval = aentry.V;
  var lhn  = aval["<"];
  var pid  = ZGC.GetArrayID(pclm);
  ZH.l('BEG: update_agent_delta_lhn: aval'); ZH.p(aval);
  var tmbs = ugcsumms.tombstone_summary[pid];
  if (!tmbs) {
    ZH.e('PARENT: ' + pid + ' NOT FOUND in TOMBSTONE SUMMARY');
    return false;
  } else {
    var tid = ZGC.GetTombstoneID(lhn);
    var tmb = tmbs[tid];
    if (!tmb) {
      ZH.e('ENTRY: LHN: ' + tid + ' NOT FOUND in TOMBSTONE SUMMARY');
      return false;
    } else {
      for (var tid in tmbs) { // Convert to array
        var tmb = tmbs[tid];
        var el  = create_array_element_from_tombstone(tid, tmb);
        insert_overwrite_element_in_array(clm, el);
      }
      apply_undo_lhn_reorder(clm, ugcsumms);
      do_array_merge(pclm, clm); // SORTS CLM to LHN
      clm = pclm.V;              // do_array_merge() rewrites pclm.V[]
      if (DebugUpdateLHN) do_full_debug_array_heartbeat(clm);
      return ZGC.AgentPostMergeUpdateLhn(aentry, clm);
    }
  }
}

function do_freshen_agent_delta(dentry, fcrdt, ugcsumms, gcv) {
  var rdentry = ZH.clone(dentry); // CLONE -> gets modified
  var odelta  = rdentry.delta;
  var initial = odelta._meta.initial_delta;
  var remove  = odelta._meta.remove;
  ZH.l('do_freshen_agent_delta: I: ' + initial + ' R: ' + remove);
  if (initial || remove) return rdentry;
  var fcrdtd  = ZH.clone(fcrdt._data.V); // CLONE -> gets modified
  for (var i = 0; i < odelta.added.length; i++) {
    var aentry  = odelta.added[i];
    var aval    = aentry.V;
    var lhn     = aval["<"];
    // NOT_ARRAY OR LHN[0,0] -> NO-OP
    if (!lhn || ((lhn["_"] === 0) && (lhn["#"] === 0))) continue;
    var op_path = aentry.op_path;
    var clm     = find_nested_element_base(fcrdtd, op_path);
    if (!clm) continue; // overwritten
    var pclm    = find_parent_element(fcrdtd, op_path);
    var lkey    = get_last_key_by_name(clm, aval, op_path);
    if (!lkey) {
      update_agent_delta_lhn(ugcsumms, aentry, pclm, clm);
    }
  }
  rdentry.delta._meta.GC_version = gcv;
  return rdentry;
}

exports.FreshenAgentDelta = function(net, ks, dentry, ocrdt, gcv, next) {
  var dgcv = ZH.GetDentryGCVersion(dentry);
  ZH.l('FreshenAgentDelta: K: ' + ks.kqk + ' DGCV: ' + dgcv + ' GCV: ' + gcv);
  if (dgcv >= gcv) next(null, dentry);
  else {
    ZGC.GetUnionedGCVSummary(net.plugin, net.collections, ks, dgcv, gcv,
    function(gerr, ugcsumms) {
      if (gerr) next(gerr, null);
      else {
        if (!ugcsumms) {
          ZH.e('FAIL: FreshenAgentDelta: K: ' + ks.kqk + ' DGCV: ' + dgcv);
          next(null, null);
        } else {
          var ndentry = do_freshen_agent_delta(dentry, ocrdt, ugcsumms, gcv);
          next(null, ndentry);
        }
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD REORDER DELTA TO REFERENCE DELTA --------------------------------------

function create_provisional_reorder_delta(rometa, reorder) {
  var rodentry = {delta : {_meta   : rometa,
                           reorder : reorder}
                 };
  return rodentry;
}

exports.AddReorderToReference = function(rodentry, rfdentry) {
  var ndentry = ZH.clone(rfdentry);
  var rometa  = rodentry.delta._meta;
  if (rometa.reference_ignore) {
    ZH.l('REFERENCE_IGNORE -> DO_IGNORE');
    ndentry.delta._meta.DO_IGNORE  = true; // Used in ZAD.do_apply_delta()
  } else {
    ndentry.delta.reorder          = ZH.clone(rodentry.delta.reorder);
    ndentry.delta._meta.REORDERED  = true; // Used in ZAD.do_apply_delta()
    // GC_Version used in REORDER-REMOVE
    ndentry.delta._meta.GC_version = rometa.GC_version;
  }
  return ndentry;
}


//TODO END move to zgc.js

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CREATE VIRGIN JSON FROM OPLOG ---------------------------------------------

/* NOTE: Used in AgentStatelessCommit() & AgentMemcacheCommit()
         ZMerge.CreateVirginJsonFromOplog() CASES:
           A.) SET       a.b.c.d 22   -> OBJECT-SET
           B.) SET       a.b.c.2 33   -> ARRAY-SET
           C.) INCR/DECR a.b.c.d 44   -> OBJECT-INCR
           D.) INSERT    a.b.c.d 1 55 -> ARRAY-INSERT
*/

exports.CreateVirginJsonFromOplog = function(ks, rchans, oplog) {
  var jdata = {_id       : ks.key,
               _channels : rchans};
  for (var i = 0; i < oplog.length; i++) {
    var op      = oplog[i];
    var opname  = op.name;
    var op_path = op.path;
    var keys    = ZH.ParseOpPath(op_path);
    if (opname === 'delete') { // NO-OP
    } else {                   // OP: [set, insert, increment, decrement]
      var lkey     = keys[(keys.length - 1)];
      var lk_isnan = isNaN(lkey);
      var jd       = jdata;
      if (keys.length > 2) { // e.g. 'a.b.c.d' -> {a:{b:{}}
        for (var j = 0; j < (keys.length - 2); j++) {
          var k = keys[j];
          jd[k] = {};
          jd    = jd[k];
        }
      }
      if (keys.length > 1) {
        var slkey = keys[(keys.length - 2)];
        if (!lk_isnan) jd[slkey] = []; // e.g. 'a.b.c.2 -> {a:{b:c:[]}} (B)
        else           jd[slkey] = {}; // e.g. 'a.b.c.d -> {a:{b:c:{}}} (A)
      }
      if (opname === "set") { // NO-OP (e.g. SET a.b.d.c 4) (A,B)
      } else if (opname === "increment" || opname === "decrement") {
        jd[lkey] = 0; // e.g. INCR a.b.c.d 5 -> {a:{b:c:{d:0}}} (C)
      } else { // OP: [insert]
        jd[lkey] = []; // e.g. INSERT a.b.c.d 1 6 -> {a:{b:c:{d:[]}}} (D)
      }
    }
  }
  return jdata;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SEPARATE CRDT, DIRECTORY, & MEMBERS ---------------------------------------

function add_present_members(pmbrs, mmap, dfield) {
  for (var i = 0; i < dfield.length; i++) {
    var mentry  = dfield[i];
    var mval    = mentry.V;
    var op_path = mentry.op_path;
    var pbeg    = op_path[0].K; // ROOT ELEMENT
    if (mmap[pbeg]) {
      pmbrs.push(pbeg);
      delete(mmap[pbeg]);
    }
  }
}

exports.GetMembersPresentInDelta = function(mbrs, delta) {
  var mmap  = {};
  for (var i = 0; i < mbrs.length; i++) mmap[mbrs[i]] = true;
  var pmbrs = [];
  add_present_members(pmbrs, mmap, delta.modified);
  add_present_members(pmbrs, mmap, delta.tombstoned);
  add_present_members(pmbrs, mmap, delta.added);
  add_present_members(pmbrs, mmap, delta.deleted);
  return pmbrs;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUMMARIZE CHANGES (plugins/memcache_agent.js)------------------------------

function summarize_write(summ, merge, dfield) {
  for (var i = 0; i < dfield.length; i++) {
    var mentry  = dfield[i];
    var mval    = mentry.V;
    var op_path = mentry.op_path;
    var pbeg    = op_path[0].K; // ROOT ELEMENT
    var clm     = find_nested_element_base(merge.crdtd, op_path);
    if (clm === null) {
      summ[pbeg] = null;
      continue;
    }
    var lkey    = get_last_key_exact(clm, mval, op_path);
    if (lkey === null) {
      summ[pbeg] = null;
      continue;
    }
    var found  = merge.crdtd[pbeg];
    summ[pbeg] = found ? found : null;
  }
}

exports.SummarizeRootLevelChanges = function(crdt, delta) {
  var summ  = {};
  var crdtd = crdt._data.V;
  var merge = {crdtd : crdtd};
  summarize_write(summ, merge, delta.modified);
  summarize_write(summ, merge, delta.tombstoned);
  summarize_write(summ, merge, delta.added);
  summarize_write(summ, merge, delta.deleted);
  var asumm = [];
  for (var k in summ) {
    asumm.push({root : k, value : summ[k]});
  }
  return asumm;
}

exports.CreateSeparateDeltaFromDirectory = function(crdt, mbrs) {
  var crdtd = crdt._data.V;
  var delta = ZH.InitDelta(null); // NO META
  for (var i = 0; i < mbrs.length; i++) {
    var field   = mbrs[i]; // ROOT LEVEL MEMBER
    var cval    = crdtd[field];
    var op_path = [];
    op_path.push(ZOplog.CreateOpPathEntry(field, cval)); // ROOT LEVEL
    delta.added.push(ZOplog.CreateDeltaEntry(op_path, cval));
  }
  return delta;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COALESCE DELTAS -----------------------------------------------------------

function cmp_op_path_entry(a, b) {
  return (a["_"] === b["_"] && a["#"] === b["#"]) ? 0 : 1;
}

function cmp_op_path(iopp, jopp) {
  if (iopp.length !== jopp.length) return 1;
  for (var i = 0; i < iopp.length; i++) {
    if (cmp_op_path_entry(iopp[i], jopp[i])) return 1;
  }
  return 0;
}

exports.CoalesceAgentDeltas = function(ndentry, odentry, cdentry) {
  var odelta  = odentry.delta;
  var cdelta  = cdentry.delta;
  if (exports.CompareZDocAuthors(odelta._meta, cdelta._meta)) {
    ZH.l('ZMerge.CoalesceAgentDeltas: FAIL (AUTHOR MISMATCH)');
    return false;
  }

  //TODO the cases that can be COALESCED can be GREATLY expanded
  // NOTE: for now, we are only dealing w/ PURE modified[] coalescing
  if ((odelta.added.length      !== 0 || cdelta.added.length      !== 0) ||
      (odelta.tombstoned.length !== 0 || cdelta.tombstoned.length !== 0) ||
      (odelta.deleted.length    !== 0 || cdelta.deleted.length    !== 0) ||
      (odelta.datatyped.length  !== 0 || cdelta.datatyped.length  !== 0)) {
    ZH.l('ZMerge.CoalesceAgentDeltas: FAIL (UNSUPPORTED OPERATIONS)');
    return false;
  }

  // USE ODENTRY's modified[]
  ndentry.delta.modified = ZH.clone(odentry.delta.modified);
  // APPEND CDENTRY's modified[]
  for (var i = 0; i < cdentry.delta.modified.length; i++) {
    var dlt = cdentry.delta.modified[i];
    ndentry.delta.modified.push(dlt);
  }

  // SEARCH FOR op_path MATCHES -> they can be COALESCED
  var to_apply = [];
  for (var i = 0; i < ndentry.delta.modified.length - 1; i++) {
    var ientry = ndentry.delta.modified[i];
    var ival   = ientry.V;
    var itype  = ival.T;
    var iopp   = ientry.op_path;
    for (var j = i + 1; j < ndentry.delta.modified.length; j++) {
      var jentry   = ndentry.delta.modified[j];
      var jopp     = jentry.op_path;
      if (!cmp_op_path(iopp, jopp)) {
        to_apply.push({dest : i, src : j});
      }
    }
  }

  // COALESCE matching entries
  var to_remove = [];
  for (var i = 0; i < to_apply.length; i++) {
    var xentry = ndentry.delta.modified[to_apply[i].dest];
    var xval   = xentry.V;
    var xtype  = xval.T;
    var sentry = ndentry.delta.modified[to_apply[i].src];
    var sval   = sentry.V;
    var stype  = sval.T;
    // NUMERIC modifications COALESCE V.D's
    if (xtype === "N" && stype === "N" && ZH.IsDefined(sval.V.D)) {
      var ndelt     = sval.V.D;
      var positive  = xval.V.P;                   // old-value.P
      var negative  = xval.V.N;                   // old-value.N
      if (ndelt > 0) positive = positive + ndelt; // add delta to old-value.P
      else           negative = negative - ndelt; // add delta to old-value.N
      xval.V.P      = positive;                   // set new-value.P
      xval.V.N      = negative;                   // set new-value.N
      if (ZH.IsDefined(xval.V.D)) { // Coalesce INCR Ds, not SET+INCR
        xval.V.D   += ndelt;                      // set new-value.D
      }
    } else { // All non-numeric modifications OVERWRITE the previous value
      ndentry.delta.modified[to_apply[i].dest] = sentry;
    }
    to_remove.push(to_apply[i].src);
  }

  // REMOVE COALESCED-entries
  for (var i = to_remove.length - 1; i >= 0; i--) {
    ndentry.delta.modified.splice(to_remove[i], 1);
  }
  return true;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BEST EFFORT POST-MERGE-DELTAS ---------------------------------------------

exports.CreateBestEffortPostMergeDeltas = function(pc) {
  ZH.l('ZMerge.CreateBestEffortPostMergeDeltas');
  var delta = pc.dentry.delta;
  var merge = {post_merge_deltas : []};

  // 1.) Modified[]
  for (var i = 0; i < delta.modified.length; i++) {
    var mentry  = delta.modified[i];
    var mval    = mentry.V;
    var mtype   = mval.T;
    var op_path = mentry.op_path;
    var pmpath  = ZH.PathGetDotNotation(op_path);
    var is_iop  = ZH.IsDefined(mval.V.D);
    if (is_iop && mtype === "N") {
      var ndelt = mval.V.D;
      var pme   = {op          : 'increment',
                   path        : pmpath,
                   value       : ndelt,
                   approximate : true};
      merge.post_merge_deltas.push(pme);
    } else { // All non-numeric modifications OVERWRITE the previous value
      var val = ZConv.CrdtElementToJson(mval);
      var pme = {op          : 'set',
                 path        : pmpath,
                 value       : val,
                 approximate : true};
      merge.post_merge_deltas.push(pme);
    }
  }

  // 2.) Tombstoned[]
  for (var i = 0; i < delta.tombstoned.length; i++) {
    var tentry  = delta.tombstoned[i];
    var op_path = tentry.op_path;
    var pmpath  = ZH.PathGetDotNotation(op_path);
    var pme     = {op          : 'delete',
                   path        : pmpath,
                   approximate : true};
    merge.post_merge_deltas.push(pme);
  }

  // 3.) Added[]
  for (var i = 0; i < delta.added.length; i++) {
    var aentry  = delta.added[i];
    var aval    = aentry.V;
    var atype   = aval.T;
    var op_path = aentry.op_path;
    var pmpath  = ZH.PathGetDotNotation(op_path);
    var ctype   = "O";
    if (op_path.length > 1) {
      var pp = op_path[op_path.length - 2]; // PARENT-PATH
      ctype  = pp.T;                        // PARENT-TYPE
    }
    var pme     = {approximate : true};
    if (ctype === "A") {
      pme.op    = 'insert';
      pme.index = Number(op_path[op_path.length - 1].K);
    } else {
      pme.op    = 'set';
    }
    pme.path  = pmpath;
    pme.value = ZConv.CrdtElementToJson(aval);
    merge.post_merge_deltas.push(pme);
  }

  // 4.) Deleted[]
  for (var i = 0; i < delta.deleted.length; i++) {
    var rentry  = delta.deleted[i];
    var op_path = rentry.op_path;
    var pmpath  = ZH.PathGetDotNotation(op_path);
    var pme     = {op          : 'delete',
                   path        : pmpath,
                   approximate : true};
    merge.post_merge_deltas.push(pme);
  }

  // 5.) Datatyped[] -> NOTHING TO DO

  return ZH.clone(merge);
}



// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZDT.RunDatatypeFunctions()
exports.FindNestedElement = function(crdtd, op_path) {
  return find_nested_element_default(crdtd, op_path);
}

// NOTE: Used by ZDT.get_page_number()
exports.FindNestedElementExact = function(crdtd, op_path) {
  return find_nested_element_exact(crdtd, op_path);
}

// NOTE: Used by ZXact.populate_nested_modified()
exports.FindNestedElementBase = function(crdtd, op_path) {
  find_nested_element_base(crdtd, op_path);
}

// NOTE: Uzed by ZGC.PostMergeUpdateLhn()
exports.GetLastKeyByName = function(clm, val, op_path) {
  return get_last_key_by_name(clm, val, op_path);
}

// NOTE: Used by ZGC.do_garbage_collection/freshen_compact()
exports.DoArrayMerge = function(pclm, carr, debug) {
  ZH.l('ZMerge.DoArrayMerge');
  DebugArrayMerge = debug;
  do_array_merge(pclm, carr);
}

// NOTE: Used by ZGC.compact_anchors();
exports.UnrollS = function(s) {
  return unroll_s(s);
}

// NOTE: Used by testing scripts
exports.ArrayMerge = function(pclm, carr, aval, debug) {
  ZH.l('ZMerge.DoArrayMerge');
  DebugArrayMerge = debug;
  return array_merge(pclm, carr, aval);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZMerge']={} : exports);

