"use strict";

var ZXact, ZConv, ZDoc, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZXact = require('./zxaction');
  ZConv = require('./zconvert');
  ZDoc  = require('./zdoc');
  ZS    = require('./zshared');
  ZH    = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

// FUNCTION: subarray_elements_match()
// PURPOSE:  See if path[index] arrays match (between member & delta.*[])
// NOTE:     used to see if op_path.K.* match
//
function subarray_elements_match(a, b, index) {
  if (a.length !== b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i][index] !== b[i][index]) return false;
  }
  return true;
}

// FUNCTION: add_null_members()
// PURPOSE:  Javascript supports writing past the end of an array
//           and fills in the blank spaces w/ null entries
//           this behavior must be mimicked in the CRDT
//
function add_null_members(me, parent) {
  for (var i = 0; i < parent.V.length; i++) {
    if (ZH.IsUndefined(parent.V[i]) || parent.V[i] === null) {
      parent.V[i] = ZConv.JsonElementToCrdt("X", parent.V[i], me.$._meta, true);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// OP-PATH & DELTA ENTRIES ---------------------------------------------------

exports.CreateOpPathEntry = function(key, member) {
  if (ZH.IsUndefined(member)) {
    return {"K" : ZH.clone(key)};
  } else {
    return {"K" : ZH.clone(key),
            "_" : member["_"], "#" : member["#"],
            "@" : member["@"], "T" : member["T"]};
  }
}

exports.CreateDeltaEntry = function(op_path, val) {
  var entry = {op_path : ZH.clone(op_path),
               V       : ZH.clone(val)};
  return entry;
}

// FUNCTION: upsert_delta_modified()
// PURPOSE:  The same element can be modified many times within a transaction
//           on same element repeat modifications, only the last one matters
//           if it's not a repeat, push it to delta.modifications
//
function upsert_delta_modified(me, entry) {
  // look for matching modifieds, if found, replace
  for (var i = 0; i < me.delta.modified.length; i++) {
    var op_path = me.delta.modified[i].op_path;
    if (subarray_elements_match(op_path, me.op_path, "K")) {
      me.delta.modified[i] = entry;
      return;
    }
  }
  me.delta.modified.push(entry);
}

// FUNCTION: remove_redundant_modified_entry()
// PURPOSE:  deletion removes any delta.modified for this [_,#]
//
function remove_redundant_modified_entry(me) {
  // Delete Previous Overwrite: remove from delta.modified[]
  for (var i = 0; i < me.delta.modified.length; i++) {
    var op_path = me.delta.modified[i].op_path;
    if (subarray_elements_match(op_path, me.op_path, "K")) {
      me.delta.modified.splice(i, 1); // remove entry from modified[]
      return; // there will only be one
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARGUMENT CHECKS -----------------------------------------------------------

// FUNCTION: perform_incr_checks()
// PURPOSE:  argument error checkign for INCR/DECR
//
function perform_incr_checks(me, key, val) {
  var res     = g(me, key); // key for JSON, me.gkey for array[index]
  if (ZH.IsUndefined(res)) return new Error(ZS.Errors.FieldNotFound);
  var rtype   = me.got.T;
  if (rtype !== "N") return new Error(ZS.Errors.IncrOnNaN);
  var n = Number(val);
  if (isNaN(n)) return new Error(ZS.Errors.IncrByNaN);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// READ OPERATIONS -----------------------------------------------------------

// FUNCTION: reset_G()
// PURPOSE:  After every zdoc.G().X() call, this.got needs to be reset
//
function reset_G(zdoc, crdt) {
  zdoc.got      = crdt._data; // latest G() search results
  zdoc.gkey     = null;       // latest G() search key
  zdoc.op_path  = [];         // Op-Path latest G() chain
}

// FUNCTION: get_array_key_skipping_tombstones()
// PURPOSE:  tombstones create garbage in CRDT arrays
//            array references:y array[index] wont work because of the garbage
//            this function transforms an index into its real index
//            skipping over tombstones (& SLEEPERS)
//
function get_array_key_skipping_tombstones(arr, key) {
  var tcnt =  0;
  var rcnt = -1;
  var sps  = ZH.IsLateComer(arr);
  var parr = arr.V;
  for (var i = 0; i < parr.length; i++) {
    var el = parr[i];
    if      (!sps && el["Z"]) tcnt += 1;
    else if (el["X"])         tcnt += 1;
    else                      rcnt += 1;
    if (rcnt === key) return i;
  }
  return key + tcnt; // past end of array insertion
}

// FUNCTION: g()
// PURPOSE:  Used in Sets and Deletes to find nested elements
// EXAMPLE:  g('x').g('y').zdoc_s('z',1);
//
function g(me, okey) {
  var rkey = okey; // real-key may be different than okey due to tombstones
  var val  = null;
  var type = me.got.T;
  if        (type === "O") {
    val  = me.got.V[rkey];
  } else if (type === "A") { // tombstones mean okey must be mapped to rkey
    rkey = get_array_key_skipping_tombstones(me.got, okey);
    val  = me.got.V[rkey];
  } else {
    return; // returns undefined
  }
  me.gkey = rkey;      // me.gkey is the real-key
  if (ZH.IsUndefined(val)) return;
  me.got  = val;       // me.got -> looked-up-val (method-chaining)
  me.op_path.push(exports.CreateOpPathEntry(okey, me.got)); // okey for JSON
  return me;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FULL BASE OPERATIONS ------------------------------------------------------

// FUNCTION: __full_incr_op)
// PURPOSE:  Base operation NUMBER-INCREMENT, no error checking
//
function __full_incr_op(parent, me, val) {
  // Edit found entry in place
  me.got.V.P  += val; // Increment P
  if (ZH.IsUndefined(me.got.V.D)) me.got.V.D  = val; // SET DELTA
  else                            me.got.V.D += val; // INCR DELTA

  // decrements modify in place -> to-be-delta.modified[]
  var entry = exports.CreateDeltaEntry(me.op_path, me.got);
  upsert_delta_modified(me, entry)
}

// FUNCTION: __full_decr_op()
// PURPOSE:  Base operation NUMBER-DECREMENT, no error checking
//
function __full_decr_op(parent, me, val) {
  // Edit found entry in place
  me.got.V.N  += val; // Increment N
  if (ZH.IsUndefined(me.got.V.D)) me.got.V.D  = (-1 * val); // SET DELTA
  else                            me.got.V.D -= val;        // DECR DELTA

  // decrements modify in place -> to-be-delta.modified[]
  var entry = exports.CreateDeltaEntry(me.op_path, me.got);
  upsert_delta_modified(me, entry)
}

// FUNCTION: __full_d_op()
// PURPOSE:  Base operation DELETE, has no error checking
// NOTE:     No LHN re-ordering in d() because it leaves tombstones
//
function __full_d_op(parent, me, key) {
  var ptype     = parent.T;
  var committed = (me.got["#"] !== -1);
  var added     = (me.got["+"] === true);
  var p_isarr   = (ptype === "A");
  var is_oa     = ZH.IsOrderedArray(parent);
  var do_t      = p_isarr && !is_oa;

  //  2.) write delta : deleted[] or tombstoned[] on D(array entry)
  if (committed) {
    var entry = exports.CreateDeltaEntry(me.op_path, me.got);
    if (do_t) me.delta.tombstoned.push(entry);
    else      me.delta.deleted.push   (entry);
    remove_redundant_modified_entry(me);
  }

  //  3.) Update "$"
  var tombstone = committed && p_isarr;
  if      (tombstone) parent.V[me.gkey]["X"] = true; // committed && type:[A]
  else if (p_isarr)   parent.V.splice(me.gkey, 1);   // not committed
  else                delete(parent.V[me.gkey]);     // type:[N,S,O]
}

// FUNCTION: __full_s_op()
// PURPOSE:  Base operation SET, reduced error checking
//
function __full_s_op(parent, me, key, val, dt, extra, is_set) {
  var ptype   = parent.T;
  var res     = g(me, key); // key for JSON, me.gkey for array[index]
  var miss    = ZH.IsUndefined(res);
  if (miss) me.op_path.push(exports.CreateOpPathEntry(key)); // new key in path
  var adding  = (miss || (me.got["+"] === true)); 
  var vtype   = ZConv.GetCrdtTypeFromJsonElement(val);
  var path    = ZH.PathGetDotNotation(me.op_path);
  var err     = ZDoc.S_ArgCheck(ptype, key, vtype, adding, path, is_set);
  if (err) return err;

  // Create CRDT member from JSON
  var member = ZConv.JsonElementToCrdt(vtype, val, me.$._meta, adding);
  if (dt) {
    member.F = dt;
    if (extra) member.E = extra;
  }
  if (!adding) { // NOTE: [S, <] unchanged: operation is overwrite
    member["_"] = -1;
    member["#"] = -1;
    member["@"] = -1;
    if (me.got["S"]) member["S"] = me.got["S"]; // only type:[A] has "S"
    if (me.got["<"]) member["<"] = me.got["<"]; // only type:[A] has "<"
    // delta.modified[] population
    var entry = exports.CreateDeltaEntry(me.op_path, member);
    upsert_delta_modified(me, entry);
  }

  // replace current CRDT element w/ member
  var p_plen        = parent.V.length; // length before s()
  parent.V[me.gkey] = member;          // s() -> CRDT element overwrite

  // Array[index] inserts past the end of the array, produce null-entries
  if (adding && (ptype === "A")) {
    var index = Number(me.gkey);
    if (index > p_plen) add_null_members(me, parent);
    var ncval = parent.V[index];
    if (index === 0) ncval["<"] = ZH.CreateHeadLHN();
    else {
      if (parent.V[index - 1]["_"] !== -1) { // COMMITTED-member LHN
        ncval["<"] = parent.V[index - 1];
      }
    }
  }
}

// FUNCTION: __full_insert_op()
// PURPOSE:  Base operation ARRAY-INSERT, reduced error checking
//
function __full_insert_op(parent, me, key, index, val) {
  var vtype   = ZConv.GetCrdtTypeFromJsonElement(val);
  var member  = ZConv.JsonElementToCrdt(vtype, val, me.$._meta, true);
  member["+"] = true;     // Mark new member as to-be-delta.added[]
  var arr     = parent.V[me.gkey];
  var par     = arr.V;
  index       = get_array_key_skipping_tombstones(arr, index);
  var is_ll   = ZH.IsLargeList(arr);
  if (is_ll && (index != par.length)) {
    return new Error(ZS.Errors.LargeListOnlyRPUSH);
  }
  if        (index >  par.length) { // ---------------------- SET equivalent
    par[index] = member;
    add_null_members(me, arr);
  } else if (index == par.length) { // ---------------------- RPUSH equivalent
    par.push(member);
  } else { // ----------------------------------------------- MIDDLE-INSERT
    var lhn;
    if (index === 0) lhn = ZH.CreateHeadLHN()
    else {
      if (par[index - 1]["_"] !== -1) { // COMMITTED-member LHN
        lhn = par[index - 1];
      }
    }
    member["<"] = lhn;
    var rhn     = par[index];
    par.splice(index, 0, member); // splice-INSERT new member into array
    // NOTE: Next line maintains a contiguous local CRDT
    //       But the OPERATION (in this line) is ONLY replicated if RHN is "+"
    //       LHN's are defined ONCE by Delta Author
    rhn["<"]    = member;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRITE OPERATIONS ----------------------------------------------------------

// FUNCTION: full_s_op()
// PURPOSE:  Set a key (van be dot-notation) to a value (can be type:AONS)
//
function full_s_op(me, key, val, dt, extra) {
  var parent = me.got;
  return __full_s_op(parent, me, key, val, dt, extra, true);
}

// FUNCTION: full_d_op()
// PURPOSE:  Delete an object[key] or array[index]
//
function full_d_op(me, key) {
  var parent = me.got;
  var res    = g(me, key); // key for JSON, me.gkey for array[index]
  if (ZH.IsUndefined(res)) return new Error(ZS.Errors.FieldNotFound);
  else                     return __full_d_op(parent, me, key);
}

// FUNCTION: full_insert_op()
// PURPOSE:  Insert into the middle of an array, using 'index' as the position
// NOTE:     full_insert_op() is built on the '__full_insert_op()' base-operation
//
function full_insert_op(me, key, index, val) {
  var err     = ZDoc.InsertArgPreCheck(index);
  if (err) return err;
  var index   = Number(index);
  var parent  = me.got;
  var res     = g(me, key);  // key for JSON, me.gkey for array[index]
  if (ZH.IsUndefined(res)) { // if not found, create one element array
    var arr    = [];
    arr[index] = val;
    return __full_s_op(parent, me, key, arr, null, null, false);
  } else {
    var rtype = me.got.T;
    err       = ZDoc.InsertArgPostCheck(rtype);
    if (err) return err;
    return __full_insert_op(parent, me, key, index, val);
  }
}

// FUNCTION: full_incr_op()
// PURPOSE:  increment a value
//
function full_incr_op(me, key, val) {
  var parent  = me.got;
  var err     = perform_incr_checks(me, key, val);
  if (err) return err;
  val         = Number(val);
  return __full_incr_op(parent, me, val);
}
// FUNCTION: full_decr_op()
// PURPOSE:  decrement a value
//
function full_decr_op(me, key, val) {
  var parent  = me.got;
  var err     = perform_incr_checks(me, key, val);
  if (err) return err;
  val         = Number(val);
  return __full_decr_op(parent, me, val);
}

// FUNCTION: zdoc_write_op()
// PURPOSE:  find nested elements via g() and
//             pass final element to end_func()
// NOTE:     currently 3 argument MAX to this func
//
function zdoc_write_op(me, end_func, path, arg1, arg2, arg3) {
  reset_G(me, me.$);
  var keys = ZH.ParseOpPath(path);
  for (var i = 0; i < keys.length - 1; i++) {
    var res = g(me, keys[i]);
    if (ZH.IsUndefined(res)) {
      return new Error(ZS.Errors.NestedFieldMissing + keys[i])
    }
  }
  return end_func(me, keys[keys.length - 1], arg1, arg2, arg3);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REPLAY WRAPPERS -----------------------------------------------------------

// FUNCTION: replay_s()
// PURPOSE:  replay Set a ZDOC field's value
//
function replay_s(zdoc, path, val, dt, extra) {
  return zdoc_write_op(zdoc, full_s_op, path, val, dt, extra);
}

// FUNCTION: replay_d()
// PURPOSE:  replay Delete a ZDOC field
//
function replay_d(zdoc, path) {
  return zdoc_write_op(zdoc, full_d_op, path);
}

// FUNCTION: replay_insert()
// PURPOSE:  replay  Insert a value into a ZDOC field (at position: index)
// NOTE:     field must be an array
//
function replay_insert(zdoc, path, index, val) {
  return zdoc_write_op(zdoc, full_insert_op, path, index, val);
}

// FUNCTION: replay_incr()
// PURPOSE:  replay increment ZDOC field
// NOTE:     field must be a number
//
function replay_incr(zdoc, path, val) {
  return zdoc_write_op(zdoc, full_incr_op, path, val);
}

// FUNCTION: replay_decr()
// PURPOSE:  replay decrement ZDOC field
// NOTE:     field must be a number
//
function replay_decr(zdoc, path, val) {
  return zdoc_write_op(zdoc, full_decr_op, path, val);
}

function replay_operation(zdoc, op) {
  //ZH.l('replay_operation'); ZH.l('crdt'); ZH.p(zdoc.$); ZH.l('op'); ZH.p(op);
  var err    = ZDoc.WriteKeyCheck(op.path);
  if (err) return err;
  var opname = op.name;
  if        (opname === 'set') {
    err = replay_s     (zdoc, op.path, op.args[0], op.args[1], op.args[2]);
  } else if (opname === 'delete') {
    err = replay_d     (zdoc, op.path);
  } else if (opname === 'insert') {
    err = replay_insert(zdoc, op.path, op.args[0], op.args[1]);
  } else if (opname === 'increment') {
    err = replay_incr  (zdoc, op.path, op.args[0]);
  } else if (opname === 'decrement') {
    err = replay_decr  (zdoc, op.path, op.args[0]);
  }
  return err;
}

// EXPORT:  CreateDelta()
// PURPOSE: Generate delta from (crdt, oplog)
// NOTE:      oplog can be NULL on Insert()
exports.CreateDelta = function(crdt, oplog, next) {
  if (oplog === null) return null;
  var meta = crdt._meta;
  var zdoc = { '$' : crdt, delta : ZH.InitDelta(meta)};
  for (var i in oplog) {
    var rerr = replay_operation(zdoc, oplog[i]);
    if (rerr) return next(rerr, null);
  }
  next(null, zdoc.delta);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZOplog']={} : exports);

