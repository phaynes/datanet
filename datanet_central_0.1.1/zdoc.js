"use strict";

var ZDocOps, ZConv, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZDocOps = require('./zdoc_ops');
  ZConv   = require('./zconvert');
  ZS      = require('./zshared');
  ZH      = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZDOC OPERATION HELPERS ----------------------------------------------------

// FUNCTION: reset_JsonG()
// PURPOSE:  After every zdoc.G().X() call, this.got needs to be reset
//
function reset_JsonG(zdoc) {
  zdoc.got      = zdoc._; // latest G() search results
  zdoc.gkey     = null;   // latest G() search key
  zdoc.op_path  = [];     // Op-Path latest G() chain  (e.g. [_1,#1],[_2,#2]])
}

// FUNCTION: update_json_mirror()
// PURPOSE:  Write-ops [S(),D(),...] need to update
//            this._ (the CRDT's JSON mirror)
//
function update_json_mirror(me, par, val, flags) {
  if (me.stateless) return;
  var spot    = me._;
  var op_path = me.op_path;
  var plen    = op_path.length;
  val         = ZH.clone(val); // do NOT store object references
  for (var i = 0; i < plen; i++) {
    var op_key = op_path[i];
    if (i === (plen - 1)) { // Need to set arr[k] = v (by reference)
      if      (flags.is_set)  spot[op_key] = val;
      else if (flags.is_del)  {
        if (Array.isArray(par)) spot.splice(op_key, 1);
        else /* object */       delete(spot[op_key]);
      }
      else if (flags.is_ins) {
        var arr = spot[op_key];
        if (flags.index > arr.length) arr[flags.index] = val; // SET equivalent
        else                          arr.splice(flags.index, 0, val);
      }
      break;
    }
    spot = spot[op_key];
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARGUMENT CHECKS -----------------------------------------------------------

// EXPORT:  WriteKeyCheck()
// PURPOSE:  Overwriting keyword-fields is prohibited
// NOTE:    OK to export, no references to 'me'
//
exports.WriteKeyCheck = function(path) {
  if (typeof(path) !== 'string') return new Error(ZS.Errors.KeyNotString);
  var res   = path.split('.');
  var kpath = res[0].toUpperCase();
  if (kpath === "_ID" || kpath === "_META" || kpath === "_CHANNELS") {
    return new Error(ZS.Errors.ReservedFieldMod);
  }
  return null;
}

// FUNCTION: S_arg_check()
// PURPOSE:  Not all Javascript types are supported, so do type checking
//
function s_arg_check(ptype, key, vtype, adding, path, is_set) {
  if (ptype === "A" && ZH.IsJsonElementInt(key) === false) {
    return new Error(ZS.Errors.SetArrayInvalid);
  }
  if (ZH.IsUndefined(vtype)) return new Error(ZS.Errors.SetValueInvalid);
  if ((ptype !== "O" && ptype !== "A") && adding) {
    return new Error(ZS.Errors.SetPrimitiveInvalid);
  }
  return null;
}

// EXPORT:  S_ArgCheck()
// PURPOSE: Wrapper for s_arg_check()
// NOTE:    OK to export, no references to 'me'
//
exports.S_ArgCheck = function(ptype, key, vtype, adding, path, is_set) {
  return s_arg_check(ptype, key, vtype, adding, path, is_set);
}

// FUNCTION: insert_arg_pre_check()
// PURPOSE:  insert() calls index === number
//
function insert_arg_pre_check(index) {
  var i = Number(index);
  if (isNaN(index) || !ZH.IsJsonElementInt(i) || i < 0) {
    return new Error(ZS.Errors.InsertArgsInvalid);
  }
  return null;
}

// EXPORT:  InsertArgPreCheck()
// PURPOSE: Wrapper for insert_arg_pre_check()
// NOTE:    OK to export, no references to 'me'
//
exports.InsertArgPreCheck = function(index) {
  return insert_arg_pre_check(index);
}

// FUNCTION: insert_arg_post_check()
// PURPOSE:  insert() calls only possible on Arrays
//
function insert_arg_post_check(ptype) {
  if (ptype !== "A") {
    return new Error(ZS.Errors.InsertTypeInvalid + ZH.getFullTypeName(ptype));
  }
  return null;
}

// EXPORT:  InsertArgPostCheck()
// PURPOSE: Wrapper for insert_arg_post_check()
// NOTE:    OK to export, no references to 'me'
//
exports.InsertArgPostCheck = function(ptype) {
  return insert_arg_post_check(ptype);
}

// FUNCTION: perform_incr_checks()
// PURPOSE:  argument error checkign for INCR/DECR
// NOTE:     NOT OK to export
//
function perform_incr_checks(me, key, val) {
  if (me.stateless) return null; // MEMCACHE -> SKIP CHECK
  var res   = zdoc_g(me, key); // key for JSON, me.gkey for array[index]
  if (ZH.IsUndefined(res)) return new Error(ZS.Errors.FieldNotFound);
  var rtype = typeof(res);
  if (rtype !== 'number') return new Error(ZS.Errors.IncrOnNaN);
  var n = Number(val);
  if (isNaN(n)) return new Error(ZS.Errors.IncrByNaN);
  return null;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZATION ------------------------------------------------------------

// CONSTRUCTOR: ZDOC
// PURPOSE:     Stores necessary data/handles to operate independently
//
function zdoc(ns, cn, json, crdt) {
  reset_JsonG(this);
  if (crdt) {
    crdt._meta.last_member_count = crdt._meta.member_count;
    // "$" stores the CRDT data (for G(k1).G(k2).S(k3, v) WRITES)
    this.$     = crdt;
    this.delta = ZH.InitDelta(this.$._meta);
  }
  
  this._     = json; // "_" stores the mirror-object (for READS)
  this.oplog = [];   // Log of write operations
  this.ns    = ns;
  this.cn    = cn;
}

exports.Create = function(ns, cn, json, crdt){
  return new zdoc(ns, cn, json, crdt);
}

exports.CreateZDoc = function(ns, cn, key, crdt, next) {
  if (!crdt) next(null, null);
  else {
    var pjson = ZH.CreatePrettyJson(crdt);
    var nzdoc = exports.Create(ns, cn, pjson, crdt);
    next(null, nzdoc);
  }
}

exports.CreateStatelessZDoc = function(client, ns, cn, key, next) {
  var json        = {_id : key};
  var nzdoc       = exports.Create(ns, cn, json, null);
  nzdoc.client    = client;
  nzdoc.stateless = true;
  next(null, nzdoc);
}

function do_create_zdocs(ns, cn, crdts, zdocs, next) {
  if (crdts.length === 0) next(null, zdocs);
  else {
    var crdt = crdts.shift();
    var key  = crdt._meta._id;
    exports.CreateZDoc(ns, cn, key, crdt, function(cerr, zdoc) {
      if (cerr) next(cerr, null);
      else {
        zdocs[key] = zdoc;
        setImmediate(do_create_zdocs, ns, cn, crdts, zdocs, next);
      }
    });
  }
}

exports.CreateZDocs = function(ns, cn, crdts, next) {
  var ccrdts = [];
  for (var key in crdts) {
    ccrdts.push(crdts[key]);
  }
  var zdocs  = {};
  do_create_zdocs(ns, cn, ccrdts, zdocs, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// READ OPERATIONS -----------------------------------------------------------

// FUNCTION: zdoc_g()
// PURPOSE:  Used in Sets and Deletes to find nested elements
// EXAMPLE:  SET x.y.z 1 -> zdoc_g('x').zdoc_g('y').zdoc_s('z',1);
//
function zdoc_g(me, okey) {
  var val  = null;
  var type = typeof(me.got);
  if (type === "object") val = me.got[okey];
  else                   return; // returns undefined
  me.gkey = okey;
  if (ZH.IsUndefined(val)) return;
  me.got  = val;         // me.got -> looked-up-val (method-chaining)
  me.op_path.push(okey); // okey for JSON
  return me.got;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BASE OPERATIONS -----------------------------------------------------------

// FUNCTION: __zdoc_s()
// PURPOSE:  Base operation SET, reduced error checking
//
function __zdoc_s(par, me, key, val, dt, extra, is_set) {
  var ptype  = ZConv.GetCrdtTypeFromJsonElement(par);
  var res    = zdoc_g(me, key); // key for JSON, me.gkey for array[index]
  var miss   = ZH.IsUndefined(res);
  if (miss) me.op_path.push(key); // new key in path
  var adding = miss;
  var vtype  = ZConv.GetCrdtTypeFromJsonElement(val);
  var path   = me.op_path.join('.');
  var err    = s_arg_check(ptype, key, vtype, adding, path, is_set);
  if (err) return err;
  update_json_mirror(me, par, val, { is_set: true});
}

// FUNCTION: __zdoc_d()
// PURPOSE:  Base operation DELETE, has no error checking
// NOTE:     No LHN re-ordering in d() because it leaves tombstones
//
function __zdoc_d(par, me, key) {
  update_json_mirror(me, par, null, {is_del : true});
}

// FUNCTION: __zdoc_insert()
// PURPOSE:  Base operation ARRAY-INSERT, no error checking
//
function __zdoc_insert(par, me, key, index, val) {
  update_json_mirror(me, [], val, {is_ins : true, index: index});
}

// FUNCTION: __zdoc_incr)
// PURPOSE:  Base operation NUMBER-INCREMENT, no error checking
//
function __zdoc_incr(me, par, val) {
  var mval    = me.got + val;
  update_json_mirror(me, par, mval, { is_set: true});
}

// FUNCTION: __zdoc_decr)
// PURPOSE:  Base operation NUMBER-DECREMENT, no error checking
//
function __zdoc_decr(me, par, val) {
  var mval    = me.got - val;
  update_json_mirror(me, par, mval, { is_set: true});
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRITE OPERATIONS ----------------------------------------------------------

// FUNCTION: zdoc_s()
// PURPOSE:  Set a key (van be dot-notation) to a value (can be type:AONS)
//
function zdoc_s(me, key, val, dt, extra) {
  var par = me.got;
  return __zdoc_s(par, me, key, val, dt, extra, true);
}

// FUNCTION: zdoc_d()
// PURPOSE:  Delete an object[key] or array[index]
//
function zdoc_d(me, key) {
  var par = me.got;
  var res = zdoc_g(me, key); // key for JSON, me.gkey for array[index]
  if (!me.stateless) {       // STATELESS -> SKIP CHECK
    if (ZH.IsUndefined(res)) return new Error(ZS.Errors.FieldNotFound);
  }
  return __zdoc_d(par, me, key);
}

// FUNCTION: zdoc_insert()
// PURPOSE:  Insert into the middle of an array, using 'index' as the position
// NOTE:     zdoc_insert() is built on the '__zdoc_insert()' base-operation
//
function zdoc_insert(me, key, index, val) {
  var err     = insert_arg_pre_check(index);
  if (err) return err;
  var index   = Number(index);
  var par     = me.got;
  var res     = zdoc_g(me, key); // key for JSON, me.gkey for array[index]
  if (ZH.IsUndefined(res)) {     // if not found, create one element array
    var arr    = [];
    arr[index] = val;
    return __zdoc_s(par, me, key, arr, false);
  }
  var rtype   = ZConv.GetCrdtTypeFromJsonElement(me.got);
  err         = insert_arg_post_check(rtype);
  if (err) return err;
  return __zdoc_insert(par, me, key, index, val);
}

function zdoc_incr(me, key, val) {
  var par     = me.got;
  var err     = perform_incr_checks(me, key, val);
  if (err) return err;
  val         = Number(val);
  return __zdoc_incr(me, par, val);
}

function zdoc_decr(me, key, val) {
  var par     = me.got;
  var err     = perform_incr_checks(me, key, val);
  if (err) return err;
  val         = Number(val);
  return __zdoc_decr(me, par, val);
}

function create_oplog_args(arg1, arg2, arg3, arg4) {
  var args = [];
  if (ZH.IsUndefined(arg1)) return args;
  args.push(arg1);
  if (ZH.IsUndefined(arg2)) return args;
  args.push(arg2);
  if (ZH.IsUndefined(arg3)) return args;
  args.push(arg3);
  if (ZH.IsUndefined(arg4)) return args;
  args.push(arg4);
  return args;
}

// FUNCTION: zoplog_add()
// PURPOSE:  Add successful write operation to oplog
//
function zoplog_add(zdoc, opname, path, arg1, arg2, arg3, arg4) {
  var args = create_oplog_args(arg1, arg2, arg3, arg4);
  zdoc.oplog.push({name: opname, path: path, args : args});
  //ZH.e(zdoc.oplog);
}

// FUNCTION: zdoc_write_op()
// PURPOSE:  find nested elements via zdoc_g() and
//           pass final element to end_func()
// NOTE:     currently 2 argument MAX to this func
//
function zdoc_write_op(me, opname, end_func, path, arg1, arg2, arg3, arg4) {
  reset_JsonG(me);
  var err  = exports.WriteKeyCheck(path);
  if (err) return err;
  var keys = ZH.ParseOpPath(path);
  for (var i = 0; i < keys.length - 1; i++) {
    var res = zdoc_g(me, keys[i]);
    if (me.stateless) continue; // MEMCACHE -> SKIP
    if (ZH.IsUndefined(res)) {
      return new Error(ZS.Errors.NestedFieldMissing + keys[i]);
    }
  }
  err = end_func(me, keys[keys.length - 1], arg1, arg2, arg3, arg4);
  if (err) return err;
  else {
    zoplog_add(me, opname, path, arg1, arg2, arg3, arg4);
  }
}

// METHOD:  s()
// PURPOSE: Set a ZDOC field's value
//
zdoc.prototype.s = function(path, val, dt, extra) {
  return zdoc_write_op(this, 'set', zdoc_s, path, val, dt, extra);
}

// METHOD:  d()
// PURPOSE: Delete a ZDOC field
//
zdoc.prototype.d = function(path) {
  return zdoc_write_op(this, 'delete', zdoc_d, path);
}

// METHOD:  insert()
// PURPOSE: Insert a value into a ZDOC field (at position: index)
// NOTE:    field must be an array
//
zdoc.prototype.insert = function(path, index, val) {
  return zdoc_write_op(this, 'insert', zdoc_insert, path, index, val);
}

// METHOD:  incr()
// PURPOSE: increment ZDOC field
// NOTE:    field must be a number
//
zdoc.prototype.incr = function(path, val) {
  return zdoc_write_op(this, 'increment', zdoc_incr, path, val);
}

// METHOD:  decr()
// PURPOSE: decrement ZDOC field
// NOTE:    field must be a number
//
zdoc.prototype.decr = function(path, val) {
  return zdoc_write_op(this, 'decrement', zdoc_decr, path, val);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// USERLAND OPERATIONS -------------------------------------------------------

// METHOD:  lpush()
// PURPOSE: LEFT-PUSH a value onto a ZDOC (array) field
// NOTE:    LPUSH calls into INSERT (i.e. userland operation)
//
zdoc.prototype.lpush = function(path, val) {
  var json = this._;
  var res  = ZH.LookupByDotNotation(json, path);
  if (res && (typeof(res) !== "object" || !Array.isArray(res))) {
    return ZS.Errors.RpushUnsupportedType;
  }
  var index = 0;
  return zdoc_write_op(this, 'insert', zdoc_insert, path, index, val);
}

// METHOD:  rpush()
// PURPOSE: RIGHT-PUSH a value onto a ZDOC (array) field
// NOTE:    RPUSH calls into INSERT (i.e. userland operation)
//
zdoc.prototype.rpush = function(path, val) {
  var json = this._;
  var res  = ZH.LookupByDotNotation(json, path);
  if (res && (typeof(res) !== "object" || !Array.isArray(res))) {
    return ZS.Errors.RpushUnsupportedType;
  }
  var index = res ? res.length : 0;
  return zdoc_write_op(this, 'insert', zdoc_insert, path, index, val);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMIT --------------------------------------------------------------------

function send_agent_commit(cli, zdoc, ns, cn, key, crdt, oplog, next) {
  ZDocOps.Commit(cli, ns, cn, key, crdt, oplog, next);
}

function send_agent_pull(cli, zdoc, ns, cn, key, crdt, oplog, next) {
  ZDocOps.Pull(cli, ns, cn, key, crdt, oplog, next);
}

function send_agent_operation(zdoc, opfunc, next) {
  var cli   = zdoc.client;
  var ns    = zdoc.ns;
  var cn    = zdoc.cn;
  var key   = zdoc.$._meta._id;
  var crdt  = zdoc.$;
  var oplog = zdoc.oplog;
  opfunc(cli, zdoc, ns, cn, key, crdt, oplog, function(serr, nzdoc) {
    if (serr) next(serr, null);
    else      next(null, nzdoc);
  });

}

zdoc.prototype.commit = function(next) {
  //ZH.l('commit: oplog'); ZH.p(this.oplog);
  send_agent_operation(this, send_agent_commit, next);
}

zdoc.prototype.pull = function(next) {
  send_agent_operation(this, send_agent_pull, next);
}

function do_nonstandard_commit(me, ismc, sep, ex, next) {
  var cli    = me.client;
  var ns     = me.ns;
  var cn     = me.cn;
  var oplog  = me.oplog;
  var json   = me._;
  var key    = json._id;
  var rchans = json._channels;
  if (ismc) {
    ZDocOps.MemcacheCommit(cli, ns, cn, key, oplog, rchans, sep, ex, next);
  } else {
    ZDocOps.StatelessCommit(cli, ns, cn, key, oplog, rchans, next);
  }
}

zdoc.prototype.memcache_commit = function(sep, ex, next) {
  do_nonstandard_commit(this, true, sep, ex, next);
}

zdoc.prototype.stateless_commit = function(next) {
  do_nonstandard_commit(this, false, undefined, undefined, next);
}

zdoc.prototype.set_channels = function(rchans) {
  //ZH.l('zdoc.set_channels: R'); ZH.p(rchans);
  if (this._) this._._channels = ZH.clone(rchans);
  else        this._           = {_channels : ZH.clone(rchans)};
  var crdt = this.$;
  if (crdt && crdt._meta) {
    crdt._meta.replication_channels = ZH.clone(rchans);
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

// EXPORT:  CreateZDocFromPullResponse()
// PURPOSE: ClientPull requires client side ZDOC reformatting
//
exports.CreateZDocFromPullResponse = function(ns, cn, key, ncrdt, mjson, oplog,
                                              next) {
  exports.CreateZDoc(ns, cn, key, ncrdt, function(err, zdoc) {
    if      (err)           next(err,  null);
    else if (zdoc === null) next(null, null);
    else {
      zdoc._     = mjson; // Override w/ Modified-JSON
      zdoc.oplog = oplog; // Override w/ Original Oplog
      next(null, zdoc);
    }
  });
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZDoc']={} : exports);

