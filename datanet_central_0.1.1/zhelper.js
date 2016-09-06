"use strict";

var crypto = null; // NOT used by BROWSER
var fs     = null; // NOT used by BROWSER
//var bcrypt;

var ZBH, ZConv, ZLog, ZS;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  crypto = require("crypto");
  //bcrypt = require('bcrypt')
  fs     = require('fs');
  ZBH    = require('./zbase_helper');
  ZConv  = require('./zconvert');
  ZLog   = require('./zlog');
  ZS     = require('./zshared');
} else {
  //bcrypt = dcodeIO.bcrypt;
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GENERAL -------------------------------------------------------------------

exports.clone = function(o) {
  return ZBH.clone(o);
}

exports.copy_members = function(o) {
  var c = {};
  for (var k in o) c[k] = o[k];
  return c;
}

exports.TryBlock = function(func) {
  try {
    func();
  } catch(e) {
    exports.e(e.message);
  }
}

exports.IfDefinedTryBlock = function(func) {
  if (func) {
    exports.TryBlock(func);
  }
}

exports.ParseJsonFile = function(fname) {
  try {
    var text = fs.readFileSync(fname, 'utf8');
    return JSON.parse(text);
  } catch (e) {
    console.error(e);
    return null;
  }
}

exports.IsJsonElementInt = function(n) {
 return ((typeof(n) === 'number') && ((n % 1) === 0));
}

exports.GetUrlParamByName = function(name) {
  name        = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
  var regex   = new RegExp("[\\?&]" + name + "=([^&#]*)");
  var results = regex.exec(location.search);
  return results ? decodeURIComponent(results[1].replace(/\+/g, " ")) : "";
}

exports.UnionArrays = function(x, y) {
  var obj = {};
  var res = [];
  for (var i = 0; i < x.length; i++) obj[x[i]] = x[i];
  for (var i = 0; i < y.length; i++) obj[y[i]] = y[i];
  for (var k in obj)                 res.push(obj[k]);
  return res;
}

exports.UniqueArray = function(arr) {
  var uniq = {};
  var tor  = [];
  for (var i = 0; i < arr.length; i++) {
    var a = arr[i];
    if (uniq[a]) tor.push(i);
    else         uniq[a] = true;
  }
  for (var i = tor.length - 1; i >= 0; i--) {
    arr.splice(tor[i], 1);
  }
  return arr;
}

exports.RemoveValueFromArray = function(arr, v) {
  for (var i = 0; i < arr.length; i++) {
    if (arr[i] == v) {
      arr.splice(i, 1);
      return;
    }
  }
}

exports.IfNumberConvert = function(x) {
  if (isNaN(x)) return x;
  else          return Number(x);
}

exports.MsTimeInPerSecond = false;
exports.GetMsTime = function() {
  return ZBH.GetMsTime(exports.MsTimeInPerSecond);
}

exports.IsDefined = function(v) {
  return (typeof(v) !== 'undefined');
}

exports.IsUndefined = function(v) {
  return (typeof(v) === 'undefined');
}

// EXPORT:  UnwindFunction()
// PURPOSE: Call a function with variable length arguments
// NOTE:    1-6 arguments are supported
//
exports.UnwindFunction = function(func, args, next) {
  if        (args.length === 1) {
    Me.net.plugin[func](args[0], next);
  } else if (args.length === 2) {
    Me.net.plugin[func](args[0], args[1], next);
  } else if (args.length === 3) {
    Me.net.plugin[func](args[0], args[1], args[2], next);
  } else if (args.length === 4) {
    Me.net.plugin[func](args[0], args[1], args[2], args[3], next);
  } else if (args.length === 5) {
    Me.net.plugin[func](args[0], args[1], args[2], args[3], args[4], next);
  } else if (args.length === 6) {
    Me.net.plugin[func](args[0], args[1], args[2], args[3], args[4], args[5], 
                        next);
  } else throw(new Error("bad number of args to unwind_function()"));
}

exports.IsOrderedArray = function(cdata) {
  return cdata.E ? cdata.E.ORDERED : false;
}

exports.IsLargeList = function(cdata) {
  return cdata.E ? cdata.E.LARGE : false;
}

exports.IsLateComer = function(cdata) {
  return cdata.E ? (cdata.E["LATE-COMER"] ? true : false) : false;
}

// NOTE: call ZH.IsLargeList() beforehand -> only works on LARGE_LISTs
exports.GetOrderedArrayMin = function(pclm) {
  if (!pclm.E) throw(new Error("ZH.GetOrderedArrayMin LOGIC ERROR"));
  return pclm.E["min"];
}

exports.CmpTraceAverage = function(t1, t2) {
  return (t1.average === t2.average) ? 0 : (t1.average > t2.average) ? 1 : -1;
}

exports.CmpTraceCount = function(t1, t2) {
  return (t1.count === t2.count) ? 0 : (t1.count > t2.count) ? 1 : -1;
}

exports.CmpTraceNumber = function(t1, t2) {
  return (t1.number === t2.number) ? 0 : (t1.number > t2.number) ? 1 : -1;
}

exports.CmpModification = function(k1, k2) {
  return (k1.modification === k2.modification) ? 0 :
         (k1.modification >   k2.modification) ? -1 : 1; // DESC
}

exports.NoOp = function(){} // fully optimized :)

exports.OnErrThrow = function(err, res) {
  if (err) throw(err);
}

exports.OnErrLog = function(err, res) {
  if (err) {
    exports.e('OnErrLog: ' + err.message);
  }
}

exports.RemoveRepeatAuthor = function(authors) {
  var avrsns = {};
  var tor    = [];
  for (var i = 0; i < authors.length; i++) {
    var author = authors[i];
    var avrsn  = author.agent_version;
    if (!avrsns[avrsn]) avrsns[avrsn] = true;
    else                tor.push(i);
  }
  for (var i = tor.length - 1; i >= 0; i--) {
    authors.splice(tor[i], 1);
  }
  return authors;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// JSON ----------------------------------------------------------------------

var DefaultDocumentChannel = "0";

exports.ValidateJson = function(jdata) {
  if (exports.IsUndefined(jdata._id)) {
    return new Error(ZS.Errors.JsonIDMissing);
  }
  if (exports.IsDefined(jdata._meta)) {
    return new Error(ZS.Errors.JsonMetaDefined);
  }
  if (exports.IsDefined(jdata._expire) && isNaN(jdata._expire)) {
    return new Error(ZS.Errors.ExpireNaN);
  }
  if (exports.IsUndefined(jdata._channels)) {
    jdata._channels = [DefaultDocumentChannel];
    return null;
  } else {
    if (typeof(jdata._channels) !== 'object' ||
        !Array.isArray(jdata._channels)) {
      return new Error(ZS.Errors.JsonChannelsNotArray);
    } else if (jdata._channels.length > 1) {
      return new Error(ZS.Errors.JsonChannelsTooMany);
    } else if (jdata._channels.length === 0) {
      jdata._channels = [DefaultDocumentChannel];
      return null;
    } else {
      var schanid = jdata._channels[0];
      if (typeof(schanid) !== 'string') {
        return new Error(ZS.Errors.JsonChannelsFormat);
      }
      return null;
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DOT-NOTATION --------------------------------------------------------------

exports.PathGetDotNotation = function(op_path) {
  var path = '';
  for (var i = 0; i < op_path.length; i++) {
    if (i > 0) path += '.';
    path += op_path[i].K;
  }
  return path;
}

// EXPORT:  LookupByDotNotation()
// PURPOSE: Find the value of a nested json element given its dot-notations
//          (dot-notations means 'x.y.z')
//
exports.LookupByDotNotation = function(json, dots) {
  if (!dots) return json;
  var steps = dots.split('.');
  for (var i = 0; i < steps.length; i++) {
    if (typeof(json) === 'undefined') return;
    json = json[steps[i]];
  }
  return json;
}

exports.SetByDotNotation = function(json, dots, val) {
  if (typeof(dots) === 'number') dots = String(dots);
  var steps = dots.split('.');
  for (var i = 0; i < steps.length - 1; i++) {
    if (typeof(json) === 'undefined') return false;
    json = json[steps[i]];
  }
  if (typeof(json) === 'undefined') return false;
  json[steps[steps.length - 1]] = val;
  return true;
}

exports.DeleteByDotNotation = function(json, dots) {
  if (typeof(dots) === 'number') dots = String(dots);
  var steps = dots.split('.');
  for (var i = 0; i < steps.length - 1; i++) {
    if (typeof(json) === 'undefined') return false;
    json = json[steps[i]];
  }
  if (typeof(json) === 'undefined') return false;
  delete(json[steps[steps.length - 1]])
  return true;
}

exports.ChopLastDot = function(dots) {
  var steps = dots.split('.');
  steps.pop();
  return steps.join('.');
}

exports.ParseOpPath = function(path) {
  var keys = path.split('.');
  for (var i = 0; i < keys.length; i++) {
    var j = Number(parseInt(keys[i], 10));
    if (j == keys[i]) keys[i] = Number(keys[i]); // NOTE: use 2 equal-signs
  }
  return keys;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REPLICATION ---------------------------------------------------------------

exports.SetIsolation = function(val) {
  exports.t('ZH.SetIsolation: ' + val);
  if      (!val)                      exports.Isolation = false;
  else if (typeof(val) === 'boolean') exports.Isolation = val;
  else { // timestampe
    var now = exports.GetMsTime();
    exports.Isolation = (val > now) ? true: false;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CDRT ----------------------------------------------------------------------

// EXPORTS: FormatCrdtForStorage()
// PURPOSE: crdt's have 3 top level fields [_id, _meta, _data]
//
exports.FormatCrdtForStorage = function(id, meta, member) {
  var crdt = { _id   : id,
               _meta : meta};
  if (member) crdt._data = ZConv.AddORSetMember(member, "O", meta);
  return crdt;
}

exports.ValidateDataType = function(dtype) {
  if      (!dtype) return;
  else if (dtype.toUpperCase() === 'LIST') return 'LIST';
}

exports.DebugCrdtData = function(crdt) {
  return ZConv.DebugCrdtData(crdt);
}

exports.SetNewCrdt = function(pc, md, ncrdt, do_gcv) {
  var ks   = pc.ks;
  var gcv  = ncrdt ? ncrdt._meta.GC_version : 0;
  var dbg  = 'ZH.SetNewCrdt: K: ' + ks.kqk;
  if (do_gcv) dbg += " MD.GCV: " + gcv;
  exports.t(dbg);
  pc.ncrdt = ncrdt; // NEW CRDT RESULT
  md.ocrdt = ncrdt; // OLD CRDT -> NEXT APPLY-DELTA
  if (do_gcv) md.gcv = gcv;
}

// NOTE: Used in ./unit_tests/test_lhn_update.js
exports.CreateDummyArrayHeartbeadCrdtData = function(duuid, step, ts) {
  var crdtd = {"_": -1, "#": -1, "@": -1,
               "T": "O",
               "V": {
                   "CONTENTS": {
                       "_": duuid,
                       "#": step,
                       "@": ts,
                       "E": {"MAX-SIZE": -16, "TRIM": 4},
                       "F": "LIST",
                       "T": "A",
                       "V": []
                   }
                 }
               };

  return crdtd;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// JSON RPC ------------------------------------------------------------------

// FUNCTION: create_jsonRpc_body()
// PURPOSE:  Generic JSON RPC 2.0 client request body generator
//
function create_jsonRpc_body(method, user, data, id) {
  var r = {jsonrpc : "2.0",
           method  : method,
           id      : id,
           params  : {
             data : data
           }
          };
  var dkey = exports.DeviceKey;
  if (dkey) r.params.data.device.key = dkey;
  if (user) r.params.authentication  = user;
  return r;
}

// EXPORT:  CreateJsonRpcBody()
// PURPOSE: Wrapper for create_jsonRpc_body()
//
exports.CreateJsonRpcBody = function(method, user, data, id) {
  return create_jsonRpc_body(method, user, data, id);
}

// EXPORT:  ParseJsonRpcCall()
// PURPOSE: Parse JSON RPC's JSON respnse
//
exports.ParseJsonRpcCall = function(method, pdata) {
  var data;
  try {
    data = JSON.parse(pdata);
  } catch(e) {
    exports.e(pdata);
    throw(new Error(method + ' response JSON.Parse() ERROR: ' + e.message));
  }
  if (exports.NetworkDebug) {
    exports.t('PARSED'); exports.t(data);
  }
  return data;
}

function handle_generic_central_response(data) { /* NO-OP */ }

exports.HandleGenericCentralResonse = function(data) {
  handle_generic_central_response(data);
}

exports.WssSendErr = function(err) {
  if (err) exports.e('WSCONN.send: ERROR: ' + err.message);
}

var NextClientRpcId = 1;
exports.GetNextClientRpcID = function() {
  NextClientRpcId += 1;
  return NextClientRpcId;
}

exports.GetNextRpcIDErr = function(err) {
  if (err) exports.e('Get Next RPC-ID: ERROR: ' + err.message);
}

exports.CreateDeviceCollname = function(duuid, cn) {
  return 'D_' + duuid + '_' + cn;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INTERNAL KEY QUEUE HELPERS ------------------------------------------------

exports.CompositeQueueKey = function(ns, cn, key) {
  var kqk = ns + '|' + cn + '|' + key;
  return {kqk : kqk, ns : ns, cn : cn, key : key};
}

exports.ParseKQK = function(kqk) {
  var arr = kqk.split('|');
  return {kqk : kqk, ns : arr[0], cn : arr[1], key : arr[2]};
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZDOC ----------------------------------------------------------------------

exports.DeltaFields = ["modified",
                       "tombstoned",
                       "added",
                       "deleted",
                       "datatyped"
                      ];

exports.DocumentKeywords = ["_id",
                            "_channels",
                            "_expiration",
                            "_server_filter"];

exports.CreatePrettyJson = function(crdt) {
  if (!crdt) return undefined;
  var meta       = crdt._meta;
  var id         = meta.id;
  var json       = crdt._data ? ZConv.CrdtElementToJson(crdt._data) : {};
  var rchans     = meta.replication_channels ? 
                                    exports.clone(meta.replication_channels) :
                                    undefined;
  var expiration = meta.expiration    ?  meta.expiration    : undefined;
  var pfname     = meta.server_filter ?  meta.server_filter : undefined;
  var pjson      = {_id            : id,
                    _channels      : rchans,
                    _expiration    : expiration,
                    _server_filter : pfname};
  for (var jkey in json) pjson[jkey] = json[jkey];
  return pjson;
}

exports.CreateHeadLHN = function() {
  return {"_" : 0, "#" : 0};
}

exports.CreateDentry = function(delta, ks) {
  return  { "_"   : delta._meta.author.agent_uuid,
            "#"   : delta._meta.delta_version,
            ns    : ks.ns,
            cn    : ks.cn,
            delta : delta};
}

exports.InitDelta = function(meta) {
  var delta        = {_meta : meta};
  delta.modified   = [];
  delta.added      = [];
  delta.tombstoned = [];
  delta.deleted    = [];
  delta.datatyped  = [];
  return delta;
}

exports.InitAuthor = function(meta, deps) {
  meta.author       = {agent_uuid : exports.MyUUID}; // NO AGENT_VERSION YET
  meta.dependencies = deps;
  meta["_"]         = exports.MyUUID;                // New Delta AuthorUUID
}

exports.InitMeta = function(ns, cn, key, rchans, expiration, sfname) {
  var meta ={ns                : ns,
             cn                : cn,
             _id               : key,
             author            : {agent_uuid : exports.MyUUID},
             member_count      : 1,
             last_member_count : 1,
            };
  if (rchans)     meta.replication_channels = rchans;
  if (expiration) meta.expiration           = expiration;
  if (sfname)     meta.server_filter        = sfname;
  return meta;
}

// EXPORT:  InitPreCommitInfo()
// PURPOSE: Pre Central Delta Commit Info
//
exports.InitPreCommitInfo = function(op, ks, dentry, xcrdt, edata) {
  return {step       : 0,
          op         : op,
          ks         : ks,
          dentry     : dentry ? exports.clone(dentry) : null,
          xcrdt      : xcrdt  ? exports.clone(xcrdt)  : null,
          extra_data : edata,
          metadata   : {}};
}

exports.CreateDeltaInfo = function(meta) {
  return {device_uuid   : meta.author.agent_uuid,
          delta_version : meta.delta_version};
}

exports.CreateAvrsn = function(auuid, avnum) {
  return auuid +'|' + avnum;
} 

exports.GetAvnum = function(avrsn) {
  if (!avrsn) return 0;
  else {
    var res   = avrsn.split('|');
    var avnum = Number(res[1]);
    return avnum;
  }
}

exports.GetAvUUID = function(avrsn) {
  if (!avrsn) return 0;
  else {
    var res   = avrsn.split('|');
    var auuid = Number(res[0]);
    return auuid;
  }
}

exports.CheckOOODependencies = function(ks, avrsn, deps, mdeps) {
  if (!deps) return false;
  var auuid = exports.GetAvUUID(avrsn);
  for (var suuid in mdeps) {
    suuid = Number(suuid);
    if (suuid !== auuid) {
      var mavrsn = mdeps[suuid];
      var mavnum = exports.GetAvnum(mavrsn);
      var davrsn = deps[suuid];
      var davnum = exports.GetAvnum(davrsn);
      if (davnum > mavnum) {
        exports.e('OOODEP: check_ooo_storage_deps: K: ' + ks.kqk + 
                  ' MAV: ' + mavrsn + ' DAV: ' + davrsn);
        return true;
      }
    }
  }
  return false;
}

exports.GetReplicationChannels = function(meta) {
  if (exports.IsDefined(meta) && exports.IsDefined(meta.replication_channels)) {
    return meta.replication_channels;
  } else {
    return [];
  }
}

function get_gc_version(meta) {
  var dgcv = meta.GC_version;
  if (exports.IsUndefined(dgcv)) return 0;
  else                           return dgcv;
}

exports.GetDentryGCVersion = function(dentry) {
  var meta = dentry.delta._meta;
  return get_gc_version(meta);
}

exports.GetCrdtGCVersion = function(meta) {
  return get_gc_version(meta);
}

exports.CalculateRchanMatch = function(pc, ocrdt, dmeta) {
  if (!ocrdt) {
    pc.rchan_match = false;
    return true;
  } else {
    var ometa    = ocrdt._meta;
    var orchans  = ometa.replication_channels;
    var oschanid = orchans[0];
    var drchans  = dmeta.replication_channels;
    var dschanid = drchans[0];
    if (oschanid !== dschanid) return false;
    else {
      pc.rchan_match = true;
      return true;
    }
  }
}

exports.getFullTypeName = function(ctype) {
  if      (ctype === "A") return "array";
  else if (ctype === "O") return "object";
  else if (ctype === "N") return "number";
  else if (ctype === "S") return "string";
  else throw(new Error("ZH.getFullTypeName unrecognized type"));
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

function build_versions_string(gres, next) {
  var cavrsns = {};
  if (gres.length !== 0) {
    cavrsns = exports.clone(gres[0]);
    delete(cavrsns._id);
  }
  var savrsns = '';
  for (var duuid in cavrsns) {
    if (savrsns.length !== 0) savrsns += ',';
    var avrsn = cavrsns[duuid];
    savrsns += avrsn;
  }
  next(null, savrsns);
}

function debug_get_agent_versions(plugin, collections, ks, next) {
  if (exports.AmCentral) {
    var kkey = ZS.GetKeyToCentralAgentVersion(ks.kqk);
    plugin.do_get(collections.global_coll, kkey, function(gerr, gres) {
      if (gerr) next(gerr, null);
      else      build_versions_string(gres, next);
    });
  } else {
    next(null, '');
  }
}

function create_debug_json_from_crdt(crdt) {
  if (!crdt) return 'EMPTY';
  var json  = exports.CreatePrettyJson(crdt);
  var cjson = exports.clone(json);
  for (var i = 0; i < exports.DocumentKeywords.length; i++) { // JUST SHOW DATA
    var kw = exports.DocumentKeywords[i];
    delete(cjson[kw]);
  }
  return JSON.stringify(cjson);
}

exports.DebugCrdt = function (plugin, collections,
                              ks, crdt, prfx, with_avs, next) {
  if (!with_avs) {
    var sjson = create_debug_json_from_crdt(crdt);
    exports.l('DebugCrdt: ' + prfx + ks.kqk + '++' + sjson);
    next(null, null);
  } else {
    debug_get_agent_versions(plugin, collections, ks, function(gerr, savrsns) {
      if (gerr) next(gerr, null);
      else {
        var sjson = create_debug_json_from_crdt(crdt);
        exports.l('DebugCrdt: ' + prfx + ks.kqk + '+' + savrsns + '+' + sjson);
        next(null, null);
      }
    });
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATABASE ------------------------------------------------------------------

exports.DecrementSet = function(plugin, coll, ckey, fname, val, next) {
  if (val < 1) plugin.do_unset_field(coll, ckey, fname, next);
  else         plugin.do_set_field  (coll, ckey, fname, val, next);
}

exports.CreateNetPerRequest = function(player) {
  return player.net;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NETWORK SERIALIZATION & COMPRESSION ---------------------------------------

exports.DebugAllNetworkSerialization = false;

exports.NetworkSerialize = function(data) {
  if (exports.NetworkDebug) {
    var pr = (data.method !== 'GeoLeaderPing' && data.method !== 'GeoDataPing');
    if (exports.DebugAllNetworkSerialization) pr = true;
    if (pr) {
      exports.t('SEND: ' + exports.GetMsTime()); exports.t(data);
    }
  }
  var cdata = JSON.stringify(data);
  return cdata;
}

exports.NetworkDeserialize = function(cdata) {
  //exports.t('JUST RECEIVED -> PARSE');
  try {
    var data = JSON.parse(cdata);
    if (exports.NetworkDebug === true) {
      var pr = (data.method !== 'GeoLeaderPing' &&
                data.method !== 'GeoDataPing');
      if (exports.DebugAllNetworkSerialization) pr = true;
      if (pr) {
        exports.t('NETWORK: RECEIVED: ' + exports.GetMsTime()); exports.t(data);
      }
    }
    return data;
  } catch(e) {
    if (exports.NetworkDebug) {
      exports.e('PROGRAM ERROR: JSON PARSE: ' + e.message);
      exports.e(cdata);
    }
    throw(e);
  }
}

exports.ConvertBodyToBinaryNetwork = function(cbody) {
  var clen = cbody.length;
  var cbuf = Buffer(clen + 4);
  cbuf.writeUInt32LE(clen, 0);
  cbuf.write(cbody, 4);
  return cbuf;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SECURITY TOKENS -----------------------------------------------------------

//TODO: this can be made MUCH more secure
exports.GenerateDeltaSecurityToken = function(dentry) {
  var meta    = dentry.delta._meta;
  var created = meta.author.created;
  var text    = exports.ClusterMethodKey + '|' + created;
  var md5     = crypto.createHash("md5");
  var tkn     = md5.update(text).digest("hex");
  exports.t('generate_delta_security_token: C: ' + created + ' T: ' + tkn);
  return tkn;
}

//TODO: this can be made MUCH more secure
exports.GenerateKeySecurityToken = function(ks) {
  var text = exports.ClusterMethodKey + '|' + ks.kqk;
  var md5  = crypto.createHash("md5");
  var tkn  = md5.update(text).digest("hex");
  exports.t('generate_key_security_token: K: ' + ks.kqk + ' T: ' + tkn);
  return tkn;
}

exports.GetKeyHash = function(str) {
  str      = String(str);
  var res  = crypto.createHash("md5").update(str).digest("hex");
  var end  = res.substr(22); // Last 10 HEX of a length 32 HEX
  return     parseInt(end, 16);
}

var UsingBcrypt = false;

exports.HashPassword = function(password, next) {
  if (UsingBcrypt) {
    throw(new Error("BCRYPT DISABLED"));
    bcrypt.genSalt(10, function(gerr, salt) {
      if (gerr) next(gerr, null);
      else      bcrypt.hash(password, salt, next);
    });
  } else {
    next(null, password);
  }
}

exports.ComparePasswordHash = function(a, b) {
  if (UsingBcrypt) {
     return bcrypt.compareSync(a, b);
  } else {
   return (a === b);
  }
}

exports.B64Encode = function(txt) {
  return new Buffer(txt).toString('base64')
}

exports.B64Decode = function(btxt) {
  return new Buffer(btxt, 'base64').toString('ascii');
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STANDARD INITIALIZERS -----------------------------------------------------

exports.CreateSub = function(duuid) {
  return {UUID : Number(duuid)}
}

exports.CreateMySub = function() {
  return {UUID : Number(exports.MyUUID)}
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBAL VARIABLES ----------------------------------------------------------

exports.MyUUID      = null;
exports.AmCentral   = false;
exports.AmRouter    = false;
exports.AmStorage   = false;
exports.AmBoth      = false;

exports.AmBrowser   = false;

exports.AmClient    = false;

exports.Agent       = null;
exports.Browser     = null;
exports.Central     = null;

exports.Isolation   = false;

exports.GetAgentOffline = function() {
  //exports.t('OFF: ISOL: '+exports.Isolation+' SYNC: '+exports.Agent.synced);
  return exports.Isolation || !exports.Agent.synced;
}

exports.MyDataCenter = null;

exports.Discovery    = null;

exports.WildCardUser = "*";

exports.MaxDataCenterUUID = 10000;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// TEST VARIABLES ------------------------------------------------------------

//exports.TestNotReadyAgentRecheck = true;
//exports.CentralReapTest          = true;
//exports.CentralDoGCAtFourTest    = true;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FEATURE KILL SWITCHES -----------------------------------------------------

exports.DisableBroadcastUpdateSubscriberMap = true;

exports.DisableDeviceKeyCheck = true;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// VOTE GLOBAL VARIABLES -----------------------------------------------------

exports.ClusterVoteInProgress = false;
exports.YetToClusterVote      = true;

exports.GeoVoteInProgress     = false;
exports.YetToGeoVote          = true;

exports.MyClusterLeader       = null;
exports.AmClusterLeader       = false;

exports.ClusterNetworkPartitionMode  = false;
exports.GeoNetworkPartitionMode      = false;
exports.GeoMajority                  = true; // default true


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEAD-OR-ALIVE SETTINGS ----------------------------------------------------

exports.FixLogActive     = false;

exports.DatabaseDisabled = false; // DEBUG setting


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT LAST CENTRAL CREATED ------------------------------------------------

exports.AgentLastCentralCreated = {};

exports.GetAgentLastCentralCreated = function(guuid) {
  return exports.AgentLastCentralCreated[guuid];
}

exports.GetLastCentralUpdateTime = function() {
  return exports.GetAgentLastCentralCreated(exports.MyDataCenter);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NETWORK SETTINGS ----------------------------------------------------------

// EXPORT:  ClusterMethodKey
// PURPOSE: Used to validate Cluster* [Delta,Cache,,,] come from a ClusterNode
//
//TODO move to zcentral.js
exports.ClusterMethodKey = 'ABCDEFGHIJKLMNOPqrstuvwxyz';

// VARIABLE: central_https_config
// PURPOSE:  Central https configuration for zcentral.js
//
var central_https_config = {
  ssl      : true,
  ssl_key  : './ssl/server.private.key',
  ssl_cert : './ssl/server.crt'
};

// EXPORT:  CentralConfig
// PURPOSE: IP & port that Central will listen on
//
exports.CentralConfig = central_https_config;

// VARIABLE: agent_https_config
// PURPOSE:  Agent https configuration for zagent.js
//
var agent_https_config = {
  ssl      : true,
  ssl_key  : './ssl/server.private.key',
  ssl_cert : './ssl/server.crt'
};

// EXPORT:  AgentConfig
// PURPOSE: IP & port that Devices will listen on
//
exports.AgentConfig   = agent_https_config;

 // Overridable func to handle AgentOnline response on first time run
 //  (device.uuid is returned from this device's AgentMaster-ClusterNode)
exports.InitNewDevice = function(arg, next) { next(null, arg); }

var BackoffSecsMin =  5; //TODO ConfigFile
var BackoffSecsMax = 10; //TODO ConfigFile
exports.GetBackoffSeconds = function() {
  return BackoffSecsMin +
         (BackoffSecsMax - BackoffSecsMin) * Math.random();
}

exports.CentralMaster = {};

exports.SameMaster = function(a, b) {
  return (a.wss.server.hostname === b.wss.server.hostname &&
          a.wss.server.port === b.wss.server.port);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEVICE KEY ----------------------------------------------------------------

exports.DeviceKey = undefined; // CENTRAL give each DEVICE a key

exports.CreateDeviceKey = function() {
  var buf = crypto.randomBytes(16);
  return buf.toString('hex');
}

exports.DeviceKeyMatch = function(device) {
  var duuid = device.uuid;
  var dkey  = device.key;
  if (duuid === -1) return false;
  if (!dkey)        return false;
  var cdkey = exports.CentralDeviceKeyMap[duuid];
  return (cdkey === dkey);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL IN-MEMORY MAPS ----------------------------------------------------

exports.CentralIsolatedAgents = {}; // Each agent's isolation (PER-AGENT)

exports.CentralWsconnMap      = []; // Maps DUUID to WebSocket (PER-AGENT)
exports.CentralSocketMap      = []; // Maps DUUID to Socket (PER-AGENT)
exports.CentralHttpsMap       = []; // Maps DUUID to HTTPS (PER-AGENT)
exports.CentralHttpsKeyMap    = []; // Maps DUUID to HTTPS-KEY (PER-AGENT)
exports.ReverseHttpsMap       = []; // Maps URI to DUUID (PER-AGENT)
exports.CentralDeviceKeyMap   = []; // Maps DUUID to DEVICE-KEY (PER-AGENT)

exports.KeepAliveMSecs        = 600000; // 10 MINUTES

function debug_is_subscriber_offline(isolated, wsconn, sock, hso) {
  exports.l('OFFLINE: I: ' + isolated + ' W: ' + wsconn + ' S: ' + sock +
            'H: ' + hso);
}

exports.IsSubscriberOffline = function(suuid) {
  var isolated = exports.CentralIsolatedAgents[suuid];
  var wsconn   = exports.CentralWsconnMap     [suuid];
  var sock     = exports.CentralSocketMap     [suuid];
  var hso      = exports.CentralHttpsMap      [suuid]
  //debug_is_subscriber_offline(isolated, wsconn, sock, hso);
  if      (isolated)              return true;
  else if (wsconn || sock || hso) return false;
  else                            return true;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT IN-MEMORY MAPS ------------------------------------------------------

exports.AgentWssRequestMap = []; // Maps outgoing Device WSS requests by 
                                 //  ID to callbacks (PER-REQUEST)

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BOTH AGENT & CENTRAL IN-MEMORY MAPS ---------------------------------------

exports.UserPasswordMap    = {}; // Stores RAW passwords in memory (PER-USER)


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT, AGENT, CENTRAL AUTHENTICATION -------------------------------------

exports.NobodyAuth  = {username : 'nobody', password : 'nobody'};
exports.BrowserAuth = exports.NobodyAuth; // Browser's current user


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT ABSTRACTIONS -------------------------------------------------------

exports.HandleIsUserStationed = function(gerr, susers, username, next) {
  if (gerr) next(gerr, null);
  else {
    if (!susers || !susers["users"]) next(null, false);
    else {
      var stationed = false;
      var users     = susers["users"];
      for (var i = 0; i < users.length; i++) {
        if (users[i] === username) {
          stationed = true;
          break;
        }
      }
      next(null, stationed);
    }
  }
}

exports.HandleIsSubscribed = function(gerr, gres, schanid, next) {
  if (gerr) next(gerr, null);
  else {
    var subs = gres.subscriptions;
    var hit  = false;
    if (subs) {
      for (var i = 0; i < subs.length; i++) {
        if (subs[i] == schanid) {
          hit = true;
          break;
        }
      }
    }
    next(null, hit);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOCAL-STORAGE WRAPPER -----------------------------------------------------

// EXPORT:  LStorage
// PURPOSE: Wrapper for LocalStorage, abstracts away JSON de/serialization
//
exports.LStorage = function() { // Constructor -> NO-OP
  this.set = function(cname, key, value) {
    var lkey = cname + "_" + key;
    //exports.t('Storage: set: svalue: ' + JSON.stringify(value));
    localStorage.setItem(lkey, JSON.stringify(value));
  }
  this.get = function(cname, key) {
    var lkey   = cname + "_" + key;
    var jvalue = localStorage.getItem(lkey);
    //exports.t('Storage: get: jvalue: ' + jvalue);
    if (!jvalue) return null;
    var value;
    try {
      value = JSON.parse(jvalue);
    } catch(e) {
      return null;
    }
    return value;
  }
  this.scan = function() {
    var items = {};
    for (var i = 0; i < localStorage.length; i++){
      var key = localStorage.key(i);
      var val = localStorage.getItem(key);
      items[key] = val;
    }
    return items;
  }
  this.remove = function(cname, key) {
    var lkey = cname + "_" + key;
    localStorage.removeItem(lkey);
  }
  this.clear = function() {
    localStorage.clear();
  }
  this.dump = function() {
    exports.t('>>>>>>>>>>>>>>>>>>>>>>>>> DUMP');
    for(var i in localStorage) {
      var x = localStorage[i];
      exports.t(i + ' : ' + x);
    }
    exports.t('<<<<<<<<<<<<<<<<<<<<<<<<< DUMP');
  }
  this.debug = function(log) {
    log('>>>>>>>>>>>>>>>>>>>>>>>>> DUMP');
    for(var i in localStorage) {
      var x = localStorage[i];
      log(i + ' : ' + x);
    }
    log('<<<<<<<<<<<<<<<<<<<<<<<<< DUMP');
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOGGING HELPERS -----------------------------------------------------------

exports.SummarizeCrdt = function(kqk, meta) {
  var gcv    = meta.GC_version ? meta.GC_version : 0;
  var nbytes = meta.num_bytes  ? meta.num_bytes  : 0;
  return ' K: ' + kqk + ' GCV: ' + gcv + ' #B: ' + nbytes;
}

exports.SummarizeDelta = function(kqk, dentry) {
  var meta  = dentry.delta._meta;
  var guuid = meta.author.datacenter;
  var avrsn = meta.author.agent_version;
  var gcv   = meta.GC_version ? meta.GC_version : 0;
  return ' K: ' + kqk + ' AV: ' + avrsn + ' GCV: ' + gcv + ' GU: ' + guuid;
}

exports.SummarizePublish = function(suuid, kqk, dentry) {
  return ' U: ' + suuid + exports.SummarizeDelta(kqk, dentry);
}

exports.SummarizeKS = function(ks) {
  delete(ks.ns);
  delete(ks.cn);
  delete(ks.key);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CHAOS MODES ---------------------------------------------------------------

//TODO move to zchaos.js
exports.ChaosMode = 0;
exports.ChaosDescriptions = [
  'NONE', // NOTE: leave unimplemented -> i.e. NO CHAOS                  //  0
  'CENTRAL AGENT-DELTA-NOT-SYNC COMMIT ROLLBACK in STEP 7',
  'CENTRAL AGENT-DELTA-NOT-SYNC COMMIT ROLLBACK in STEP 8',
  'GEO-PING ISOLATION (LEADER & DATA)',
  'NOT IMPLEMENTED',
  'GEO_COMMIT_DELTA ON ANY GEO_DELTA_ACK',
  'DROP CLUSTER VOTE COMMIT',
  'DROP GEO VOTE COMMIT',
  'DROP GEO-SUBSCRIBER-COMMIT-DELTA',
  'NOT IMPLEMENTED',
  'CENTRAL DROP GEO-DELTA',                                              // 10
  'CENTRAL DROP GEO-NEED-MERGE',
  'CENTRAL DROP AGENT-NEED-MERGE',
  'NOT IMPLEMENTED',
  'CENTRAL REDIRECT AGENT-ONLINE TO OTHER GEO-CLUSTER',
  'CENTRAL DO NOT GEO-SYNC-KEYS',
  'CENTRAL DROP DATACENTER-ONLINE',
  'CENTRAL DROP AGENT-DELTA',
  'NOT IMPLEMENTED',
  'NOT IMPLEMENTED',
  'ACK GEO DELTA OOO -> NO-OP',                                          // 20
  'NOT IMPLEMENTED',
  'DROP SUBSCRIBER COMMIT DELTA',
  'NOT IMPLEMENTED',
  'WAIT 30 SECONDS FOR ROUTER TO SEND GEO_NEED_MERGE',
  'WAIT 30 SECONDS TO PROCESS STORAGE_GEO_NEED_MERGE',
  'WAIT 10 SECONDS TO PUSH STORAGE CLUSTER-APPLY-DELTA & AGENT-NEED-MERGE',
  'NOT IMPLEMENTED',
  'NOT IMPLEMENTED',
  'NOT IMPLEMENTED',
  'NOT IMPLEMENTED',                                                     // 30
  'CENTRAL DATACENTER-MERGE FAILURE in STEP 1',
  'STORAGE PRIMARY SKIP FIX OOO GCV DELTA',
  'ROUTER DELAY SEND SUBSCRIBER-DELTA BY 10 SECONDS',
  'NOT IMPLEMENTED',
  'HANDLE ALTERNATING SUBSCRIBER-DELTAS',
];
exports.ChaosMax = exports.ChaosDescriptions.length - 1;


exports.PathologicalMode = 0;
exports.PathologicalDescriptions = [
  'NONE', // NOTE: leave unimplemented -> i.e. NO PATHOLOGICALNESS           0
  'AGENT-DELTA ROLLBACK in STEP 1',
 ];
exports.PathologicalMax = exports.PathologicalDescriptions.length - 1;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HTML ----------------------------------------------------------------------

exports.GetQueryVariable = function(variable) {
  var query = window.location.search.substring(1);
  var vars = query.split('&');
  for (var i = 0; i < vars.length; i++) {
    var pair = vars[i].split('=');
    if (decodeURIComponent(pair[0]) == variable) {
        return decodeURIComponent(pair[1]);
    }
  }
  return null;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RANDOM --------------------------------------------------------------------

function json_parse_or_val(txt) {
  var json;
  try {
    json = JSON.parse(txt);
  } catch(e) {
    if (isNaN(txt)) return txt;
    else            return Number(txt);
  }
  return json;
}

String.prototype.replaceAll = function (find, replace) {
  var str = this;
  return str.replace(new RegExp(find.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&'), 'g'), replace);
};

String.prototype.insert = function (index, string) {
  if (index > 0) {
    return this.substr(0, index) + string + this.substr(index, this.length);
  } else return string + this;
};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MEMCACHE-GET DEFINITIONS --------------------------------------------------

exports.GetMemcacheDirectoryMembers = function(mkey) {
  return mkey + '__MEMBER_DIRECTORY';
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOGGING WRAPPER -----------------------------------------------------------

exports.CloseLogFile = function(next) {
  ZLog.CloseLogFile();
}

exports.LogToConsole = false;

exports.p = function(json) {
  ZLog.p(json);
}

exports.l = function(x) {
  ZLog.l(x);
}

exports.t = function(x) {
  ZLog.t(x);
}

exports.e = function(x) {
  ZLog.e(x);
}

exports.SetLogLevel = function(level) {
  ZLog.SetLogLevel(level);
}

exports.Debug = true;

var DebugAll = false;
DebugAll = true;
if (DebugAll) {
  //exports.LogToConsole = true;
  exports.Debug        = true;
  exports.NetworkDebug = true;
}
//exports.LogToConsole = true;

// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZH']={} : exports);

