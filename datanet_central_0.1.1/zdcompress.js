"use strict";

var ZS  = require('./zshared');
var ZH  = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXAMPLES ------------------------------------------------------------------

var ExampleMeta = { // NOT: NOT-USED -> DOCUMENTATION
                    "@" : 1462408764011,
                    "GC_version" : 0,
                    "_" : 1000000001,
                    "_id" : "INCREMENT_HEARTBEAT",
                    "agent_received" : 1462408764015,
                    "agent_sent" : 1462408764013,
                    "author" : {
                            "agent_uuid" : 1000000001,
                            "agent_version" : "1000000001|13",
                            "created" : 1462408764020,
                            "datacenter" : "D1",
                            "username" : "hbuser"
                    },
                    "cn" : "statistics",
                    "created" : {
                            "D1" : 1462408764020
                    },
                    "delta_version" : 100000000100012,
                    "dependencies" : {
                            "1000000002" : "1000000002|1",
                            "1000000030" : "1000000030|1"
                    },
                    "document_creation" : {
                            "#" : 100000000200000,
                            "@" : 1462393113401,
                            "_" : 1000000002
                    },
                    "member_count" : 55,
                    "ns" : "production",
                    "num_bytes" : 313,
                    "replication_channels" : [
                            1
                    ],
                    "subscriber_received" : 1462408764055,
                    "subscriber_sent" : 1462408764053,
                    "xaction" : "5607d89893df03d5bc2e636a1fa540ef"
                  };

var ExampleArray = { // NOT: NOT-USED -> DOCUMENTATION (ARRAY w/ F & E)
          "_": 100001,
          "#": 10000100001,
          "@": 1465930823995,
          "T": "A",
          "V": [],
          "F": "LIST",
          "E": {
              "MAX-SIZE": -16,
              "TRIM": 4
          }
  };

var ExampleMember = { // NOT: NOT-USED -> DOCUMENTATION (ARRAY-MEMBER)
          "_" : 1000000030,
          "#" : 100000003000002,
          "@" : 1462409088814,
          "S" : [
                  1462409088814
          ],
          "T" : "N",
          "V" : {
                  "N" : 0,
                  "P" : 1
          },
          "<" : {
                  "#" : 0,
                  "_" : 0
          }
  };


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZATION ------------------------------------------------------------

// NOTE must match ./c_client/dcompress.cpp
var MetaFields = [ "@", "_", "_id", "AUTO", "GC_version",
                   "DO_DS", "DS_DTD", "DO_GC", "DO_IGNORE", "DO_REAP",
                   "DO_REORDER", "OOO_GCV",
                   "agent_received", "agent_sent", "author", "auto_cache",
                   "cn", "created", "delta_version",
                   "dependencies", "dirty_central", "document_creation",
                   "expire", "expiration", "from_central",
                   "geo_received", "geo_sent", "initial_delta", "is_geo",
                   "last_member_count", "member_count", "ns", "num_bytes",
                   "op_count", "overwrite",
                   "reap_gc_version",
                   "reference_GC_version", "reference_ignore",
                   "reference_uuid", "reference_version",
                   "remove", "removed_channels",
                   "replication_channels", "server_filter",
                   "subscriber_received", "subscriber_sent", "xaction"];

var MetaFieldsMap = {}

function populate_MetaFieldsMap() {
  for (var i = 0; i < MetaFields.length; i++) {
    MetaFieldsMap[MetaFields[i]] = i
  }
}
populate_MetaFieldsMap();

var CrdtTypes = ["O", "A", "N", "S"];
var CrdtTypesMap = {}
function populate_CrdtTypesMap() {
  for (var i = 0; i < CrdtTypes.length; i++) {
    CrdtTypesMap[CrdtTypes[i]] = i;
  };
}
populate_CrdtTypesMap();


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARRAY F & E COMPRESSION ---------------------------------------------------

var InternalDataTypes = {"NONE"       : 0,
                         "LIST"       : 1,
                         "LARGE_LIST" : 2};

function get_internal_data_type(data) {
  var f = data["F"];
  if (!f) return InternalDataTypes["NONE"];
  else {
    var e = data["E"];
    if      (e["MAX-SIZE"]) return InternalDataTypes["LIST"];
    else if (e["LARGE"])    return InternalDataTypes["LARGE_LIST"];
    else                    return InternalDataTypes["NONE"];
  }
}

function encode_array_f_e(m, data) {
  var fval = get_internal_data_type(data);
  m.push(fval);
  if (!fval) return;
  var e = data["E"];
  if (fval === InternalDataTypes["LIST"]) {
    m.push(Number(e["MAX-SIZE"]));
    if (e["TRIM"]) m.push(Number(e["TRIM"]));
    else           m.push(0);
  } else { // LARGE_LIST
    m.push(Number(e['PAGE-SIZE']));
  }
}

function decode_array_f_e(m, udata) {
  var fval = m.shift();
  if (fval) { // DECODE F & E
    udata["F"] = "LIST"; // NOTE: currently only type is LIST
    udata["E"] = {};
    if (fval === InternalDataTypes["LIST"]) {
      udata["E"]["MAX-SIZE"] = m.shift();
      var trim               = m.shift();
      if (trim) udata["E"]["TRIM"] = trim;
    } else { // LARGE_LIST
      udata["E"]["LARGE"]     = true;
      udata["E"]["PAGE-SIZE"] = m.shift();
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// META COMPRESSION ----------------------------------------------------------

function compress_crdt_meta(cmeta, meta) {
  var vmeta = {};
  for (var mfield in meta) {
    var cnum    = MetaFieldsMap[mfield];
    vmeta[cnum] = meta[mfield]; 
  }
  cmeta.F = [];
  cmeta.V = [];
  for (var cnum in vmeta) {
    var val = vmeta[cnum];
    cmeta.F.push(Number(cnum));
    cmeta.V.push(val);
  }
}

function decompress_crdt_meta(umeta, zmeta) {
  while (zmeta.F.length) {
    var cnum      = zmeta.F.shift();
    var val       = zmeta.V.shift();
    var mfield    = MetaFields[cnum];
    umeta[mfield] = val;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CRDT MEMBER COMPRESSION ---------------------------------------------------

function compress_crdt_type(ctype) {
  return CrdtTypesMap[ctype];
}

function decompress_crdt_type(tnum) {
  return CrdtTypes[tnum];
}


function compress_crdt_member(cdata, data) {
  var ctype = data["T"];
  var vdata = {};
  vdata.M   = [];
  vdata.M.push(compress_crdt_type(ctype));
  vdata.M.push(data["_"]);
  vdata.M.push(data["#"]);
  vdata.M.push(data["@"]);
  encode_array_f_e(vdata.M, data);
  if (data["S"]) { // ARRAY-MEMBER
    var xval = data["X"] ? (data["A"] ? 2 : 1) : 0;
    vdata.M.push(xval);           // FIRST X/A
    vdata.M.push(data["<"]["_"]); // THEN LHN
    vdata.M.push(data["<"]["#"]);
    var s = ZH.clone(data["S"]);  // LAST (variable-length) S[]
    while (s.length) {
      var el = s.shift();
      vdata.M.push(el);
    }
  }
  if        (ctype === "S") {
    vdata.V = data.V;
  } else if (ctype === "N") {
    vdata.V = [data.V.P, data.V.N];                       // NOTE: NESTED ARRAY
  } else if (ctype === "O") {
    vdata.V = {};                                               // NOTE: OBJECT
    for (var k in data.V) {
      vdata.V[k] = [];                                          // NOTE: ARRAY
      compress_crdt_member(vdata.V[k], data.V[k]);
    }
  } else if (ctype === "A") {
    vdata.V = [];                                               // NOTE: ARRAY
    for (var i = 0; i < data.V.length; i++) {
      vdata.V[i] = [];                                          // NOTE: ARRAY
      compress_crdt_member(vdata.V[i], data.V[i]);
    }
  }
  //ZH.e('compress_crdt_member: M: ' + JSON.stringify(vdata.M));
  cdata.push(vdata.M);
  cdata.push(vdata.V);
}

function decompress_crdt_member(udata, zcval) {
  if (!zcval) return;
  var m      = zcval.shift();
  var v      = zcval.shift();
  //ZH.e('decompress_crdt_member: M: ' + JSON.stringify(m));
  udata["T"] = decompress_crdt_type(m.shift());
  udata["_"] = m.shift();
  udata["#"] = m.shift();
  udata["@"] = m.shift();
  decode_array_f_e(m, udata);
  if (m.length) { // ARRAY-MEMBER
    var xval        = m.shift();
    if (xval >=  1) udata["X"] = true;
    if (xval === 2) udata["A"] = true;
    udata["<"]      = {};
    udata["<"]["_"] = m.shift();
    udata["<"]["#"] = m.shift();
    udata["S"]      = [];
    while(m.length) {
      udata["S"].push(m.shift());
    }
  }
  var ctype  = udata["T"];
  if        (ctype === "S") {
    udata.V = v;
  } else if (ctype === "N") {
    udata.V   = {};
    udata.V.P = v.shift();
    udata.V.N = v.shift();
  } else if (ctype === "O") {
    udata.V = {};                                               // NOTE: OBJECT
    for (var k in v) {
      udata.V[k] = {};                                          // NOTE: OBJECT
      decompress_crdt_member(udata.V[k], v[k]);
    }
  } else if (ctype === "A") {
    udata.V = [];                                               // NOTE: ARRAY
    for (var i = 0; i < v.length; i++) {
      udata.V[i] = {};                                          // NOTE: OBJECT
      decompress_crdt_member(udata.V[i], v[i]);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CRDT COMPRESSION ----------------------------------------------------------

function compress_crdt(crdt) {
  var cmeta = {};
  var meta  = crdt._meta;
  compress_crdt_meta(cmeta, meta);
  
  var cdata;
  var cval  = crdt._data;
  if (cval) {   // SEPARATE-CRDT has NO DATA 
    cdata = [];                                                 // NOTE: ARRAY
    compress_crdt_member(cdata, cval);
  }
  
  var zcrdt = {_id   : crdt._id,
               _meta : cmeta,
               _data : cdata};
  return zcrdt;
}

function decompress_crdt(zcrdt) {
  if (!zcrdt) return null;
  var umeta = {};
  var zmeta = zcrdt._meta;
  decompress_crdt_meta(umeta, zmeta);
  
  var udata;
  var zcval = zcrdt._data;
  if (zcval) {  // SEPARATE-CRDT has NO DATA 
    udata = {};                                                 // NOTE: OBJECT
    decompress_crdt_member(udata, zcval);
  }
  
  var ucrdt = {_id   : zcrdt._id,
               _meta : umeta,
               _data : udata};
  return ucrdt;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// API -----------------------------------------------------------------------

exports.CompressCrdtMember = function(member) {
  var cdata = [];
  compress_crdt_member(cdata, member);
  return cdata;
}

exports.CompressCrdt = function(crdt) {
  return compress_crdt(crdt);
}

exports.DecompressCrdtMember = function(zmember) {
  if (!zmember) return null;
  var udata = {};
  decompress_crdt_member(udata, zmember);
  return udata;
}

exports.DecompressCrdt = function(zcrdt) {
  return decompress_crdt(zcrdt);
}

