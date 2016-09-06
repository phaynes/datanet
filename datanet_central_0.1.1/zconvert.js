"use strict";

var ZH;
if (typeof(exports) !== 'undefined') {
  ZH    = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

var SupportedMetadataFields = {"MAX-SIZE"   : true,
                               "TRIM"       : true,
                               "ORDERED"    : true,
                               "LARGE"      : true,
                               "PAGE-SIZE"  : true,
                               "LATE-COMER" : true,
                              };

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// JSON-TO-CRDT --------------------------------------------------------------

// FUNCTION: add_OR_Set_member()
// PURPOSE:  Adds an OR-SET member
// NOTE:     ALL CRDT data (Array, Object, Number, String) are OR-Set MEMBERS
//
function add_OR_Set_member(val, type, meta, adding) {
  var member = {"_"  : -1,      // _  -> DeviceUUID
                "#"  : -1,      // #  -> Device's op-counter
                "@"  : -1,      // @  -> Element's creation timestamp
                "T"  : type,    // T  -> Type: [A,O,N,S,X]
                "V"  : val   }; // V  -> Value Object (contains P/N,S,etc...)
  if (adding === true) member["+"] = true; // Member added during this xaction
  if (typeof(meta) !== 'undefined') meta.member_count = meta.member_count + 1;
  return member;
}

exports.AddORSetMember = function(val, type, meta, adding) {
  return add_OR_Set_member(val, type, meta, adding);
}

function add_String_member(val, meta, adding) {
  return add_OR_Set_member(val, "S", meta, adding);
}

// NOTE: When an array's value is set by an index greater than 
//        the length of the array, javascript pads the inbetween (empty)
//        positions w/ nulls, to be compliant, the same is done
function add_Null_member(meta, adding) {
  return add_OR_Set_member(null, "X", meta, adding);
}

function add_Number_member(num, meta, adding) {
  if (num >= 0) {
    return add_OR_Set_member({"P" : num, "N" : 0},        "N", meta, adding);
  } else {
    return add_OR_Set_member({"P" : 0, "N" : (num * -1)}, "N", meta, adding);
  }
}

function add_Array_member(val, meta, adding) {
  var data = convert_JsonArray_to_crdt(val, meta, adding);
  return add_OR_Set_member(data, "A", meta, adding);
}

function add_Object_member(val, meta, adding) {
  var data = convert_JsonObject_to_crdt(val, meta, adding);
  return add_OR_Set_member(data, "O", meta, adding);
}

function check_metadata_fields(dmd) {
  var large    = dmd["LARGE"];
  var max_size = dmd["MAX-SIZE"];
  var ordered  = dmd["ORDERED"];
  var psize    = dmd["PAGE-SIZE"];
  var trim     = dmd["TRIM"];
  ZH.l('check_metadata_fields: L: ' + large + ' MS: ' + max_size + 
       ' O: ' + ordered + ' P: ' + psize + ' T: ' + trim);
  if (large    && max_size)  return false;
  if (!large   && !max_size) return false;
  if (large    && trim)      return false;
  if (max_size && psize)     return false;
  if (large) {
    if (!ordered || !psize)       return false;
    if (psize    && isNaN(psize)) return false;
  }
  if (max_size) {
    if (max_size && isNaN(max_size)) return false;
    if (trim     && isNaN(trim))     return false;
  }
  return true;
}

function initialize_metadata_fields(dmd) {
  if (dmd["LARGE"] && !dmd['page_number']) dmd['page_number'] = 1;
}

function check_json_object_dt(jdata) {
  var nk = Object.keys(jdata).length;
  if (nk !== 3) return false; // [_data, _type, _metadata]
  else {
    var has_data = false;
    var dtype    = null;
    var dmd      = null;
    for (var jkey in jdata) {
      if      (jkey === "_data")     has_data = true;
      else if (jkey === "_type")     dtype    = jdata[jkey];
      else if (jkey === "_metadata") dmd      = jdata[jkey];
    }
    var vdt = ZH.ValidateDataType(dtype);
    ZH.l('check_json_object_dt: D: ' + has_data + ' T: ' + vdt); ZH.p(dmd);
    var ok  = (has_data && vdt && dmd);
    if (!ok) return false;
    if (!dmd.internal) { // NOTE NON-'internal' entries have addtl fields
      for (var k in dmd) {
        if (!SupportedMetadataFields[k]) return false;
      }
    }
    if (!check_metadata_fields(dmd)) return false;
    initialize_metadata_fields(dmd);
    return true;
  }
}

function convert_dt_to_crdt(jdata, meta, adding) {
  var dtype = jdata._type;
  var vdt   = ZH.ValidateDataType(dtype);
  if (ZH.IsUndefined(vdt)) return;
  var ddata = jdata._data;
  var dmd   = jdata._metadata;
  var cval  = convert_JsonElement_to_crdt(ddata, meta, adding);
  cval.F    = vdt;
  cval.E    = ZH.clone(dmd);
  return cval;
}

exports.JsonElementToCrdt = function(vtype, val, meta, adding) {
  if      (vtype === "A") return add_Array_member (val, meta, adding);
  else if (vtype === "N") return add_Number_member(val, meta, adding);
  else if (vtype === "S") return add_String_member(val, meta, adding);
  else if (vtype === "X") return add_Null_member  (     meta, adding);
  else if (vtype === "O") {
    var cfunc = check_json_object_dt(val) ? convert_dt_to_crdt :
                                            add_Object_member;
    return cfunc(val, meta, adding);
  } else {
    var etxt = 'VTYPE (' + vtype + ') unsupported type (' + typeof(val) + ')';
    throw(new Error(etxt));
  }
}

function convert_JsonElement_to_crdt(val, meta, adding) {
  var vtype = exports.GetCrdtTypeFromJsonElement(val);
  return exports.JsonElementToCrdt(vtype, val, meta, adding);
}

function convert_JsonArray_to_crdt(jarr, meta, adding) {
  var carr = [];
  for (var i = 0; i < jarr.length; i++) {
    carr.push(convert_JsonElement_to_crdt(jarr[i], meta, adding));
  }
  return carr;
}

function convert_JsonObject_to_crdt(jdata, meta, adding) {
  var cdata = {};
  for (var jkey in jdata) {
    cdata[jkey] = convert_JsonElement_to_crdt(jdata[jkey], meta, adding);
  }
  return cdata;
}

exports.GetCrdtTypeFromJsonElement = function(val) {
  var type = typeof(val);
  if        (type === 'object') {
    if (Array.isArray(val))     return "A";
    else                        return "O";
  } else if (type === 'number') return "N";
    else if (type === 'string') return "S";
    else if (val  === null)     return "X";
  else return; // NO-OP, returned undefined
}

exports.ConvertJsonToCrdtType = function(val) {
  var vtype = exports.GetCrdtTypeFromJsonElement(val);
  return      exports.JsonElementToCrdt(vtype, val, {}, false);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA-TYPE CONVERSION ------------------------------------------------------

function crdt_DT_LIST_to_json(cval, debug) {
  var sps = ZH.IsLateComer(cval);
  var arr = crdt_element_to_json(cval, sps, debug);
  if (!debug) return arr;
  else        return {_type: "LIST", _metadata : cval.E, _data : arr};
}

function crdt_array_to_json(cdata, sps, debug) {
  var jdata = [];
  for (var i = 0; i < cdata.length; i++) {
    var cval  = cdata[i];
    if (!sps && cval["Z"]) continue; // skip SLEEPER
    var jelem = crdt_element_to_json(cval, sps, debug);
    if (ZH.IsDefined(jelem)) jdata.push(jelem);
  }
  return jdata;
}

function crdt_object_to_json(cdata, sps, debug) {
  var jdata = {};
  for (var ckey in cdata) {
    var cval    = cdata[ckey];
    if (!sps && cval["Z"]) continue; // skip SLEEPER
    if (cval.F) jdata[ckey] = crdt_DT_LIST_to_json(cval, debug);
    else        jdata[ckey] = crdt_element_to_json(cval, sps, debug);
  }
  return jdata;
}

function crdt_element_to_json(cval, sps, debug) {
  if (cval["X"])         return; // Tombstone
  if (!sps && cval["Z"]) return; // SLEEPER
  var ctype = cval["T"];
  if      (ctype === "A") return crdt_array_to_json (cval.V, sps, debug);
  else if (ctype === "O") return crdt_object_to_json(cval.V, sps, debug);
  else if (ctype === "N") return Number(cval.V.P) - Number(cval.V.N);
  else if (ctype === "S") return cval.V;
  else if (ctype === "X") return null;
}

exports.CrdtElementToJson = function(cval) {
  return crdt_element_to_json(cval, true, false);
}

exports.DebugCrdtData = function(crdt) {
  return crdt_object_to_json(crdt._data.V, false, true);
}

exports.ConvertCrdtDataValue = function(crdtdv) {
  return crdt_object_to_json(crdtdv, true, false);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NUM BYTES CRDT ------------------------------------------------------------

var SIZE_NUMBER = 8;

function calculate_size_field(k, v) {
  var size = k.length;
  var type = typeof(v);
  if        (type === 'object') {
    if (Array.isArray(v)) size += calculate_size_array(v);
    else                  size += calculate_size_map(v);
  } else if (type === 'number') size += SIZE_NUMBER;
    else if (type === 'string') size += v.length;
  return size;
}

function calculate_size_array(m) {
  var size = 0;
  var k    = ""; // NOTE: EMPTY
  for (var i = 0; i < m.length; i++) {
    var v  = m[i];
    size  += calculate_size_field(k, v);
  }
  return size;
}

function calculate_size_map(m) {
  var size = 0;
  for (var k in m) {
    var v  = m[k];
    size  += calculate_size_field(k, v);
  }
  return size;
}

exports.CalculateMetaSize = function(cmeta) {
  return calculate_size_map(cmeta);
}

var CRDT_MEMBER_METADATA_SIZE        = (4*8);
var CRDT_ARRAY_MEMBER_METADATA_SIZE  = (6*8);
var CRDT_NUMBER_MEMBER_METADATA_SIZE = (2*8);

exports.CalculateCrdtMemberSize = function(cval) {
  var size  = 0;
  var cdata = cval.V;
  var ctype = cval.T;
  if        (ctype === "A") {
    for (var i = 0; i < cdata.length; i++) {
      size += CRDT_ARRAY_MEMBER_METADATA_SIZE;
      size += exports.CalculateCrdtMemberSize(cdata[i]);
    }
  } else if (ctype === "O") {
    for (var ckey in cdata) {
      size += ckey.length;
      size += CRDT_MEMBER_METADATA_SIZE;
      size += exports.CalculateCrdtMemberSize(cdata[ckey]);
    }
  } else if (ctype === "N") {
    size += CRDT_NUMBER_MEMBER_METADATA_SIZE;
  } else if (ctype === "S") {
    size += cdata.length;
  }
  // ELSE TYPE=X -> HAS NO SIZE
  return size;
}

exports.CalculateCrdtBytes = function(crdt) {
  var cmeta = crdt._meta;
  var crdtd = crdt._data;
  var size  = exports.CalculateMetaSize(cmeta);
  ZH.l('zconv_calculate_crdt_bytes: META-SIZE: ' + size);
  size      += exports.CalculateCrdtMemberSize(crdtd);
  ZH.l('zconv_calculate_crdt_bytes: #B: ' + size);
  return size;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZDelt.create_crdt_from_json() &
//               ZAD.create_crdt_from_initial_delta()
exports.ConvertJsonObjectToCrdt = function(jdata, meta, adding) {
  return convert_JsonObject_to_crdt(jdata, meta, adding);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZConv']={} : exports);

