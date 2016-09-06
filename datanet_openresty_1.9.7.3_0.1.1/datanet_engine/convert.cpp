#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>

extern "C" {
  #include <time.h>
  #include <sys/time.h>
  #include <sys/types.h>
  #include <sys/un.h>
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

#include "json/json.h"
#include "easylogging++.h"

#include "helper.h"
#include "convert.h"

using namespace std;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

map<string, bool> SupportedMetadataFields = {{"MAX-SIZE",   true},
                                             {"TRIM",       true},
                                             {"ORDERED",    true},
                                             {"LARGE",      true},
                                             {"PAGE-SIZE",  true},
                                             {"LATE-COMER", true}};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FORWARD DECLARATIONS ------------------------------------------------------

static jv zconv_convert_json_array_to_crdt(jv *jjson, jv *jdmeta, bool adding);

static jv crdt_element_to_json(jv *cval, bool sps, bool debug);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// JSON-TO-CRDT --------------------------------------------------------------

jv zconv_add_OR_set_member(jv *jval, string ctype, jv *jdmeta, bool adding) {
  jv jmember   = JOBJECT;
  jmember["_"] = -1;     // _  -> DeviceUUID
  jmember["#"] = -1;     // _  -> Device's op-counter
  jmember["@"] = -1;     // @  -> Element's creation timestamp
  jmember["T"] = ctype;  // T  -> Type: [A,O,N,S,X]
  jmember["V"] = *jval;  // V  -> Value Object (contains P/N,S,etc..)
  if (adding) {          // Member added during this xaction
    jmember["+"] = true; 
  }
  if (zjd(jdmeta)) {
    (*jdmeta)["member_count"] = (*jdmeta)["member_count"].asUInt64() + 1;
  }
  return jmember;
}

static jv add_number_member(jv *jval, jv *jdmeta, bool adding) {
  jv    jdata = JOBJECT;
  Int64 val   = jval->asInt64();
  if (val >= 0) {
    jdata["P"] = val;
    jdata["N"] = 0;
  } else {
    jdata["P"] = 0;
    jdata["N"] = (val * -1);
  }
  return zconv_add_OR_set_member(&jdata, "N", jdmeta, adding);
}

static jv add_string_member(jv *jval, jv *jdmeta, bool adding) {
  jv jdata = jval->asString();
  return zconv_add_OR_set_member(&jdata, "S", jdmeta, adding);
}

static jv add_null_member(jv *jdmeta, bool adding) {
  jv jn = JNULL;
  return zconv_add_OR_set_member(&jn, "X", jdmeta, adding);
}

static jv add_object_member(jv *jval, jv *jdmeta, bool adding) {
  jv jdata = zconv_convert_json_object_to_crdt(jval, jdmeta, adding);
  return zconv_add_OR_set_member(&jdata, "O", jdmeta, adding);
}

static jv add_array_member(jv *jval, jv *jdmeta, bool adding) {
  jv jdata = zconv_convert_json_array_to_crdt(jval, jdmeta, adding);
  return zconv_add_OR_set_member(&jdata, "A", jdmeta, adding);
}

static bool check_metadata_fields(jv *jdmd) {
  bool   large        = zh_jv_is_member(jdmd, "LARGE");
  bool   ordered      = zh_jv_is_member(jdmd, "ORDERED");
  bool   has_max_size = zh_jv_is_member(jdmd, "MAX-SIZE");
  bool   has_psize    = zh_jv_is_member(jdmd, "PAGE-SIZE");
  bool   has_trim     = zh_jv_is_member(jdmd, "TRIM");
  Int64  max_size     = 0;
  UInt64 psize        = 0;
  Int64  trim         = 0;
  if (has_max_size) {
    jv jmsize = (*jdmd)["MAX-SIZE"];
    if (jmsize.isInt()) max_size = jmsize.asInt64();
  }
  if (has_psize) {
    jv jpsize = (*jdmd)["PAGE-SIZE"];
    if (jpsize.isUInt()) psize = jpsize.asUInt64();
  }
  if (has_trim) {
    jv jtrim = (*jdmd)["TRIM"];
    if (jtrim.isInt()) trim = jtrim.asInt64();
  }
  LD("check_metadata_fields: L: " << large << " MS: " << max_size <<
     " O: " << ordered << " P: " << psize << " T: " << trim);
  if (large    && max_size)  return false;
  if (!large   && !max_size) return false;
  if (large    && trim)      return false;
  if (max_size && psize)     return false;
  if (large) {
    if (!ordered || !psize)  return false;
  }
  return true;
}

static void initialize_metadata_fields(jv *jdmd) {
  bool large  = zh_jv_is_member(jdmd, "LARGE");
  bool has_pn = zh_jv_is_member(jdmd, "page_number");
  if (large && !has_pn) (*jdmd)["page_number"] = 1;
}

static bool check_json_object_dt(jv *jdata) {
  vector<string> mbrs = zh_jv_get_member_names(jdata);
  UInt64         nk   = mbrs.size();
  if (nk != 3) return false; // [_data, _type, _metadata]
  else {
    string dtype; // NOTE: EMPTY
    jv     jdmd     = JOBJECT;
    bool   has_data = false;
    for (uint i = 0; i < jdata->size(); i++) {
      string key = mbrs[i];
      if      (!key.compare("_data"))     has_data = true;
      else if (!key.compare("_type"))     dtype    = (*jdata)[key].asString();
      else if (!key.compare("_metadata")) jdmd     = (*jdata)[key];
    }
    bool vdt = zh_validate_datatype(dtype);
    LD("check_json_object_dt: D: " << has_data << " T: " << vdt);
    bool ok  = (has_data && vdt && zjd(&jdmd));
    if (!ok) return false;
    // NOTE NON-'internal' entries have addtl fields
    if (!zh_jv_is_member(&jdmd, "internal")) {
      vector<string> mbrs = zh_jv_get_member_names(&jdmd);
      for (uint i = 0; i < mbrs.size(); i++) {
        string k  = mbrs[i];
        map<string, bool>::iterator it = SupportedMetadataFields.find(k);
        if (it == SupportedMetadataFields.end()) return false;
      }
    }
    if (!check_metadata_fields(&jdmd)) return false;
    initialize_metadata_fields(&jdmd);
    return true;
  }
}

static jv convert_dt_to_crdt(jv *jdata, jv *jdmeta, bool adding) {
  string  dtype = (*jdata)["_type"].asString();
  bool    vdt   = zh_validate_datatype(dtype);
  if (!vdt) return JNULL;
  jv     *jval  = &((*jdata)["_data"]);
  jv     *jdmd  = &((*jdata)["_metadata"]);
  string  vtype = "A"; // NOTE: ALL DT are currently type:A
  jv      jcval = zconv_json_element_to_crdt(vtype, jval, jdmeta, adding);
  jcval["F"]    = zh_to_upper(dtype);
  jcval["E"]    = *jdmd;
  return jcval;
}

jv zconv_json_element_to_crdt(string vtype, jv *jval, jv *jdmeta, bool adding) {
  if        (!vtype.compare("A")) {
    return add_array_member(jval, jdmeta, adding);
  } else if (!vtype.compare("N")) {
    return add_number_member(jval, jdmeta, adding);
  } else if (!vtype.compare("S")) {
    return add_string_member(jval, jdmeta, adding);
  } else if (!vtype.compare("X")) {
    return add_null_member(jdmeta, adding);
  } else if (!vtype.compare("O")) {
    if (check_json_object_dt(jval)) {
      return convert_dt_to_crdt(jval, jdmeta, adding);
    } else {
      return add_object_member(jval, jdmeta, adding);
    }
  }
  zh_fatal_error("LOGIC(zconv_json_element_to_crdt)");
  return JNULL; // compiler warning
}

jv convert_json_element_to_crdt(jv *jval, jv *jdmeta, bool adding) {
  string vtype = zconv_get_crdt_type_from_json_element(jval);
  if (!vtype.size()) return JNULL; //TODO WHY does this check exist?
  else {
    return zconv_json_element_to_crdt(vtype, jval, jdmeta, adding);
  }
}

static jv zconv_convert_json_array_to_crdt(jv *jjson, jv *jdmeta, bool adding) {
  jv jcdata = JARRAY;
  for (uint i = 0; i < jjson->size(); i++) {
    jv *jval = &((*jjson)[i]);
    jv  jcel = convert_json_element_to_crdt(jval, jdmeta, adding);
    zh_jv_append(&jcdata, &jcel);
  }
  return jcdata;
}

jv zconv_convert_json_object_to_crdt(jv *jjson, jv *jdmeta, bool adding) {
  jv             jcdata = JOBJECT;
  vector<string> mbrs   = zh_jv_get_member_names(jjson);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  key  = mbrs[i];
    jv     *jval = &((*jjson)[key]);
    jcdata[key]  = convert_json_element_to_crdt(jval, jdmeta, adding);
  }
  return jcdata;
}
    
string zconv_get_crdt_type_from_json_element(jv *jval) {
  string miss; //NOTE: empty string
  if (zjn(jval))        return "X";
  if (jval->isObject()) return "O";
  if (jval->isString()) return "S";
  if (jval->isArray())  return "A";
  if (jval->isInt())    return "N";
  return miss;
}

jv zconv_convert_json_to_crdt_type_UInt64(UInt64 val) {
  jv jval(val);
  string vtype = zconv_get_crdt_type_from_json_element(&jval);
  return zconv_json_element_to_crdt(vtype, &jval, NULL, false);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CRDT-TO-JSON --------------------------------------------------------------

static jv crdt_DT_LIST_to_json(jv *cval, bool debug) {
  bool sps  = zh_is_late_comer(cval);
  jv   jarr = crdt_element_to_json(cval, sps, debug);
  if (!debug) return jarr;
  else {
    jv jdbg;
    jdbg["_type"]     = "LIST";
    jdbg["_metadata"] = (*cval)["E"];
    jdbg["_data"]     = jarr;
    return jdbg;
  }
}

static jv crdt_array_to_json(jv *cdata, bool sps, bool debug) {
  jv jdata = JARRAY;
  for (uint i = 0 ; i < cdata->size(); i++) {
    jv *cval  = &((*cdata)[i]);
    if (!sps && zh_jv_is_member(cval, "Z")) continue; // skip SLEEPER
    jv  jelem = crdt_element_to_json(cval, sps, debug);
    if (zjd(&jelem)) zh_jv_append(&jdata, &jelem);
  }
  return jdata;
}

static jv crdt_object_to_json(jv *cdata, bool sps, bool debug) {
  jv             jdata = JOBJECT;
  vector<string> mbrs  = zh_jv_get_member_names(cdata);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  ckey = mbrs[i];
    jv     *cval = &((*cdata)[ckey]);
    if (!sps && zh_jv_is_member(cval, "Z")) continue; // skip SLEEPER
    if (zh_jv_is_member(cval, "F")) {
      jdata[ckey] = crdt_DT_LIST_to_json(cval, debug);
    } else {
      jdata[ckey] = crdt_element_to_json(cval, sps, debug);
    }
  }
  return jdata;
}

static jv crdt_element_to_json(jv *cval, bool sps, bool debug) {
  if (zh_jv_is_member(cval, "X"))         return JNULL; // Tombstone
  if (!sps && zh_jv_is_member(cval, "Z")) return JNULL; // SLEEPER
  string ctype = (*cval)["T"].asString();
  if        (!ctype.compare("A")) {
    jv *cdata = &((*cval)["V"]);
    return crdt_array_to_json(cdata, sps, debug);
  } else if (!ctype.compare("O")) {
    jv *cdata = &((*cval)["V"]);
    return crdt_object_to_json(cdata, sps, debug);
  } else if (!ctype.compare("N")) {
    Int64 val = (*cval)["V"]["P"].asUInt64() - (*cval)["V"]["N"].asUInt64();
    return jv(val);
  } else if (!ctype.compare("S")) {
    return (*cval)["V"];
  } else if (!ctype.compare("X")) {
    return JNULLSTRING;
  }
  LE("crdt_element_to_json: UNKNOWN: T: :" << ctype); LE("cval: " << *cval);
  zh_fatal_error("LOGIC(crdt_element_to_json)");
  return JNULL; // compiler warning
}

jv zconv_crdt_element_to_json(jv *cval, bool debug) {
  return crdt_element_to_json(cval, true, debug);
}

jv zconv_convert_crdt_data_value(jv *jcrdtdv) {
  return crdt_object_to_json(jcrdtdv, true, false);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NUM BYTES CRDT ------------------------------------------------------------

#define SIZE_NUMBER 8

static UInt64 calculate_size_map(jv *jmap);
static UInt64 calculate_size_array(jv *jmap);

static UInt64 calculate_size_field(string &mname, jv *mfield) {
  UInt64 size = mname.size();
  if (mfield->isObject()) {          // OBJECT
    size += calculate_size_map(mfield);
  } else if (mfield->isArray()) {    // ARRAY
    size += calculate_size_array(mfield);
  } else if (mfield->isIntegral()) { // NUMBER
    size += SIZE_NUMBER;
  } else {                           // STRING
    string s  = mfield->asString();
    size     += s.size();
  }
  return size;
}

static UInt64 calculate_size_array(jv *jarr) {
  UInt64 size = 0;
  string mname; // NOTE EMPTY
  for (uint i = 0; i < jarr->size(); i++) {
    jv *mfield  = &((*jarr)[i]);
    size       += calculate_size_field(mname, mfield);
  }
  return size;
}

static UInt64 calculate_size_map(jv *jmap) {
  UInt64 size = 0;
  vector<string> mbrs = zh_jv_get_member_names(jmap);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  mname   = mbrs[i];
    jv     *mfield  = &((*jmap)[mname]);
    size           += calculate_size_field(mname, mfield);
  }
  return size;
}

UInt64 zconv_calculate_meta_size(jv *jmeta) {
  return calculate_size_map(jmeta);
}

#define CRDT_MEMBER_METADATA_SIZE        (4*8)
#define CRDT_ARRAY_MEMBER_METADATA_SIZE  (6*8)
#define CRDT_NUMBER_MEMBER_METADATA_SIZE (2*8)

// NOTE: FAST APPROXIMIATION
void zconv_calculate_crdt_member_size(jv *cval, UInt64 &size) {
  string ctype = (*cval)["T"].asString();
  if        (!ctype.compare("A")) {
    jv *cdata = &((*cval)["V"]);
    for (uint i = 0 ; i < cdata->size(); i++) {
      jv *el  = &((*cdata)[i]);
      size   += CRDT_ARRAY_MEMBER_METADATA_SIZE;
      zconv_calculate_crdt_member_size(el, size);
    }
  } else if (!ctype.compare("O")) {
    jv *cdata = &((*cval)["V"]);
    vector<string> mbrs  = zh_jv_get_member_names(cdata);
    for (uint i = 0; i < mbrs.size(); i++) {
      string  ckey  = mbrs[i];
      jv     *el    = &((*cdata)[ckey]);
      size         += ckey.size();
      size         += CRDT_MEMBER_METADATA_SIZE;
      zconv_calculate_crdt_member_size(el, size);
    }
  } else if (!ctype.compare("N")) {
    size += CRDT_NUMBER_MEMBER_METADATA_SIZE;
  } else if (!ctype.compare("S")) {
    string s  = (*cval)["V"].asString();
    size     += s.size();
  }
  // ELSE TYPE=X -> HAS NO SIZE
}

UInt64 zconv_calculate_crdt_bytes(jv *jcrdt) {
  jv     *jcmeta = &((*jcrdt)["_meta"]);
  jv     *jcrdtd = &((*jcrdt)["_data"]);
  UInt64  size   = zconv_calculate_meta_size(jcmeta);
  LD("zconv_calculate_crdt_bytes: META-SIZE: " << size);
  zconv_calculate_crdt_member_size(jcrdtd, size);
  LD("zconv_calculate_crdt_bytes: #B: " << size);
  return size;
}

