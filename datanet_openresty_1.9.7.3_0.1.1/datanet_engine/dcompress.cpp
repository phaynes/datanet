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
#include "shared.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG SETTINGS ------------------------------------------------------------

#define DISABLE_LT
#ifdef DISABLE_LT
  #undef LT
  #define LT(text) /* text */
#endif


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZATION ------------------------------------------------------------

// NOTE: must match ../zdcompress.js
vector<string> MetaFields =
                 { "@", "_", "_id", "AUTO", "GC_version",
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
                   "subscriber_received", "subscriber_sent", "xaction"};

map<string, uint> MetaFieldsMap;

static void populate_meta_fields_map() {
  for (uint i = 0; i < MetaFields.size(); i++) {
    string mfield = MetaFields[i];
    MetaFieldsMap.insert(make_pair(mfield, i));
  }
}

vector<string> CrdtTypes = {"O", "A", "N", "S"};
map<string, uint> CrdtTypesMap;

static void populate_crdt_types_map() {
  for (uint i = 0; i < CrdtTypes.size(); i++) {
    string ctype = CrdtTypes[i];
    CrdtTypesMap.insert(make_pair(ctype, i));
  }
}


void zcmp_initialize() {
  LT("zcmp_initialize");
  populate_meta_fields_map();
  populate_crdt_types_map();
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARRAY F & E COMPRESSION ---------------------------------------------------

enum internal_data_types_t {NONE = 0, LIST = 1, LARGE_LIST = 2};

static internal_data_types_t get_internal_data_type(jv *data) {
  internal_data_types_t fval = NONE;
  if (!zh_jv_is_member(data, "F")) return fval;
  else {
    jv *e = &((*data)["E"]);
    if      (zh_jv_is_member(e, "MAX-SIZE")) fval = LIST;
    else if (zh_jv_is_member(e, "LARGE"))    fval = LARGE_LIST;
    return fval;
  }
}

static void encode_array_f_e(jv *jm, jv *data) {
  LT("encode_array_f_e");
  internal_data_types_t fval = get_internal_data_type(data);
  jv                    jval = fval;
  zh_jv_append(jm, &jval);
  if (!fval) return;
  jv *e = &((*data)["E"]);
  if (fval == LIST) {
    zh_jv_append(jm, &((*e)["MAX-SIZE"]));
    if (zh_jv_is_member(e, "TRIM")) zh_jv_append(jm, &((*e)["TRIM"]));
    else                     zh_jv_append(jm, 0);
  } else { // LARGE_LIST
    zh_jv_append(jm, &((*e)["PAGE-SIZE"]));
  }
}

static void decode_array_f_e(jv *jm, jv *udata) {
  LT("decode_array_f_e");
  jv                    jval  = zh_jv_shift(jm);
  UInt64                fval  = jval.asUInt64();
  internal_data_types_t ifval = (internal_data_types_t)fval;
  if (fval) { // DECODE F & E
    (*udata)["F"] = "LIST"; // NOTE: currently only type is LIST
    (*udata)["E"] = JOBJECT;
    if (ifval == LIST) {
      (*udata)["E"]["MAX-SIZE"] = zh_jv_shift(jm);
      jv     jtval              = zh_jv_shift(jm);
      if (jtval.isUInt()) {
        UInt64 trim = jtval.asUInt64();
        if (trim) (*udata)["E"]["TRIM"] = trim;
      }
    } else { // LARGE_LIST
      (*udata)["E"]["LARGE"]     = true;
      (*udata)["E"]["PAGE-SIZE"] = zh_jv_shift(jm);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// META COMPRESSION ----------------------------------------------------------

void zcmp_compress_meta(jv *jmeta) {
  LT("zcmp_compress_meta");
  jv jcmeta = JOBJECT;
  jv vmeta  = JOBJECT;
  for (map<string, uint>::iterator it  = MetaFieldsMap.begin();
                                   it != MetaFieldsMap.end(); it++) {
    string mfield = it->first;
    uint   cnum   = it->second;
    if (zh_jv_is_member(jmeta, mfield)) {
      string scnum = to_string(cnum);
      vmeta[scnum] = (*jmeta)[mfield];
    }
  }
  jcmeta["F"] = JARRAY;
  jcmeta["V"] = JARRAY;
  vector<string> mbrs = zh_jv_get_member_names(&vmeta);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  scnum = mbrs[i];
    UInt64  cnum  = strtoul(scnum.c_str(), NULL, 10);
    jv      jcnum = cnum;
    jv     *jval  = &(vmeta[scnum]);
    zh_jv_append(&(jcmeta["F"]), &jcnum);
    zh_jv_append(&(jcmeta["V"]), jval);
  }
  *jmeta = jcmeta;
}

void zcmp_decompress_meta(jv *jmeta) {
  LT("zcmp_decompress_meta");
  jv  jumeta = JOBJECT;
  jv *f      = &((*jmeta)["F"]);
  jv *v      = &((*jmeta)["V"]);
  while (f->size()) {
    jv     jcnum   = zh_jv_shift(f);
    jv     jval    = zh_jv_shift(v);
    UInt64 cnum    = jcnum.asUInt64();
    string mfield  = MetaFields[cnum];
    jumeta[mfield] = jval;
  }
  *jmeta = jumeta;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CRDT MEMBER COMPRESSION ---------------------------------------------------

static uint compress_crdt_type(jv &jctype) {
  string ctype = jctype.asString();
  map<string, uint>::iterator it = CrdtTypesMap.find(ctype);
  if (it == CrdtTypesMap.end()) zh_fatal_error("LOGIC(compress_crdt_type)");
  return it->second;
}

static string decompress_crdt_type(jv &jcnum) {
  UInt64 cnum = jcnum.asUInt64();
  return CrdtTypes[cnum];
}

static void compress_crdt_member(jv *cdata, jv *data) {
  LT("compress_crdt_member");
  string ctype = (*data)["T"].asString();
  jv     vdata = JOBJECT;
  vdata["M"]   = JARRAY;
  jv jctype    = compress_crdt_type((*data)["T"]);
  zh_jv_append(&(vdata["M"]), &jctype);
  zh_jv_append(&(vdata["M"]), &((*data)["_"]));
  zh_jv_append(&(vdata["M"]), &((*data)["#"]));
  zh_jv_append(&(vdata["M"]), &((*data)["@"]));
  encode_array_f_e(&(vdata["M"]), data);
  if (zh_jv_is_member(data, "S")) { // ARRAY-MEMBER
    UInt64 xval = 0;
    if (zh_jv_is_member(data, "X")) {
      xval = zh_jv_is_member(data, "A") ? 2 : 1;
    }
    jv     jxval = xval;
    zh_jv_append(&(vdata["M"]), &jxval);               // FIRST X
    zh_jv_append(&(vdata["M"]), &((*data)["<"]["_"])); // THEN LHN
    zh_jv_append(&(vdata["M"]), &((*data)["<"]["#"]));
    jv js = (*data)["S"];                              // LAST S[,,,]
    while (js.size()) {
      jv jel = zh_jv_shift(&js);
      zh_jv_append(&(vdata["M"]), &jel);
    }
  }
  if        (!ctype.compare("S")) {
    vdata["V"] = (*data)["V"];
  } else if (!ctype.compare("N")) {
    UInt64 p   = (*data)["V"]["P"].asUInt64();
    jv     jp  = p;
    UInt64 n   = (*data)["V"]["N"].asUInt64();
    jv     jn  = n;
    vdata["V"] = JARRAY;                                  // NOTE: NESTED ARRAY
    zh_jv_append(&(vdata["V"]), &jp);
    zh_jv_append(&(vdata["V"]), &jn);
  } else if (!ctype.compare("O")) {
    vdata["V"] = JOBJECT;                                       // NOTE: OBJECT
    vector<string> mbrs = zh_jv_get_member_names(&((*data)["V"]));
    for (uint i = 0; i < mbrs.size(); i++) {
      string  k     = mbrs[i];
      jv     *jval  = &((*data)["V"][k]);
      vdata["V"][k] = JARRAY;                                   // NOTE: ARRAY
      jv     *jcval = &(vdata["V"][k]);
      compress_crdt_member(jcval, jval);
    }
  } else if (!ctype.compare("A")) {
    vdata["V"] = JARRAY;                                        // NOTE: ARRAY
    for (uint i = 0; i < (*data)["V"].size(); i++) {
      jv     *jval  = &((*data)["V"][i]);
      vdata["V"][i] = JARRAY;                                   // NOTE: ARRAY
      jv     *jcval = &(vdata["V"][i]);
      compress_crdt_member(jcval, jval);
    }
  }
  //LE("compress_crdt_member: M: " << vdata["M"]);
  zh_jv_append(cdata, &(vdata["M"]));
  zh_jv_append(cdata, &(vdata["V"]));
}

static void decompress_crdt_member(jv *udata, jv *zcval) {
  LT("decompress_crdt_member: ZCVAL: " << *zcval);
  if (zjn(zcval)) return;
  jv m          = zh_jv_shift(zcval);
  jv v          = zh_jv_shift(zcval);
  //LE("decompress_crdt_member: M: " << m);
  jv jcnum      = zh_jv_shift(&m);
  (*udata)["T"] = decompress_crdt_type(jcnum);
  (*udata)["_"] = zh_jv_shift(&m);
  (*udata)["#"] = zh_jv_shift(&m);
  (*udata)["@"] = zh_jv_shift(&m);
  decode_array_f_e(&m, udata);
  if (m.size()) { // ARRAY-MEMBER
    jv     jxval       = zh_jv_shift(&m);
    UInt64 xval        = jxval.asUInt64();
    if (xval >= 1) (*udata)["X"] = true;
    if (xval == 2) (*udata)["A"] = true;
    (*udata)["<"]      = JOBJECT;
    (*udata)["<"]["_"] = zh_jv_shift(&m);
    (*udata)["<"]["#"] = zh_jv_shift(&m);
    (*udata)["S"]      = JARRAY;
    while(m.size()) {
      jv jm = zh_jv_shift(&m);
      zh_jv_append(&((*udata)["S"]), &jm);
    }
  }
  string ctype = (*udata)["T"].asString();
  if        (!ctype.compare("S")) {
    (*udata)["V"] = v;
  } else if (!ctype.compare("N")) {
    (*udata)["V"]      = {};
    (*udata)["V"]["P"] = zh_jv_shift(&v);
    (*udata)["V"]["N"] = zh_jv_shift(&v);
  } else if (!ctype.compare("O")) {
    (*udata)["V"] = JOBJECT;                                    // NOTE: OBJECT
    vector<string> mbrs = zh_jv_get_member_names(&v);
    for (uint i = 0; i < mbrs.size(); i++) {
      string  k     = mbrs[i];
      jv     *jval  = &(v[k]);
      (*udata)["V"][k] = JOBJECT;                               // NOTE: OBJECT
      jv     *jcval = &((*udata)["V"][k]);
      decompress_crdt_member(jcval, jval);
    }
  } else if (!ctype.compare("A")) {
    (*udata)["V"] = JARRAY;                                     // NOTE: ARRAY
    for (uint i = 0; i < v.size(); i++) {
      jv     *jval     = &(v[i]);
      (*udata)["V"][i] = JOBJECT;                               // NOTE: OBJECT
      jv     *jcval    = &((*udata)["V"][i]);
      decompress_crdt_member(jcval, jval);
    }
  }
  //LD("decompress_crdt_member: UDATA: " << *udata);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CRDT COMPRESSION ----------------------------------------------------------

void zcmp_compress_crdt(jv *jcrdt) {
  LT("zcmp_compress_crdt");
  jv *jmeta = &((*jcrdt)["_meta"]);
  zcmp_compress_meta(jmeta);
  jv *jcval = &((*jcrdt)["_data"]);
  jv  cdata = JARRAY;                                          // NOTE: ARRAY
  compress_crdt_member(&cdata, jcval);
  //LD("compress_crdt_member: CDATA: " << cdata);
  *jcval    = cdata;
}

void zcmp_decompress_crdt(jv *jcrdt) {
  LT("zcmp_decompress_crdt");
  if (zjn(jcrdt)) return;
  jv *jmeta = &((*jcrdt)["_meta"]);
  zcmp_decompress_meta(jmeta);
  jv *jcval = &((*jcrdt)["_data"]);
  jv  cdata = JOBJECT;                                         // NOTE: OBJECT
  decompress_crdt_member(&cdata, jcval);
  //LD("decompress_crdt_member: CDATA: " << cdata);
  *jcval    = cdata;
}

