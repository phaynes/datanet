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
#include "convert.h"
#include "doc.h"
#include "oplog.h"

using namespace std;

extern Int64 MyUUID;

extern vector<string> ZH_DeltaFields;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// TYPEDEF FUNCTIONS ---------------------------------------------------------

typedef string (*op_func)(gv *getter, string key, jv *arg1, jv *arg2, jv *arg3);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FORWARD DECLARATIONS-------------------------------------------------------

static bool zoplog_get(gv *getter, string okey);



// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG OVERRIDES -----------------------------------------------------------

#define DISABLE_LT
#ifdef DISABLE_LT
  #undef LT
  #define LT(text) /* text */
#endif


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static bool subarray_elements_match(jv *a, jv *b, string index) {
  if (a->size() != b->size()) return false;
  for (uint i = 0; i < a->size(); i++) {
    if ((*a)[i][index] != (*b)[i][index]) return false;
  }
  return true;
}

static void add_null_members(gv *getter, jv *parent) {
  jv *jcmeta = &((*getter->jcrdt)["_meta"]);
  for (uint i = 0; i < (*parent)["V"].size(); i++) {
    if (!(*parent)["V"].isValidIndex(i)) {
      jv *jval = &((*parent)["V"][i]);
      (*parent)["V"][i] = zconv_json_element_to_crdt("X", jval, jcmeta, true);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// OP-PATH & DELTA ENTRIES ---------------------------------------------------

jv zoplog_create_op_path_entry(jv *jkey, jv *jval) {
  jv jentry;
  jentry["K"] = *jkey;
  if (zjd(jval)) {
    jentry["_"] = (*jval)["_"];
    jentry["#"] = (*jval)["#"];
    jentry["@"] = (*jval)["@"];
    jentry["T"] = (*jval)["T"];
  }
  return jentry;
}

jv zoplog_create_delta_entry(jv *jop_path, jv *jval) {
  jv jentry;
  jentry["op_path"] = *jop_path;
  jentry["V"]       = *jval;
  return jentry;
}

static void upsert_delta_modified(gv *getter, jv *jentry) {
  jv *jdelta = getter->jdelta;
  for (uint i = 0; i < (*jdelta)["modified"].size(); i++) {
    jv *op_path = &((*jdelta)["modified"][i]["op_path"]);
    if (subarray_elements_match(op_path, &(getter->op_path), "K")) {
      (*jdelta)["modified"][i] = *jentry;
      return;
    }
  }
  zh_jv_append(&((*jdelta)["modified"]), jentry);
}

static void remove_redundant_modified_entry(gv *getter) {
  jv *jdelta    = getter->jdelta;
  jv *jmodified = &((*jdelta)["modified"]);
  for (uint i = 0; i < jmodified->size(); i++) {
    jv *op_path = &((*jmodified)[i]["op_path"]);
    if (subarray_elements_match(op_path, &(getter->op_path), "K")) {
      zh_jv_splice(jmodified, (int)i); // remove entry from modified[]
      return; // there will only be one
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARGUMENT CHECKS -----------------------------------------------------------

static string perform_incr_checks(gv *getter, string key,
                                  UInt64 uval, char *endptr) {
  string err; //NOTE: empty string
  bool found = zoplog_get(getter, key);
  if (!found)            return ZS_Errors["FieldNotFound"] + key;
  if (endptr && *endptr) return ZS_Errors["IncrOnNaN"];
  return err;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// READ OPERATIONS -----------------------------------------------------------

static void init_getter(gv *getter, jv *jcrdt, jv *jdelta) {
  getter->op_path = JARRAY;
  getter->got     = &((*jcrdt)["_data"]);
  getter->jdelta  = jdelta;
  getter->jcrdt   = jcrdt;
}

static string get_array_key_skipping_tombstones(jv *arr, string key) {
  UInt64  tcnt =  0;
  UInt64  rcnt = -1;
  bool    sps  = zh_is_late_comer(arr);
  jv     *parr = &((*arr)["V"]);
  UInt64  ikey = strtoul(key.c_str(), NULL, 10);
  for (uint i = 0; i < parr->size(); i++) {
    jv *jel = &((*parr)[i]);
    if      (!sps && zh_jv_is_member(jel, "Z")) tcnt += 1;
    else if (zh_jv_is_member(jel, "X"))         tcnt += 1;
    else                                        rcnt = rcnt + 1;
    if (rcnt == ikey) return to_string(i);
  }
  UInt64 index = ikey + tcnt;
  return to_string(index); // past end of array insertion
}

static bool zoplog_get(gv *getter, string okey) {
  string  rkey   = okey; // tombstones may make real-key differ from okey
  int     rindex = -1;
  jv     *got    = getter->got;
  string  type   = (*got)["T"].asString();
  jv     *gotv   = &((*got)["V"]);
  bool    iso    = !type.compare("O");
  bool    isa    = !type.compare("A");
  jv jval;
  if        (iso) {
    jval   = zh_jv_is_member(gotv, rkey) ? (*gotv)[rkey] : JNULL;
  } else if (isa) {// tombstones mean okey must be mapped to rkey
    rkey   = get_array_key_skipping_tombstones(got, okey);
    rindex = atoi(rkey.c_str());
    jval   = gotv->isValidIndex(rindex) ? (*gotv)[rindex] : JNULL;
  } else {
    return false;
  }
  getter->gkey = rkey; // gkey is the real-key
  if (zjn(&jval)) return false;
  getter->got = isa ? &((*gotv)[rindex]) : &((*gotv)[rkey]);
  // JSON uses okey (not rkey);
  jv jokey    = okey;
  jv joentry  = zoplog_create_op_path_entry(&jokey, getter->got);
  zh_jv_append(&(getter->op_path), &joentry);
  return true;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FULL BASE OPERATIONS ------------------------------------------------------

static string __full_incr_op(jv *parent, gv *getter, UInt64 val) {
  LT("__full_incr_op");
  string  err; //NOTE: empty string
  jv     *got  = getter->got;
  jv     *gotv = &((*got)["V"]);
  UInt64  pval = (*gotv)["P"].asUInt64();
  (*gotv)["P"] = pval + val;
  if (zh_jv_is_member(gotv, "D")) {
    Int64 ival   = val; // SET DELTA
    (*gotv)["D"] = ival;
  } else {
    Int64 dval  = (*gotv)["D"].asInt64();
    Int64 ival   = dval + val; // INCR DELTA
    (*gotv)["D"] = ival;
  }
  jv *jop_path = &(getter->op_path);
  jv  jentry   = zoplog_create_delta_entry(jop_path, got);
  upsert_delta_modified(getter, &jentry);
  return err;
}

static string __full_decr_op(jv *parent, gv *getter, UInt64 val) {
  LT("__full_decr_op");
  string  err; //NOTE: empty string
  jv     *got  = getter->got;
  jv     *gotv = &((*got)["V"]);
  UInt64  nval = (*gotv)["N"].asUInt64();
  (*gotv)["N"] = nval + val;
  if (zh_jv_is_member(gotv, "D")) {
    Int64 ival   = (val * -1);
    (*gotv)["D"] = ival; // SET DELTA
  } else {
    Int64 dval   = (*gotv)["D"].asInt64();
    Int64 ival   = dval - val; // DECR DELTA
    (*gotv)["D"] = ival;
  }
  jv *jop_path = &(getter->op_path);
  jv  jentry   = zoplog_create_delta_entry(jop_path, got);
  upsert_delta_modified(getter, &jentry);
  return err;
}

static string __full_d_op(jv *parent, gv *getter, string key) {
  LT("__full_d_op");
  string err; //NOTE: empty string
  string  ptype     = (*parent)["T"].asString();
  bool    found     = zoplog_get(getter, key);
  bool    miss      = !found;
  if (miss) return ZS_Errors["FieldNotFound"] + key;
  jv     *got       = getter->got;
  jv      step      = zh_jv_is_member(got, "#") ? (*got)["#"] : JNULL;
  bool    committed = zjn(&step) ? false : (step.asInt64() != -1);
  jv      plus      = zh_jv_is_member(got, "+") ? (*got)["+"] : JNULL;
  bool    p_isarr   = !ptype.compare("A");
  if (committed) { //  write delta : deleted[] or tombstoned[]
    jv *jop_path = &(getter->op_path);
    jv  jentry   = zoplog_create_delta_entry(jop_path, got);
    jv *jdelta   = getter->jdelta;
    if (p_isarr) zh_jv_append(&((*jdelta)["tombstoned"]), &jentry);
    else         zh_jv_append(&((*jdelta)["deleted"]),    &jentry);
    remove_redundant_modified_entry(getter);
  }
  bool  tombstone = committed && p_isarr;
  jv   *pv        = &((*parent)["V"]);
  if        (tombstone) { // committed array delete -> tombstone
    int index = atoi(getter->gkey.c_str());
    (*pv)[index]["X"] = true;   // committed && type:[A]
  } else if (p_isarr) {   // not committed array delete -> splice
    int index = atoi(getter->gkey.c_str());
    zh_jv_splice(pv, index);
  } else {                // Object delete
    pv->removeMember(getter->gkey); // type:[N,S,O]
  }
  return err;
}

static UInt64 SetUUIDCounter = 0;
static string get_unique_set_uuid() {
  SetUUIDCounter += 1;
  return to_string(MyUUID) + "|" + to_string(SetUUIDCounter);
}

static string __full_s_op(jv *parent, gv *getter, string key,
                          jv *jval, jv *jdt, jv *jextra, bool is_set) {
  LT("__full_s_op");
  string  err; //NOTE: empty string
  jv     *jcmeta = &((*getter->jcrdt)["_meta"]);
  string  ptype  = (*parent)["T"].asString();
  bool    iso    = !ptype.compare("O");
  bool    isa    = !ptype.compare("A");
  jv     *got    = getter->got;
  bool    found  = zoplog_get(getter, key);
  bool    miss   = !found;
  if (miss) { // new key in path
    jv jval; // NOTE: EMPTY
    jv jokey  = key;
    jv jentry = zoplog_create_op_path_entry(&jokey, &jval);
    zh_jv_append(&(getter->op_path), &jentry);
  } else {
    got = getter->got;
  }
  jv     plus   = zh_jv_is_member(got, "+") ? (*got)["+"] : JNULL;
  bool   adding = (miss || zjd(&plus));
  string vtype  = zconv_get_crdt_type_from_json_element(jval);
  err           = zdoc_s_arg_check(iso, isa, key, vtype, adding);
  if (err.size()) return err;
  jv jmember = convert_json_element_to_crdt(jval, jcmeta, adding);
  if (zjd(jdt)) {
    jmember["F"] = *jdt;
    if (zjd(jextra)) jmember["E"] = *jextra;
  }
  if (!adding) { // NOTE: [S, <] unchanged: operation is overwrite
    jmember["_"] = -1;
    jmember["#"] = -1;
    jmember["@"] = -1;
    jmember["U"] = get_unique_set_uuid();
    // only type:[A] has "S"
    if (zh_jv_is_member(got, "S")) jmember["S"] = (*got)["S"];
    // only type:[A] has "<"
    if (zh_jv_is_member(got, "<")) jmember["<"] = (*got)["<"];
    // delta.modified[] population
    jv *jop_path = &(getter->op_path);
    jv  jentry   = zoplog_create_delta_entry(jop_path, &jmember);
    upsert_delta_modified(getter, &jentry);
  }
  if (iso) { // PARENT(OBJECT): replace current CRDT element w/ member
    (*parent)["V"][getter->gkey] = jmember; // s() -> CRDT element overwrite
  } else {   // PARENT(ARRAY):  replace current CRDT element w/ member
    jv     *pv     = &((*parent)["V"]);
    int     index  = atoi(getter->gkey.c_str());
    UInt64  p_plen = pv->size();            // length before s()
    (*pv)[index]   = jmember;               // s() -> CRDT element overwrite
    jv     *njval  = &((*pv)[index]);
    if (adding) { // Array[i] inserts past end of array, produce null-entries
      if (index > (int)p_plen) add_null_members(getter, parent);
      if (index == 0) (*njval)["<"] = zh_create_head_lhn();
      else {
        jv *ljval = &((*pv)[index - 1]);
        if ((*ljval)["_"].asInt64() != -1) { // COMMITTED-member LHN
          (*njval)["<"] = *ljval; // SET LHN
        }
      }
    }
  }
  return err;
}

static string __full_insert_op(jv *parent, gv *getter, string key,
                               string sindex, jv *jval) {
  LT("__full_insert_op");
  string  err; //NOTE: empty string
  jv     *jcmeta  = &((*getter->jcrdt)["_meta"]);
  string  vtype   = zconv_get_crdt_type_from_json_element(jval);
  jv      jmember = convert_json_element_to_crdt(jval, jcmeta, true);
  jmember["+"]    = true;     // Mark new member as to-be-delta.added[]
  jv     *arr     = &((*parent)["V"][getter->gkey]);
  jv     *par     = &((*arr)["V"]);
  string  rkey    = get_array_key_skipping_tombstones(arr, sindex);
  UInt64  rindex  = strtoul(rkey.c_str(), NULL, 10);
  int     ri      = rindex;
  bool    is_ll   = zh_is_large_list(arr);
  if (is_ll && (rindex != par->size())) {
    return ZS_Errors["LargeListOnlyRPUSH"];
  }
  if        (rindex > par->size()) { //- ---------------------- SET equivalent
    (*par)[ri] = jmember;
    add_null_members(getter, parent);
  } else if (rindex == par->size()) { // ---------------------- RPUSH equivalent
    zh_jv_append(par, &jmember);
  } else { // ------------------------------------------------- MIDDLE-INSERT
    jv lhn;
    if (ri == 0) lhn = zh_create_head_lhn();
    else {
      jv *ljval = &((*par)[ri - 1]);
      if ((*ljval)["_"].asInt64() != -1) { // COMMITTED-member LHN
        lhn = *ljval;
      }
    }
    if (zjd(&lhn)) jmember["<"] = lhn;
    zh_jv_insert(par, ri, &jmember); // INSERT new member into array
    // NOTE: Next line maintains a contiguous local CRDT
    //       But the OPERATION (in this line) is ONLY replicated if RHN is "+"
    //       LHN's are defined ONCE by Delta Author
    jv *rhn     = &((*par)[ri + 1]);
    (*rhn)["<"] = jmember;
  }
  return err;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRITE OPERATIONS ----------------------------------------------------------

static string full_s_op(gv *getter, string key, jv *jval, jv *jdt, jv *jextra) {
  jv *parent = getter->got;
  return __full_s_op(parent, getter, key, jval, jdt, jextra, true);
}

static string full_d_op(gv *getter, string key, jv *arg1, jv *arg2, jv *arg3) {
  jv *parent = getter->got;
  return __full_d_op(parent, getter, key);
}

static string full_insert_op(gv *getter, string key,
                             jv *jindex, jv *jval, jv *arg3) {
  jv     *parent = getter->got;
  string  sindex = jindex->asString();
  char   *endptr = NULL;
  UInt64  index  = strtoul(sindex.c_str(), &endptr, 10);
  string  err    = zdoc_insert_arg_pre_check(index, endptr);
  if (err.size()) return err;
  bool    found  = zoplog_get(getter, key);
  if (!found) {
    jv jarr = JARRAY;
    int i   = index;
    jarr[i] = *jval;
    return __full_s_op(parent, getter, key, &jarr, NULL, NULL, false);
  } else {
    jv     *got   = getter->got;
    string  rtype = (*got)["T"].asString();
    err           = zdoc_insert_post_arg_check(rtype);
    if (err.size()) return err;
    return __full_insert_op(parent, getter, key, sindex, jval);
  }
}

static string full_incr_op(gv *getter, string key,
                           jv *jval, jv *arg2, jv *arg3) {
  jv     *parent = (getter->got);
  string  val    = jval->asString();
  char   *endptr = NULL;
  UInt64  uval   = strtoul(val.c_str(), &endptr, 10);
  string  err    = perform_incr_checks(getter, key, uval, endptr);
  if (err.size()) return err;
  return __full_incr_op(parent, getter, uval);
}

static string full_decr_op(gv *getter, string key,
                           jv *jval, jv *arg2, jv *arg3) {
  jv     *parent = (getter->got);
  string  val    = jval->asString();
  char   *endptr = NULL;
  UInt64  uval   = strtoul(val.c_str(), &endptr, 10);
  string  err    = perform_incr_checks(getter, key, uval, endptr);
  if (err.size()) return err;
  return __full_decr_op(parent, getter, uval);
}

static string zoplog_write_op(jv *jdelta, jv *jcrdt, op_func end_func,
                              string path, jv *arg1, jv *arg2, jv *arg3) {
  gv getter;
  init_getter(&getter, jcrdt, jdelta);
  vector<string> keys = zdoc_parse_op_key(path);
  for (uint i = 0; i < keys.size() - 1; i++) {
    bool found = zoplog_get(&getter, keys[i]);
    if (!found) {
      string err = ZS_Errors["NestedFieldMissing"] + keys[i];
      return err;
    }
  }
  string lkey = keys[keys.size() - 1];
  return end_func(&getter, lkey, arg1, arg2, arg3);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REPLAY WRAPPERS -----------------------------------------------------------

static string replay_operation(jv *jdelta, jv *jcrdt, jv *op) {
  string mt; // NOTE: empty string
  string path  = (*op)["path"].asString();
  string err   = zdoc_write_key_check(path);
  if (err.size()) return err;
  jv     *args = &((*op)["args"]);
  jv *arg1  = args->size() < 1 ? NULL : &((*op)["args"][0]);
  jv *arg2  = args->size() < 2 ? NULL : &((*op)["args"][1]);
  jv *arg3  = args->size() < 3 ? NULL : &((*op)["args"][2]);
  string opname = (*op)["name"].asString();
  op_func of;
  if        (!opname.compare("set")) {
    of = &full_s_op;
  } else if (!opname.compare("delete")) {
    of = &full_d_op;
  } else if (!opname.compare("insert")) {
    of = &full_insert_op;
  } else if (!opname.compare("increment")) {
    of = &full_incr_op;
  } else if (!opname.compare("decrement")) {
    of = &full_decr_op;
  }
  return zoplog_write_op(jdelta, jcrdt, of, path, arg1, arg2, arg3);
}

jv zoplog_create_delta(jv *jcrdt, jv *joplog) {
  LT("zoplog_create_delta");
  jv jdelta = JOBJECT;
  for (uint i = 0; i < ZH_DeltaFields.size(); i++) {
    string f  = ZH_DeltaFields[i];
    jdelta[f] = JARRAY;
  }
  for (uint i = 0; i < joplog->size(); i++) {
    string err = replay_operation(&jdelta, jcrdt, &((*joplog)[i]));
    if (err.size()) {
      jdelta["err"] = err;
      return jdelta;
    }
  }
  LT("zoplog_create_delta: DELTA: " << jdelta);
  return jdelta;
}

