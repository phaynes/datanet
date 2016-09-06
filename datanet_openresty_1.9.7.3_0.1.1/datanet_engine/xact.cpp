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
#include "merge.h"
#include "deltas.h"
#include "oplog.h"
#include "trace.h"

using namespace std;

extern Int64 MyUUID;

extern vector<string> ZH_DeltaFields;

// NOTE: All XACT.CPP is PURE ALGORITHM
//       this means it can happen on any worker in any Coroutine/thread
//       there may be state changes, but none that are KQK specific

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMIT CRDT ---------------------------------------------------------------

/* //TODO(V2) (for PULL & MEMCACHE_COMMIT)
static UInt64 count_json_elements(jv *jval) {
}
static UInt64 get_oplog_new_member_count(jv *joplog) {
}
*/

static UInt64 count_new_members(jv *jcdata) {
  UInt64  nmm   = 0;
  if ((*jcdata)["#"].asInt64() == -1) nmm += 1;
  jv     *jcval = &((*jcdata)["V"]);
  string  ctype = (*jcdata)["T"].asString();
  if        (!ctype.compare("A")) {
    for (uint i = 0; i < jcval->size(); i++) {
      jv *jval = &((*jcval)[i]);
      nmm      += count_new_members(jval);
    }
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv *jval  = &((*jcval)[mbrs[i]]);
      nmm      += count_new_members(jval);
    }
  }
  return nmm;
}

static UInt64 count_new_modified_members(jv *jdelta) {
  UInt64 nmm = 0;
  for (uint i = 0; i < (*jdelta)["modified"].size(); i++) {
    jv *mentry  = &((*jdelta)["modified"][i]);
    jv *mval    = &(*mentry)["V"];
    nmm        += count_new_members(mval);
  }
  return nmm;
}

UInt64 zxact_get_operation_count(jv *jocrdt, jv *joplog, bool remove,
                                 bool expire, jv *jdelta) {
  LT("zxact_get_operation_count");
  if (remove) return 1;
  if (expire) return 1;
  jv     *jmeta = &((*jocrdt)["_meta"]);
  UInt64  ncnt  = 0;
  if (zh_jv_is_member(jmeta, "member_count") &&
      zh_jv_is_member(jmeta, "last_member_count")) {
    UInt64 mcnt  = (*jocrdt)["_meta"]["member_count"].asUInt64();
    UInt64 lmcnt = (*jocrdt)["_meta"]["last_member_count"].asUInt64();
    ncnt         = mcnt - lmcnt;
  }
  //ncnt += get_oplog_new_member_count(joplog); //TODO(V2) PULL/MEMCACHE_COMMIT
  ncnt += count_new_modified_members(jdelta);
  return ncnt;
}

sqlres zxact_reserve_op_id_range(UInt64 cnt) {
  sqlres sr  = increment_next_op_id(cnt);
  RETURN_SQL_ERROR(sr)
  sr.ures   -= cnt; // New version is start of ID-range
  return sr;
}

static void assign_document_creation(jv *jmeta, UInt64 dvrsn, UInt64 ts) {
  // AGENT-Author Metadata -> SET ONLY ONCE
  if (!zh_jv_is_member(jmeta, "document_creation")) {
    (*jmeta)["document_creation"]["_"] = MyUUID;
    (*jmeta)["document_creation"]["#"] = dvrsn;
    (*jmeta)["document_creation"]["@"] = ts;
    string r = zh_convert_jv_to_string(&((*jmeta)["document_creation"]));
    LOG(INFO) << "ADDING META.DOCUMENT_CREATION: " << r << endl;
  }
}

static UInt64 assign_member_version(jv *jcdata, UInt64 dvrsn, UInt64 ts,
                                    bool isa) {
  if ((*jcdata)["#"].asInt64() == -1) {
    (*jcdata)["#"]  = dvrsn;
    dvrsn          += 1;
  }
  if ((*jcdata)["_"].asInt64() == -1) (*jcdata)["_"] = MyUUID;
  if ((*jcdata)["@"].asInt64() == -1) (*jcdata)["@"] = ts;
  if (isa) {
    if (!zh_jv_is_member(jcdata, "S")) {
      (*jcdata)["S"] = JARRAY;
      jv jts         = ts;
      zh_jv_append(&((*jcdata)["S"]), &jts);
    }
  }

  jv     *jcval = &((*jcdata)["V"]);
  string  ctype = (*jcdata)["T"].asString();
  if        (!ctype.compare("A")) {
    for (uint i = 0; i < jcval->size(); i++) {
      jv *jval = &((*jcval)[i]);
      dvrsn    = assign_member_version(jval, dvrsn, ts, true);
    }
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv *jval = &((*jcval)[mbrs[i]]);
      dvrsn    = assign_member_version(jval, dvrsn, ts, false);
    }
  }
  return dvrsn;
}

static void assign_array_left_neighbor(jv *jcdata) {
  jv     *jcval = &((*jcdata)["V"]);
  string  ctype = (*jcdata)["T"].asString();
  if        (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv *jval = &((*jcval)[mbrs[i]]);
      assign_array_left_neighbor(jval);
    }
  } else if (!ctype.compare("A")) {
    bool oa = zh_is_ordered_array(jcdata);
    for (uint i = 0; i < jcval->size(); i++) {
      jv  *jval = &((*jcval)[i]);
      if (!oa) {
        // NOTE: First element: {<:{ _:0,#:0}}
        int left = (i == 0) ? 0 : i - 1;
        if (!zh_jv_is_member(jval, "<")) { // needs a proper "<"
          UInt64 el_underscore = (i == 0) ? 0 : (*jcval)[left]["_"].asUInt64();
          UInt64 el_hashsign   = (i == 0) ? 0 : (*jcval)[left]["#"].asUInt64();
          (*jval)["<"]["_"] = el_underscore;
          (*jval)["<"]["#"] = el_hashsign;
        }
      }
      assign_array_left_neighbor(jval);
    }
  }
}

UInt64 zxact_update_crdt_meta_and_added(jv *jcrdt, UInt64 cnt, UInt64 dvrsn,
                                        UInt64 ts) {
  LT("zxact_update_crdt_meta_and_added");
  jv *jmeta = &((*jcrdt)["_meta"]);
  (*jmeta)["author"]["agent_uuid"] = MyUUID;
  (*jmeta)["op_count"]             = cnt;
  (*jmeta)["delta_version"]        = dvrsn;
  assign_document_creation(jmeta, dvrsn, ts);
  // Remove: MEMCACHE (separate) NO crdt._data
  if (zh_jv_is_member(jcrdt, "_data")) {
    jv *jcrdtd = &((*jcrdt)["_data"]);
    // Add [_, #] to new members
    dvrsn = assign_member_version(jcrdtd, dvrsn, ts, false);
    // Add [<] to new ARRAY members
    assign_array_left_neighbor(jcrdtd);
  }
  return dvrsn;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZDOC COMMIT ---------------------------------------------------------------

static void update_missing_rhn_entries(jv *jcval, uint i) {
  if (i == 0) return;
  else {
    jv    *jval   = &((*jcval)[i]);
    Int64  rduuid = (*jval)["<"]["_"].asInt64();
    if (rduuid == -1) { // MISSING [_,#]
      uint  r     = i - 1;
      jv   *jrval = &((*jcval)[r]);
      (*jval)["<"]["_"] = (*jrval)["_"];
      (*jval)["<"]["#"] = (*jrval)["#"];
    }
  }
}

static bool has_plus_entry(jv *jval) { // PERFORMANCE OPTIMIZATION
  string ctype = (*jval)["T"].asString(); // CHILD's TYPE
  bool   primt = !ctype.compare("S") || !ctype.compare("N"); // PRIMITIVE-TYPE
  if (primt) return zh_jv_is_member(jval, "+");
  else       return true;  // TYPE: [O,A] are nested -> may have nested +'s
}

static void populate_delta_added(jv *jcdata, jv *jadded, jv *jop_path) {
  jv *jcval = &((*jcdata)["V"]);
  if (zh_jv_is_member(jcdata, "+")) {
    jv jentry = zoplog_create_delta_entry(jop_path, jcdata);
    zh_jv_append(jadded, &jentry);
    return; // Adding highest member of tree -> adds all descendants
  }
  string ctype = (*jcdata)["T"].asString();
  if        (!ctype.compare("A")) {
    for (uint i = 0; i < jcval->size(); i++) {
      update_missing_rhn_entries(jcval, i);
      jv   *jval = &((*jcval)[i]);
      bool  chp  = has_plus_entry(jval);
      if (chp) {
        jv jkey = i;
        jv jentry = zoplog_create_op_path_entry(&jkey, jval);
        zh_jv_append(jop_path, &jentry);
        populate_delta_added(jval, jadded, jop_path);
        zh_jv_pop(jop_path);
      }
    }
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv   *jval = &((*jcval)[mbrs[i]]);
      bool  chp  = has_plus_entry(jval);
      if (chp) {
        jv jkey   = mbrs[i];
        jv jentry = zoplog_create_op_path_entry(&jkey, jval);
        zh_jv_append(jop_path, &jentry);
        populate_delta_added(jval, jadded, jop_path);
        zh_jv_pop(jop_path);
      }
    }
  }
}

static void remove_crdt_plus_entries(jv *jcdata) {
  jv     *jcval = &((*jcdata)["V"]);
  string  ctype = (*jcdata)["T"].asString();
  if (zh_jv_is_member(jcdata, "+")) jcdata->removeMember("+");
  if        (!ctype.compare("A")) {
    for (uint i = 0; i < jcval->size(); i++) {
      jv *jval = &((*jcval)[i]);
      remove_crdt_plus_entries(jval);
    }
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv *jval = &((*jcval)[mbrs[i]]);
      remove_crdt_plus_entries(jval);
    }
  }
}

static void truncate_lhn_entries(jv *jcdata) {
  jv     *jcval = &((*jcdata)["V"]);
  string  ctype = (*jcdata)["T"].asString();
  if (zh_jv_is_member(jcdata, "<")) { // truncate LHN
    jv *jlhn = &((*jcdata)["<"]);
    if (jlhn->size() > 2) { // MORE MEMBERS THAN [_,#]
      Int64 duuid = (*jcdata)["<"]["_"].asInt64();
      Int64 step  = (*jcdata)["<"]["#"].asInt64();
      jcdata->removeMember("<");
      (*jcdata)["<"]["_"] = duuid;
      (*jcdata)["<"]["#"] = step;
    }
  }
  if        (!ctype.compare("A")) {
    for (uint i = 0; i < jcval->size(); i++) {
      jv *jval = &((*jcval)[i]);
      truncate_lhn_entries(jval);
    }
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv *jval = &((*jcval)[mbrs[i]]);
      truncate_lhn_entries(jval);
    }
  }
}

static void cleanup_delta_fields(jv *jdelta) {
  LT("cleanup_delta_fields");
  for (uint i = 0; i < ZH_DeltaFields.size(); i++) {
    string f = ZH_DeltaFields[i];
    for (uint j = 0; j < (*jdelta)[f].size(); j++) {
      jv *jdlt = &((*jdelta)[f][j]);
      if (zh_jv_is_member(&((*jdlt)["V"]), "<")) { // truncate LHN
        jv    *jlhn =  &((*jdlt)["V"]["<"]);
        Int64  duuid = (*jlhn)["_"].asInt64();
        Int64  step  = (*jlhn)["#"].asInt64();
        (*jdlt)["V"]["<"].clear();
        (*jdlt)["V"]["<"]["_"] = duuid;
        (*jdlt)["V"]["<"]["#"] = step;
      }
    }
  }
  for (uint i = 0; i < (*jdelta)["added"].size(); i++) {
    jv *jadded = &((*jdelta)["added"][i]["V"]);
    remove_crdt_plus_entries(jadded);
  }
}

static jv *find_nested_element_via_set_uuid(jv *jcdata, string &u) {
  if (zh_jv_is_member(jcdata, "U")) {
    string cu  = (*jcdata)["U"].asString();
    bool   hit = !cu.compare(u);
    if (hit) return jcdata;
  }

  jv     *jcval = &((*jcdata)["V"]);
  string  ctype = (*jcdata)["T"].asString();
  if        (!ctype.compare("A")) {
    for (uint i = 0; i < jcval->size(); i++) {
      jv *jval = &((*jcval)[i]);
      jv *jres = find_nested_element_via_set_uuid(jval, u);
      if (jres) return jres;
    }
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv *jval = &((*jcval)[mbrs[i]]);
      jv *jres = find_nested_element_via_set_uuid(jval, u);
      if (jres) return jres;
    }
  }
  return NULL;
}

static UInt64 populate_nested_modified(jv *jcrdt, jv *jdelta,
                                       UInt64 dvrsn, UInt64 ts) {
  LT("populate_nested_modified");
  for (uint i = 0; i < (*jdelta)["modified"].size(); i++) {
    jv *jval = &((*jdelta)["modified"][i]["V"]);
    if (zh_jv_is_member(jval, "U")) { // "U" exists on SET, not on INCR
      string  u      = (*jval)["U"].asString();
      jv     *jcrdtd = &((*jcrdt)["_data"]);
      jv     *jres   = find_nested_element_via_set_uuid(jcrdtd, u);
      jres->removeMember("U"); // REMOVE FIELD "U" FROM CRDT
      *jval          = *jres;
    }
  }
  return dvrsn;
}

static void assign_delta_meta(jv *jcrdt, jv *jdelta, UInt64 ts) {
  (*jdelta)["_meta"]      = (*jcrdt)["_meta"]; // NOTE: CLONE META
  (*jdelta)["_meta"]["@"] = ts;                // SET Delta Timestamp
}

static void calculate_delta_size(jv *jdelta) {
  jv     *jdmeta = &((*jdelta)["_meta"]);
  UInt64  size   = zconv_calculate_meta_size(jdmeta);
  LD("calculate_delta_size: META-SIZE: " << size);
  for (uint i = 0; i < (*jdelta)["modified"].size(); i++) {
    jv *jval = &((*jdelta)["modified"][i]["V"]);
    zconv_calculate_crdt_member_size(jval, size);
  }
  for (uint i = 0; i < (*jdelta)["added"].size(); i++) {
    jv *jval = &((*jdelta)["added"][i]["V"]);
    zconv_calculate_crdt_member_size(jval, size);
  }
  LD("calculate_delta_size: #B: " << size);
  (*jdelta)["_meta"]["delta_bytes"] = size;
}

void zxact_finalize_delta_for_commit(jv *jcrdt, jv *jdelta,
                                     UInt64 dvrsn, UInt64 ts) {
  LT("zxact_finalize_delta_for_commit");
  // Remove: MEMCACHE (separate) NO crdt._data
  if (zh_jv_is_member(jcrdt, "_data")) {
    jv  jop_path = JARRAY;
    jv *jcrdtd   = &((*jcrdt)["_data"]);
    jv *jadded   = &((*jdelta)["added"]);
    ztrace_start("ZX.FDC.populate_delta_added");
    populate_delta_added(jcrdtd, jadded, &jop_path);
    ztrace_finish("ZX.FDC.populate_delta_added");
    remove_crdt_plus_entries(jcrdtd);
    truncate_lhn_entries(jcrdtd);
  }
  cleanup_delta_fields(jdelta);
  dvrsn = populate_nested_modified(jcrdt, jdelta, dvrsn, ts);
  assign_delta_meta(jcrdt, jdelta, ts);
  calculate_delta_size(jdelta);
}


