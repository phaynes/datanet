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
#include "dt.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static jv get_clm_min_json(jv *clm) {
  for (uint i = 0; i < clm->size(); i++) {
    if (!zh_jv_is_member(&((*clm)[i]), "X") &&
        !zh_jv_is_member(&((*clm)[i]), "Z")) {
      return zconv_crdt_element_to_json(&((*clm)[i]), false);
    }
  }
  return JNULL;
}

static jv get_clm_max_json(jv *clm) {
  for (int i = (int)(clm->size() - 1); i >= 0; i--) {
    if (!zh_jv_is_member(&((*clm)[i]), "X") &&
        !zh_jv_is_member(&((*clm)[i]), "Z")) {
      return zconv_crdt_element_to_json(&((*clm)[i]), false);
    }
  }
  return JNULL;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA-TYPE SORTING----------------------------------------------------------

static bool cmp_key(jv ja, jv jb) {
  string akey = ja["key"].asString();
  string bkey = jb["key"].asString();
  return (akey.compare(bkey) < 0);
}

static vector<jv> create_sorted_array(jv *ace) {
  vector<jv> as;
  vector<string> mbrs = zh_jv_get_member_names(&((*ace)["V"]));
  for (uint i = 0; i < mbrs.size(); i++) {
    string  key = mbrs[i];
    jv     *val = &((*ace)["V"][key]);
    jv      entry;
    entry["key"] = key;
    entry["val"] = val;
  }
  std::sort(as.begin(), as.end(), cmp_key);
  return as;
}

static bool cmp_crdt_ordered_object(jv *ace, jv *bce) {
  if (zh_jv_is_member(ace, "X") && zh_jv_is_member(bce, "X")) return false;
  if (zh_jv_is_member(ace, "X")) return true;
  if (zh_jv_is_member(bce, "X")) return false;
  UInt64 asize = (*ace)["V"].size();
  UInt64 bsize = (*bce)["V"].size();
  if (asize != bsize) return (asize < bsize);
  vector<jv> as = create_sorted_array(ace);
  vector<jv> bs = create_sorted_array(bce);
  for (uint i = 0; i < asize; i++) {
    string  akey = as[i]["key"].asString();
    string  bkey = bs[i]["key"].asString();
    bool   ret   = (akey.compare(bkey) < 0);
    if (ret) return ret;
    jv     *vace = &(as[i]["val"]);
    jv     *vbce = &(bs[i]["val"]);
    ret          = cmp_crdt_ordered_list(vace, vbce);
    if (ret) return ret;
  }
  return false;
}

static bool cmp_crdt_ordered_array(jv *ace, jv *bce) {
  if (zh_jv_is_member(ace, "X") && zh_jv_is_member(bce, "X")) return false;
  if (zh_jv_is_member(ace, "X")) return true;
  if (zh_jv_is_member(bce, "X")) return false;
  UInt64 asize = (*ace)["V"].size();
  UInt64 bsize = (*bce)["V"].size();
  if (asize != bsize) return (asize < bsize);
  for (uint i = 0 ; i < asize; i++) {
    jv   *vace = &((*ace)["V"][i]);
    jv   *vbce = &((*bce)["V"][i]);
    bool  ret  = cmp_crdt_ordered_list(vace, vbce);
    if (ret) return ret;
  }
  return false;
}

bool cmp_crdt_ordered_list(jv *ace, jv *bce) {
  if (zh_jv_is_member(ace, "X") && zh_jv_is_member(bce, "X")) return false;
  if (zh_jv_is_member(ace, "X")) return true;
  if (zh_jv_is_member(bce, "X")) return false;
  string atype = (*ace)["T"].asString();
  string btype = (*bce)["T"].asString();
  if (!atype.compare("O")) {
    if      (!btype.compare("O")) return cmp_crdt_ordered_object(ace, bce);
    else if (!btype.compare("A")) return true;  // Object BEFORE Array
    else                          return false; // Objects & Arrays LAST
  }
  if (!atype.compare("A")) {
    if      (!btype.compare("A")) return cmp_crdt_ordered_array(ace, bce);
    else if (!btype.compare("O")) return false; // Object BEFORE Array
    else                          return false; // Objects & Arrays LAST
  }
  if (!btype.compare("O")) return false;        // Objects & Arrays LAST
  if (!btype.compare("A")) return false;        // Objects & Arrays LAST
  if (!atype.compare(btype)) { 
    jv aval = zconv_crdt_element_to_json(ace, false);
    jv bval = zconv_crdt_element_to_json(bce, false);
    if (!atype.compare("S")) {
      string a = aval.asString();
      string b = bval.asString();
      return (a.compare(b) < 0);
    } else {
      UInt64 a = aval.asUInt64();
      UInt64 b = bval.asUInt64();
      return (a < b);
    }
  } else {
    if (!atype.compare("S")) return true;  // Strings BEFORE Numbers
    else                     return false;
  }
}

static void update_ordered_list_min_max(jv *jcrdtd, jv *op_path, jv *clm) {
  uint    f        = op_path->size() - 1;
  string  lkey     = (*op_path)[f]["K"].asString();
  jv      cop_path = *op_path;
  zh_jv_pop(&cop_path);
  jv     *pclm     = zmerge_find_nested_element(jcrdtd, &cop_path);
  (*pclm)[lkey]["E"]["min"] = get_clm_min_json(clm);
  (*pclm)[lkey]["E"]["max"] = get_clm_max_json(clm);
}

void zdt_run_data_type_functions(jv *jcrdtd, jv *jdmeta, jv merge, jv *dts) {
  LT("zdt_run_data_type_functions");
  vector<string> mbrs = zh_jv_get_member_names(dts);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  vname = mbrs[i];
    jv     *dtd   = &((*dts)[vname]);
    if (zh_jv_is_member(dtd, "E")) {
      jv *e       = &((*dtd)["E"]);
      jv *op_path = &((*dtd)["op_path"]);
      jv *clm     = zmerge_find_nested_element(jcrdtd, op_path);
      if (clm) {
        // NOTE: check_list_max_size & check_large_list ONLY @CENTRAL
        bool ordered = zh_jv_is_member(e, "ORDERED");
        if (ordered) {
          zh_jv_sort(clm, cmp_crdt_ordered_list);
          update_ordered_list_min_max(jcrdtd, op_path, clm);
        }
      }
    }
  }
}


