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
#include "dt.h"
#include "ooo_replay.h"
#include "gc.h"
#include "trace.h"
#include "storage.h"

using namespace std;

typedef ostringstream oss;

bool DeepDebugContent        = true; // TODO FIXME HACK
bool DeepDebugArrayHeartbeat = true; // TODO FIXME HACK


/*
  TODO Handle clock-skew issues (need GlobalTime protocol)
  E.g.: If a delta comes in with dates very far in the past,
        cmp_child_member() will push them to the front of a CRDT_ARRAY
  SOLUTION: Central should baseline "S"s and propogate any "S" changes
*/

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG OVERRIDES -----------------------------------------------------------

//#define DISABLE_LT
#ifdef DISABLE_LT
  #undef LT
  #define LT(text) /* text */
#endif


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FORWARD DECLARATIONS ------------------------------------------------------

static jv create_provisional_reorder_delta(jv *rometa, jv *reorder);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static void debug_element_value(string field, jv jel, string prfx) {
  LD(prfx << " NAME: " << field << 
     " [D:" << jel["D"].asInt64()  << ",P:" << jel["P"].asUInt64() <<
     ",N:"  << jel["N"].asUInt64() << "]");
}

static void add_merge_not_found_error(jv *merge, string dsection,
                                      string pmpath) {
  string merr  = "Section: (" + dsection + ") Path: (" + pmpath +
                 ") key not found - possible overwritten";
  LD("MERGE_CRDT: " << merr);
  jv     jmerr = merr;
  zh_jv_append(&((*merge)["errors"]), &jmerr);
}

static void add_merge_overwritten_error(jv *merge, string dsection,
                                        string pmpath) {
  string merr  = "Section: (" + dsection + ") Path: (" + pmpath +
                 ") has been overwritten";
  LD("MERGE_CRDT: " << merr);
  jv     jmerr = merr;
  zh_jv_append(&((*merge)["errors"]), &jmerr);
}

static bool full_member_match(jv *ma, jv *mb) {
  return ((*ma)["_"].asUInt64() == (*mb)["_"].asUInt64() &&
          (*ma)["#"].asUInt64() == (*mb)["#"].asUInt64() &&
          (*ma)["@"].asUInt64() == (*mb)["@"].asUInt64());
}

static void insert_overwrite_element_in_array(jv *clm, jv *el) {
  for (uint j = 0; j < clm->size(); j++) {
    if (full_member_match(el, &((*clm)[j]))) {
      (*clm)[j] = *el;
      return;
    }
  }
  zh_jv_append(clm, el);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MERGE ARRAY INSERTS -------------------------------------------------------

static bool tie_breaker_child_member(jv *da, jv *db) {
  if ((*da)["_"].asUInt64() != (*db)["_"].asUInt64()) {
    return ((*da)["_"].asUInt64() < (*db)["_"].asUInt64());
  } else {
    return ((*da)["#"].asUInt64() < (*db)["#"].asUInt64());
  }
  zh_fatal_error("LOGIC(tie_breaker_child_member)");
}

static jv unroll_s(jv *s) {
  jv u = JARRAY;
  for (uint i = 0; i < s->size(); i++) {
    jv *e = &((*s)[i]);
    if (e->isIntegral()) {
      zh_jv_append(&u, e);
    } else { //  NEXTED
      jv iu = unroll_s(e);
      for (uint j = 0; j < iu.size(); j++) {
        zh_jv_append(&u, &(iu[j]));
      }
    }
  }
  return u;
}

// DESC for S[] values, ASC for array-length
static int cmp_s(jv *usa, jv *usb) {
  for (uint i = 0; i < usa->size(); i++) {
    if (i == usb->size()) return 1; // ASC
    Int64 sa = (*usa)[i].asInt64();
    Int64 sb = (*usb)[i].asInt64();
    if (sa != sb) return (sa > sb) ? 1 : -1; // DESC
  }
  uint susa = usa->size();
  uint susb = usb->size();
  return (susa == susb) ? 0 : ((susa > susb) ? -1 : 1); // ASC
}

// Sort to CreationTimestamp[S](DESC), AUUID[_], DELTA_VERSION[#]
static bool cmp_child_member(jv *ja, jv *jb) {
  jv  *da  = &((*ja)["data"]);
  jv  *db  = &((*jb)["data"]);
  jv   usa = unroll_s(&((*da)["S"]));
  jv   usb = unroll_s(&((*db)["S"]));
  int  rs  = cmp_s(&usa, &usb);
  if (rs) return (rs > 0); // DESC
  else    return tie_breaker_child_member(da, db);
}

// LHN[0,0] is sorted BEFORE other roots
static bool cmp_root_child_member(jv *ja, jv *jb) {
  jv   *da    = &((*ja)["data"]);
  jv   *db    = &((*jb)["data"]);
  bool  roota = (((*da)["<"]["_"].asUInt64() == 0) &&
                 ((*da)["<"]["#"].asUInt64() == 0));
  bool  rootb = (((*db)["<"]["_"].asUInt64() == 0) &&
                 ((*db)["<"]["#"].asUInt64() == 0));
  if ((!roota && !rootb) || (roota && rootb)) { // NEITHER OR BOTH ROOT
    return cmp_child_member(ja, jb);
  } else {
    return roota;
  }
}

static jv get_root_ll_node(jv *carr) {
  vector<int> tor;
  jv  roots        = JOBJECT;
  roots["members"] = JARRAY; // create_ll_node()
  jv *rmbrs        = &(roots["members"]);
  for (uint i = 0; i < carr->size(); i++) {
    jv   *ma  = &((*carr)[i]);
    jv   *lhn = &((*ma)["<"]);
    bool  hit = false;
    if ((*lhn)["_"].asUInt64() == 0 && (*lhn)["#"].asUInt64() == 0) hit = false;
    else {
      for (uint j = 0; j < carr->size(); j++) {
        if (i == j) continue;
        jv *mb = &((*carr)[j]);
        if ((*lhn)["_"].asUInt64() == (*mb)["_"].asUInt64() &&
            (*lhn)["#"].asUInt64() == (*mb)["#"].asUInt64()) {
          hit = true;
          break;
        }
      }
    }
    if (!hit) {
      jv jpar;
      jpar["data"] = *ma; // create_parent()
      zh_jv_append(rmbrs, &jpar);
      tor.push_back((int)i);
    }
  }
  for (int i = (int)tor.size() - 1; i >= 0; i--) {
    zh_jv_splice(carr, tor[i]);
  }
  return roots;
}

static void assign_children(jv *par, jv *carr) {
  vector<int> tor;
  (*par)["child"]["members"] = JARRAY; // create_ll_node()
  jv *children = &((*par)["child"]["members"]);
  jv *pdata    = &((*par)["data"]);
  for (uint i = 0; i < carr->size(); i++) {
    jv *ma  = &((*carr)[i]);
    jv *lhn = &((*ma)["<"]);
    if ((*pdata)["_"].asUInt64() == (*lhn)["_"].asUInt64() &&
        (*pdata)["#"].asUInt64() == (*lhn)["#"].asUInt64()) {
      jv jpar;
      jpar["data"] = *ma; // create_parent()
      zh_jv_append(children, &jpar);
      tor.push_back(i);
    }
  }
  for (int i = (int)(tor.size() - 1); i >= 0; i--) {
    zh_jv_splice(carr, tor[i]);
  }
  if (children->size() == 0) par->removeMember("child"); // EMPTY
  else {
    zh_jv_sort(children, cmp_child_member);
    for (uint i = 0; i < children->size(); i++) {
      jv *npar = &((*children)[i]);
      assign_children(npar, carr);
    }
  }
}

static jv create_ll_from_crdt_array(jv *carr) {
  jv  roots = get_root_ll_node(carr); // CARR gets modified -> OK
  jv *rmbrs = &(roots["members"]);
  zh_jv_sort(rmbrs, cmp_root_child_member);
  for (uint i = 0; i < roots["members"].size(); i++) {
    jv *par = &(roots["members"][i]);
    assign_children(par, carr);
  }
  return roots;
}

static void flatten_llca(jv *sarr, jv *child) {
  for (uint i = 0; i < (*child)["members"].size(); i++) {
    jv *par  = &((*child)["members"][i]);
    jv *data = &((*par)["data"]);
    zh_jv_append(sarr, data);
    if (par->isMember("child")) {
      jv *c = &((*par)["child"]);
      flatten_llca(sarr, c);
    }
  }
}

void zmerge_do_array_merge(jv *carr) {
  LT("zmerge_do_array_merge");
  jv llca = create_ll_from_crdt_array(carr);
  carr->clear();
  flatten_llca(carr, &llca);
}

static UInt64 array_merge(jv *pclm, jv *carr, jv *aval) {
  bool oa = zh_is_ordered_array(pclm);
  LT("array_merge: OA: " << oa);
  if (oa) {
    zh_jv_append(carr, aval);
    return 0;
  } else {
    zh_jv_append(carr, aval);
    zmerge_do_array_merge(carr);
    UInt64 hit = 0;
    for (uint i = 0; i < carr->size(); i++) {
      jv *ma = &((*carr)[i]);
      if ((*aval)["_"].asUInt64() == (*ma)["_"].asUInt64() &&
          (*aval)["#"].asUInt64() == (*ma)["#"].asUInt64()) {
        return hit;
      }
      if (!zh_jv_is_member(ma, "X")) hit += 1; // Ignore Tombstones
    }
    zh_fatal_error("LOGIC(array_merge)");
    return 0; // compiler warning
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD DATATYPE OPERATION METADATA -------------------------------------------

static void add_dts(jv *jdts, jv *pclm, jv *entry, jv *op_path) {
  jv  *jev     = &((*entry)["V"]);
  jv   jevf    = zh_jv_is_member(jev, "F")  ? (*jev)["F"]  : JNULL;
  bool entry_f = zjd(&jevf);
  jv   jpf     = zh_jv_is_member(pclm, "F") ? (*pclm)["F"] : JNULL;
  bool pclm_f  = zjd(&jpf);
  if (!entry_f && !pclm_f) return;
  jv fname;
  jv e;
  if (entry_f) {
    fname = (*jev)["F"];
    e     = (*jev)["E"];
  } else if (pclm_f) {
    fname = (*pclm)["F"];
    e     = (*pclm)["E"];
  }
  jv     coppath            = *op_path;
  if (pclm_f) zh_jv_pop(&coppath);
  string ppath              = zh_path_get_dot_notation(&coppath);
  (*jdts)[ppath]["F"]       = fname;
  (*jdts)[ppath]["E"]       = e;
  (*jdts)[ppath]["op_path"] = coppath;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIND ELEMENTS -------------------------------------------------------------

static string get_element_real_path(jv *clm, jv *op_path) {
  string ctype("O"); // base CRDT data-type always an object
  string path;
  for (uint i = 0; i < op_path->size(); i++) {
    string hit;
    if (!ctype.compare("A")) { // need to search array for op_path[_,#,@] match
      for (uint j = 0; j < clm->size(); j++) {
        if (full_member_match(&((*op_path)[i]), &((*clm)[j]))) {
          hit   = to_string(j);
          ctype = (*clm)[j]["T"].asString();
          clm   = &((*clm)[j]["V"]);
          break;
        }
      }
    } else {
      hit       = (*op_path)[i]["K"].asString();
      jv *nextc = &((*clm)[hit]);
      ctype     = (*nextc)["T"].asString();
      clm       = &((*nextc)["V"]);
    }
    if (path.size()) path += ".";
    path += hit;
  }
  return path;
}

static jv *__find_nested_element(jv* clm, jv *op_path, bool match_full_path,
                                 bool need_path, bool need_parent) {
  string  ctype = "O"; // base CRDT data-type always an object
  jv     *par   = clm;
  jv     *lastc = clm;
  uint    oplen = match_full_path ? op_path->size() : (op_path->size() - 1);
  for (uint i = 0; i < oplen; i++) {
    lastc = clm;
    if (!ctype.compare("A")) { // need to search array for op_path[_,#,@] match
      bool match = false;
      for (uint j = 0; j < clm->size(); j++) {
        if (full_member_match(&((*op_path)[i]), &((*clm)[j]))) {
          par   = &((*clm)[j]);
          ctype = (*clm)[j]["T"].asString();
          clm   = &((*clm)[j]["V"]);
          match = true;
          break;
        }
      }
      if (!match) return NULL; // Branch containing member is gone -> NO-OP
    } else {
      string  okey  = (*op_path)[i]["K"].asString();
      if (!zh_jv_is_member(clm, okey)) return NULL; // Member gone -> NO-OP
      jv     *nextc = &((*clm)[okey]);
      // Check that nextc matches not just by name, but by [_,#,@]
      if (!full_member_match(&((*op_path)[i]), nextc)) return NULL;
      par   = nextc;
      ctype = (*nextc)["T"].asString();
      clm   = &((*nextc)["V"]);
    }
  }
  return need_parent ? par   :
         need_path   ? lastc :
                       clm;
}

static jv *find_nested_element_exact(jv *clm, jv *op_path) {
  return __find_nested_element(clm, op_path, true, true, false);
}

static jv *find_nested_element_base(jv *clm, jv *op_path) {
  return __find_nested_element(clm, op_path, false, false, false);
}

// NOTE: Used by xact.populate_nested_modified()
jv *zmerge_find_nested_element_base(jv *clm, jv *op_path) {
  return find_nested_element_base(clm, op_path);
}

static jv *find_parent_element(jv *clm, jv *op_path) {
  return __find_nested_element(clm, op_path, false, false, true);
}

static jv *find_nested_element_default(jv *clm, jv *op_path) {
  return __find_nested_element(clm, op_path, true, false, false);
}

// NOTE: Used by zdt_run_data_type_functions()
jv *zmerge_find_nested_element(jv *clm, jv *op_path) {
  return find_nested_element_default(clm, op_path);
}

static string __get_last_key(jv *clm, jv *val, jv *op_path, bool lww) {
  string miss; //NOTE: empty string
  if (op_path->size() == 1) { // Top-level is Object
    return (*op_path)[0]["K"].asString();
  }
  if (!clm->isArray()) { // Non-array key match is Object
    uint f = op_path->size() - 1;
    return (*op_path)[f]["K"].asString();
  }

  // modding Array: Find [_,#,@] match (not LHN match)
  for (uint i = 0; i < clm->size(); i++) {
    jv *lval = &((*clm)[i]);
    if (full_member_match(val, lval)) return to_string(i);
  }
  return miss;
}

static string get_last_key_overwrite(jv *clm, jv *val, jv *op_path) {
  return __get_last_key(clm, val, op_path, true);
}

static string get_last_key_exact(jv *clm, jv *val, jv *op_path) {
  return __get_last_key(clm, val, op_path, false);
}

// PURPOSE: get key respecting array tombstones
static uint get_last_key_array_adding(jv *clm, jv *val, jv *op_path) {
  // Scan clm(array) if (clm[i][_,#]==val[<][_,#]) -> LeftHandNeighbor match
  for (uint i = 0; i < clm->size(); i++) {
    if ((*val)["<"]["_"].asUInt64() == (*clm)[i]["_"].asUInt64() &&
        (*val)["<"]["#"].asUInt64() == (*clm)[i]["#"].asUInt64()) {
      // NOTE: "@"s do not have to match (array adding is about position)
      return i + 1; // add one to go the left of the match
    }
  }
  return 0; // NOTE: a HEAD insert ("<";[0,0]) never matches clm[] entries
}

uint zmerge_get_last_key_array_adding(jv *clm, jv *val, jv *op_path) {
  return get_last_key_array_adding(clm, val, op_path);
}

// PURPOSE: get key respecting array tombstones
static string get_last_key_by_name(jv *clm, jv *val, jv *op_path, bool isa) {
  if (op_path->size() == 1) {
    return (*op_path)[0]["K"].asString();
  } else if (!isa) {
    uint f = op_path->size() - 1;
    return (*op_path)[f]["K"].asString();
  } else {
    uint index = get_last_key_array_adding(clm, val, op_path);
    return to_string(index);
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEEP DEBUG MERGE CRDT -----------------------------------------------------

static void deep_debug_array_heartbeat_invalid(jv *jcval) {
  LE("DeepDebugArrayHeartbeat: ARRAY_HEARTBEAT: INVALID SYNTAX");
  LE("cval"); LE(*jcval);
}

static void deep_debug_array_heartbeat_error(jv *jcval, string &kqk, jv *nvals,
                                             string &name, UInt64 ts) {
  LE("ERROR: (EDEEP) DeepDebugArrayHeartbeat: K: " << kqk <<
       " VAL: " << ts << "-" << name);
  LE("nvals"); LE(*nvals);
  LE("cval");  LE(*jcval);
}

static void do_deep_debug_array_heartbeat(string &kqk, jv *jcrdtdv) {
  LT("do_deep_debug_array_heartbeat");
  jv  nvals = JOBJECT;
  jv *jcval = &((*jcrdtdv)["CONTENTS"]["V"]);
  for (uint i = 0; i < jcval->size(); i++) {
    jv             *jchild = &((*jcval)[i]);
    string          ctype  = (*jchild)["T"].asString();
    if (ctype.compare("S")) continue; // ZMerge.RewindCrdtGCVersion() adds "N"s
    string          val    = (*jchild)["V"].asString();
    vector<string>  res    = split(val, '-');
    if (res.size() != 2) {
      deep_debug_array_heartbeat_invalid(jcval);
      return;
    }
    UInt64          ts     = strtoul(res[0].c_str(), NULL, 10);
    jv              jts    = ts;
    string          name   = res[1];
    if (!zh_jv_is_member(&nvals, name)) nvals[name] = JARRAY;
    zh_jv_append(&(nvals[name]), &jts);
  }
  bool ok = true;
  vector<string> mbrs = zh_jv_get_member_names(&nvals);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  name  = mbrs[i];
    jv     *jvals = &(nvals[name]);
    UInt64  max   = 0;
    for (uint j = 0; j < jvals->size(); j++) {
      UInt64 ts = (*jvals)[j].asUInt64();
      if (ts > max) max = ts;
      else {
        ok = false;
        deep_debug_array_heartbeat_error(jcval, kqk, &nvals, name, ts);
      }
    }
  }
  if (ok) LD("OK: DeepDebugArrayHeartbeat: K: "   << kqk);
  else    LE("FAIL: DeepDebugArrayHeartbeat: K: " << kqk);
}

static string print_unrolled_s(jv *us) {
  if (us->size() == 0) return (*us)[0].asString();
  else {
    string ret;
    for (uint i = 0; i < us->size(); i++) {
      if (ret.size()) ret += ",";
      ret += (*us)[i].asString();
    }
    return "[" + ret + "]";
  }
}

static void do_full_debug_array_heartbeat(jv *jcval) {
  oss res;
  for (uint i = 0; i < jcval->size(); i++) {
    jv     *jchild = &((*jcval)[i]);
    string  ctype  = (*jchild)["T"].asString();
    string  v;
    if (!ctype.compare("S")) {// ZMerge.RewindCrdtGCVersion() adds "N"s
      v = (*jchild)["V"].asString();
    }
    string  x      = zh_jv_is_member(jchild, "X") ?
                       (zh_jv_is_member(jchild, "A") ? " A" : " X") : "";
    UInt64  d      = (*jchild)["#"].asUInt64();
    jv      us     = unroll_s(&((*jchild)["S"]));
    string  s      = print_unrolled_s(&us);
    UInt64  lhnd   = (*jchild)["<"]["#"].asUInt64();
    res << "\n\tV: " << v << "\t(D: " << d << " S: " << s <<
           " L: " << lhnd << ")" << x;
  }
  LD(res.str());
}

static void full_debug_array_heartbeat(jv *jcrdtdv) {
  jv  *jcval = &((*jcrdtdv)["CONTENTS"]["V"]);
  do_full_debug_array_heartbeat(jcval);
}

void zmerge_deep_debug_merge_crdt(jv *jmeta, jv *jcrdtdv, bool is_reo) {
  LD("DeepDebugContent");
  string kqk    = zh_create_kqk_from_meta(jmeta);
  bool   is_ahb = (!kqk.compare("production|statistics|ARRAY_HEARTBEAT") &&
                  (jcrdtdv && zh_jv_is_member(jcrdtdv, "CONTENTS") &&
                  zh_jv_is_member(&((*jcrdtdv)["CONTENTS"]), "V")));
  if (DeepDebugContent) {
    if (is_ahb) {
      full_debug_array_heartbeat(jcrdtdv);
    } else {
      jv json = zconv_convert_crdt_data_value(jcrdtdv);
      LD(json);
    }
  }
  if (DeepDebugArrayHeartbeat && is_ahb) {
    if (!is_reo) { // REORDER_DELTAS are TEMPORARILY NOT-YET-ORDERED
      do_deep_debug_array_heartbeat(kqk, jcrdtdv);
    }
  }
}

void zmerge_debug_crdt(jv *jcrdt) {
  jv *jcmeta  = &((*jcrdt)["_meta"]);
  jv *jcrdtd  = &((*jcrdt)["_data"]);
  jv *jcrdtdv = &((*jcrdtd)["V"]);
  zmerge_deep_debug_merge_crdt(jcmeta, jcrdtdv, false);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MERGE DELTA INTO CRDT -----------------------------------------------------

// NOTE: equivalent CREATION-TIME is rare
static bool get_tiebraker_add_operation(jv *oval, jv *aval) {
  bool   ok = false;
  UInt64 aa = (*aval)["@"].asUInt64();
  UInt64 oa = (*oval)["@"].asUInt64();
  if      (aa >  oa) ok = true;
  else if (aa == oa) { // EQUIVALENT TIMES (VERY rare)
    if ((*aval)["_"].asUInt64() < (*oval)["_"].asUInt64()) {
      ok = true;; // TIE-BREAKER
    }
  }
  return ok;
}

//TODO -> makes debugging harder
static void emaciate_tombstone(jv *clm, string &lkey) {
  // NOTE: ONLY: [_,#,S,@,<] are needed
  //delete(clm[lkey].V); delete(clm[lkey].T);
}

jv zmerge_apply_deltas(jv *jocrdt, jv *jdelta, bool is_reo) {
  LD("ZMerge.ApplyDeltas: IS_REO: " << is_reo);
  //LT("ZMerge.ApplyDeltas: CRDT: " << *jocrdt << " DELTA: " << *jdelta);
  jv    merge;
  merge["errors"]            = JARRAY;
  merge["post_merge_deltas"] = JARRAY;
  jv   *jcrdtd    = &((*jocrdt)["_data"]["V"]);
  jv   *jcmeta    = &((*jocrdt)["_meta"]);
  merge["crdtd"]  = (*jcrdtd);
  jv    jdts;
  jv   *mcrdtd    = &(merge["crdtd"]);

  // 1.) Modified[]
  LT("ZMerge.ApplyDeltas: modified[]");
  for (uint i = 0; i < (*jdelta)["modified"].size(); i++) {
    jv     *mentry  = &((*jdelta)["modified"][i]);
    jv     *mval    = &(*mentry)["V"];
    string  mtype   = (*mval)["T"].asString();
    jv     *op_path = &((*mentry)["op_path"]);
    string  pmpath  = zh_path_get_dot_notation(op_path);
    bool   is_iop   = (*mval)["V"].isObject() &&
                      zh_jv_is_member(&((*mval)["V"]), "D");
    jv     *clm     = find_nested_element_base(mcrdtd, op_path);
    if (!clm) {
      add_merge_not_found_error(&merge, "modified", pmpath);
      continue; // Key not found & LWW fail -> NO-OP
    }
    jv     *pclm  = find_parent_element(mcrdtd, op_path);
    string lkey   = is_iop ? get_last_key_exact    (clm, mval, op_path) :
                             get_last_key_overwrite(clm, mval, op_path);
    if (!lkey.size()) {
      add_merge_overwritten_error(&merge, "modified", pmpath);
      continue;
    }
    add_dts(&jdts, pclm, mentry, op_path);
    bool    isa      = clm->isArray();
    string  rpath    = get_element_real_path(mcrdtd, op_path);
    int     index    = isa ? (int)strtoul(lkey.c_str(), NULL, 10) : -1;
    jv      *jelem   = isa ? &((*clm)[index]) : &((*clm)[lkey]);
    bool     c_isnum = (*jelem)["V"].isObject() &&
                       zh_jv_is_member(&((*jelem)["V"]), "P");
    if (c_isnum && is_iop && !mtype.compare("N")) {
      // Number modifications are done via deltas (INCR or DECR)
      Int64  ndelt    = (*mval)["V"]["D"].asInt64();
      UInt64 positive = (*jelem)["V"]["P"].asUInt64(); // old-value.P
      UInt64 negative = (*jelem)["V"]["N"].asUInt64(); // old-value.N
      if (ndelt > 0) positive = positive + ndelt; // add delta to old-value.P
      else           negative = negative - ndelt; // add delta to old-value.N
      debug_element_value(lkey, (*jelem)["V"], "PRE INCR: ");
      (*jelem)["V"]["P"] = positive;             // set new-value.P
      (*jelem)["V"]["N"] = negative;             // set new-value.N
      debug_element_value(lkey, (*jelem)["V"], "POST INCR: ");
      Int64 result  = (positive - negative);
      jv pme;
      pme["op"]     = "increment";
      pme["path"]   = rpath;
      pme["value"]  = ndelt;
      pme["result"] = result;
      zh_jv_append(&(merge["post_merge_deltas"]), &pme);
    } else { // All non-numeric modifications OVERWRITE the previous value
      (*jelem)     = (*mval); // SET NEW VALUE
      jv val       = zconv_crdt_element_to_json(mval, false);
      jv pme;
      pme["op"]    = "set";
      pme["path"]  = rpath;
      pme["value"] = val;
      zh_jv_append(&(merge["post_merge_deltas"]), &pme);
    }
  }

  // 2.) Tombstoned[]
  LT("ZMerge.ApplyDeltas: tombstoned[]");
  for (uint i = 0; i < (*jdelta)["tombstoned"].size(); i++) {
    jv     *tentry  = &((*jdelta)["tombstoned"][i]);
    jv     *tval    = &(*tentry)["V"];
    jv     *op_path = &((*tentry)["op_path"]);
    string  pmpath  = zh_path_get_dot_notation(op_path);
    jv     *clm     = find_nested_element_base(mcrdtd, op_path);
    if (!clm) {
      add_merge_not_found_error(&merge, "tombstoned", pmpath);
      continue;
    }
    string  lkey    = get_last_key_exact(clm, tval, op_path);
    if (!lkey.size()) {
      add_merge_overwritten_error(&merge, "tombstoned", pmpath);
      continue;
    }
    int index       = (int)strtoul(lkey.c_str(), NULL, 10);
    (*clm)[index]["X"] = true;
    emaciate_tombstone(clm, lkey);
    string rpath       = get_element_real_path(mcrdtd, op_path);
    jv pme;
    pme["op"]   = "delete";
    pme["path"] = rpath;
    zh_jv_append(&(merge["post_merge_deltas"]), &pme);
  }

  // 3.) Added[]
  LT("ZMerge.ApplyDeltas: added[]");
  ztrace_start("zmerge_apply_deltas.added[]");
  for (uint i = 0; i < (*jdelta)["added"].size(); i++) {
    jv     *aentry  = &((*jdelta)["added"][i]);
    jv     *aval    = &((*aentry)["V"]);
    string  atype   = (*aval)["T"].asString();
    jv     *op_path = &((*aentry)["op_path"]);
    string  pmpath  = zh_path_get_dot_notation(op_path);
    jv     *clm     = find_nested_element_base(mcrdtd, op_path);
    if (!clm) {
      add_merge_not_found_error(&merge, "added", pmpath);
      continue; // Key not found & LWW fail -> NO-OP
    }
    jv     *pclm  = find_parent_element(mcrdtd, op_path);
    bool    isa   = clm->isArray();
    bool    oa    = zh_is_ordered_array(pclm);
    string  lkey  = oa ? "0" : get_last_key_by_name(clm, aval, op_path, isa);
    if (!isa) { // SET [N,S,O] -> NOT INSERT [A]
      if (zh_jv_is_member(clm, lkey)) { // Added value was concurrently ADDED
        jv *oval = &((*clm)[lkey]);
        bool ok = get_tiebraker_add_operation(oval, aval);
        if (!ok) {
          add_merge_overwritten_error(&merge, "added", pmpath);
          continue;
        }
      }
    }
    jv pme;
    if (oa) { // CHECK FOR LARGE-LIST SLEEPER
      UInt64 oamin = zh_get_ordered_array_min(pclm);
      if (oamin) { // not defined when empty
        jv coamin = zconv_convert_json_to_crdt_type_UInt64(oamin);
        if (!cmp_crdt_ordered_list(aval, &coamin)) { // SLEEPER
          pme["late"]  = true;
          (*aval)["Z"] = true;
        }
      }
    }
    add_dts(&jdts, pclm, aentry, op_path);
    if (isa) {
      ztrace_start("zmerge_apply_deltas.added[]->array_merge");
      UInt64 hit   = array_merge(pclm, clm, aval);
      ztrace_finish("zmerge_apply_deltas.added[]->array_merge");
      pme["op"]    = "insert";
      pme["index"] = hit;
    } else {
      (*clm)[lkey] = (*aval);
      pme["op"]    = "set";
    }
    pme["path"]  = get_element_real_path(mcrdtd, op_path);
    pme["value"] = zconv_crdt_element_to_json(aval, false);
    zh_jv_append(&(merge["post_merge_deltas"]), &pme);
  }
  ztrace_finish("zmerge_apply_deltas.added[]");

  // 4.) Deleted[]
  LT("ZMerge.ApplyDeltas: deleted[]");
  for (uint i = 0; i < (*jdelta)["deleted"].size(); i++) {
    jv     *rentry  = &((*jdelta)["deleted"][i]);
    jv     *rval    = &((*rentry)["V"]);
    jv     *op_path = &((*rentry)["op_path"]);
    string  pmpath  = zh_path_get_dot_notation(op_path);
    jv     *clm     = find_nested_element_base(mcrdtd, op_path);
    if (!clm) {
      add_merge_not_found_error(&merge, "deleted", pmpath);
      continue; // Key not found & LWW fail -> NO-OP
    }
    string  lkey    = get_last_key_exact(clm, rval, op_path);
    if (!lkey.size()) {
      add_merge_overwritten_error(&merge, "deleted", pmpath);
      continue;
    }
    string rpath = get_element_real_path(mcrdtd, op_path);
    bool   isa   = clm->isArray();
    if (isa) {
      int index = (int)strtoul(lkey.c_str(), NULL, 10);
      zh_jv_splice(clm, index);
    } else {
      clm->removeMember(lkey);
    }
    jv pme;
    pme["op"]   = "delete";
    pme["path"] = rpath;
    zh_jv_append(&(merge["post_merge_deltas"]), &pme);
  }

  // 5.) Datatyped[]
  for (uint i = 0; i < (*jdelta)["datatyped"].size(); i++) {
    jv     *mentry  = &((*jdelta)["datatyped"][i]);
    jv     *mval    = &((*mentry)["V"]);
    if (!mval->size()) continue; // NO-OP
    jv     *op_path = &((*mentry)["op_path"]);
    jv     *clm     = find_nested_element_exact(mcrdtd, op_path);
    uint    f       = op_path->size() - 1;
    string  lkey    = (*op_path)[f]["K"].asString();
    for (uint j = 0; j < mval->size(); j++) {
      jv     *m     = &((*mval)[i]);
      string  fname = (*m)["field"].asString();
      string  dop   = (*m)["dop"].asString();
      if (!dop.compare("increment")) {
        (*clm)[lkey]["E"][fname] = (*clm)[lkey]["E"][fname].asUInt64() + 1;
      } else if (!dop.compare("set")) {
        UInt64 val = (*m)["value"].asUInt64();
        (*clm)[lkey]["E"][fname] = val;
      }
    }
  }

  UInt64 ndts = jdts.size();
  if (ndts != 0) {
    jv *jdmeta = &((*jdelta)["_meta"]);
    zdt_run_data_type_functions(mcrdtd, jdmeta, merge, &jdts);
  }

  zmerge_deep_debug_merge_crdt(jcmeta, mcrdtd, is_reo);

  return merge;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMPARE ZDOC AUTHORS ------------------------------------------------------

bool zmerge_compare_zdoc_authors(jv *m1, jv *m2) {
  UInt64 m1u = (*m1)["document_creation"]["_"].asUInt64();
  UInt64 m1h = (*m1)["document_creation"]["#"].asUInt64();
  UInt64 m2u = (*m2)["document_creation"]["_"].asUInt64();
  UInt64 m2h = (*m2)["document_creation"]["#"].asUInt64();
  LD("zmerge_compare_zdoc_authors: [" << m1u << "," << m1h <<
     "] [" << m2u << "," << m2h << "]");
  return (m1u != m2u || m1h != m2h);
}


//TODO START move to zgc.js

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FRESHEN HELPERS -----------------------------------------------------------

static jv create_array_element_from_tombstone(string &tid, jv *jtmb) {
  jv             *s      = &((*jtmb)[0]);
  jv              us     = unroll_s(s);
  UInt64          ts     = us[0].asUInt64();
  vector<string>  res    = split(tid, '|');
  UInt64          duuid  = strtoul(res[0].c_str(), NULL, 10);
  UInt64          step   = strtoul(res[1].c_str(), NULL, 10);
  jv              el;
  el["T"]                = "N"; // NOTE: DUMMY VALUE
  el["V"]["P"]           = 0;   // NOTE: DUMMY VALUE
  el["V"]["N"]           = 0;   // NOTE: DUMMY VALUE
  el["_"]                = duuid;
  el["#"]                = step;
  el["@"]                = ts,
  el["S"]                = *s,
  el["X"]                = true;
  el["<"]["_"]           = (*jtmb)[1];
  el["<"]["#"]           = (*jtmb)[2];
  return el; // PSEUDO TOMBSTONE
}

void zmerge_check_apply_reorder_element(jv *cval, jv *reo) {
  string cid = zgc_get_tombstone_id(cval);
  if (zh_jv_is_member(reo, cid)) {
    jv *jdo_reo       = &((*reo)[cid]);
    (*cval)["S"]      = (*jdo_reo)[0];
    (*cval)["<"]["_"] = (*jdo_reo)[1];
    (*cval)["<"]["#"] = (*jdo_reo)[2];
  }
}

static void apply_reorder(jv *clm, jv *reo) {
  for (uint i = 0; i < clm->size(); i++) {
    jv *cval = &((*clm)[i]);
    zmerge_check_apply_reorder_element(cval, reo);
  }
}

static void apply_undo_lhn_reorder(jv *clm, jv *jugcsumms) {
  jv *reo = &((*jugcsumms)["undo_lhn_reorder"]);
  apply_reorder(clm, reo);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REWIND/FORWARD CRDT -------------------------------------------------------

static void rewind_add_tombstones(jv *clm, jv *jtmbs) {
  LT("rewind_add_tombstones");
  vector<string> mbrs = zh_jv_get_member_names(jtmbs);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  tid  = mbrs[i];
    jv     *jtmb = &((*jtmbs)[tid]);
    jv      el   = create_array_element_from_tombstone(tid, jtmb);
    insert_overwrite_element_in_array(clm, &el);
  }
}

static void rewind_crdt_gc_version(jv *jcrdtd, jv *jugcsumms) {
  jv     *jcval = &((*jcrdtd)["V"]);
  string  ctype = (*jcrdtd)["T"].asString();
  if (!ctype.compare("A")) {
    string pid = zgc_get_array_id(jcrdtd);
    if (zh_jv_is_member(&((*jugcsumms)["tombstone_summary"]), pid)) {
      jv *jtmbs = &((*jugcsumms)["tombstone_summary"][pid]);
      rewind_add_tombstones(jcval, jtmbs);
      apply_undo_lhn_reorder(jcval, jugcsumms);
      zmerge_do_array_merge(jcval);
    }
    for (uint i = 0; i < jcval->size(); i++) {
      jv *jchild = &((*jcval)[i]);
      rewind_crdt_gc_version(jchild, jugcsumms);
    }
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv *jchild = &((*jcval)[mbrs[i]]);
      rewind_crdt_gc_version(jchild, jugcsumms);
    }
  }
}

// NOTE: returns CRDT
jv zmerge_rewind_crdt_gc_version(string &kqk, jv *jocrdt, jv *jgcsumms) {
  vector<string> mbrs = zh_jv_get_member_names(jgcsumms);
  UInt64         ngcs = mbrs.size();
  if (ngcs == 0) return *jocrdt;
  else {
    UInt64  min_gcv   = zgc_get_min_gcv(jgcsumms);
    LD("ZMerge.RewindCrdtGCVersion: K: " << kqk << " (MIN)GCV: " << min_gcv);
    jv      jagcsumms = zh_convert_numbered_object_to_sorted_array(jgcsumms);
    jv      jugcsumms = zgc_union_gcv_summaries(&jagcsumms);
    jv      jncrdt    = *jocrdt;
    jv     *jcrdtd    = &(jncrdt["_data"]);
    rewind_crdt_gc_version(jcrdtd, &jugcsumms);
    UInt64  ngcv      = (min_gcv - 1); // REWIND -> BEFORE MIN_GCV
    jv     *jnmeta    = &(jncrdt["_meta"]);
    (*jnmeta)["GC_version"] = ngcv;
    LD("ZMerge.RewindCrdtGCVersion: (N)GCV: " << ngcv);
    return jncrdt;
  }
}

static sqlres apply_agent_reorder_deltas(jv *pc, jv *jards) {
  for (uint i = 0; i < jards->size(); i++) {
    jv     *jard     = &((*jards)[i]);
    jv     *rometa   = &((*jard)["rometa"]);
    jv     *reorder  = &((*jard)["reorder"]);
    jv      rodentry = create_provisional_reorder_delta(rometa, reorder);
    sqlres sr = zoor_forward_crdt_gc_version_apply_reference_delta(pc,
                                                                   &rodentry);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

sqlres zmerge_forward_crdt_gc_version(jv *pc, jv *jagcsumms) {
  jv     *jks = &((*pc)["ks"]);
  jv     *md  = &((*pc)["extra_data"]["md"]);
  string  kqk = (*jks)["kqk"].asString();
  LD("ZMerge.ForwardCrdtGCVersion: K: " << kqk);
  for (uint i = 0; i < jagcsumms->size(); i++) {
    jv     *jgcs   = &((*jagcsumms)[i]);
    UInt64  gcv    = (*jgcs)["gcv"].asUInt64();
    jv     *jtsumm = &((*jgcs)["tombstone_summary"]);
    jv     *jlhnr  = &((*jgcs)["lhn_reorder"]);
    jv      jards  = zh_jv_is_member(jgcs, "agent_reorder_deltas") ?
                       (*jgcs)["agent_reorder_deltas"] : JARRAY;
    (*md)["ocrdt"] = (*pc)["ncrdt"]; // FORWARD-GC-VERSION on THIS CRDT
    bool    ok     = zgc_do_garbage_collection(pc, gcv, jtsumm, jlhnr);
    if (!ok) {
      sqlres sr = zoor_set_gc_wait_incomplete(kqk);
      RETURN_EMPTY_SQLRES // BAD -> STOP -> GC-WAIT_INCOMPLETE continues
    } else {
      zh_set_new_crdt(pc, md, &((*md)["ocrdt"]), true);
      sqlres  sr     = apply_agent_reorder_deltas(pc, &jards);
      RETURN_SQL_ERROR(sr)
      sr             = zoor_check_replay_ooo_deltas(pc, false, true);
      RETURN_SQL_ERROR(sr)
    }
  }
  return zoor_end_gc_wait_incomplete(kqk);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FRESHEN AGENT DELTA -------------------------------------------------------

static void update_agent_delta_lhn(jv *jugcsumms, jv *aentry,
                                   jv *pclm, jv *clm) {
  jv     *aval = &((*aentry)["V"]);
  jv     *lhn  = &((*aval)["<"]);
  string  pid  = zgc_get_array_id(pclm);
  LD("BEG: update_lhn: aval: " << *aval);
  if (!zh_jv_is_member(&((*jugcsumms)["tombstone_summary"]), pid)) {
    LD("PARENT: " << pid << " NOT FOUND in TOMBSTONE SUMMARY");
    return;
  } else {
    jv     *jtmbs = &((*jugcsumms)["tombstone_summary"][pid]);
    string  tid   = zgc_get_tombstone_id(lhn);
    if (!zh_jv_is_member(jtmbs, tid)) {
      LD("ENTRY: LHN: " << tid << " NOT FOUND in TOMBSTONE SUMMARY");
      return;
    } else {
      // NOTE: assigning field OS only needed on CENTRAL
      vector<string> mbrs = zh_jv_get_member_names(jtmbs);
      for (uint i = 0; i < mbrs.size(); i++) {
        string  tid  = mbrs[i];
        jv     *jtmb = &((*jtmbs)[tid]);
        jv      el   = create_array_element_from_tombstone(tid, jtmb);
        insert_overwrite_element_in_array(clm, &el);
      }
      apply_undo_lhn_reorder(clm, jugcsumms);
      zmerge_do_array_merge(clm); // SORTS CLM to LHN
      zgc_agent_post_merge_update_lhn(aentry, clm);
    }
  }
}

static jv do_freshen_agent_delta(jv *jdentry,   jv *jfcrdt,
                                 jv *jugcsumms, UInt64 gcv) {
  jv    rdentry = *jdentry;               // NOTE: CLONE -> gets modified
  jv   *jodelta = &(rdentry["delta"]);
  jv   *jdmeta  = &((*jodelta)["_meta"]);
  bool  initial = zh_get_bool_member(jdmeta, "initial_delta");
  bool  remove  = zh_get_bool_member(jdmeta, "remove");
  LD("do_freshen_agent_delta: I: " << initial << " R: " << remove);
  if (initial || remove) return rdentry;
  jv    jfcrdtd = (*jfcrdt)["_data"]["V"]; // NOTE: CLONE -> gets modified
  jv   *fcrdtd  = &jfcrdtd;
  for (uint i = 0; i < (*jodelta)["added"].size(); i++) {
    jv   *aentry  = &((*jodelta)["added"][i]);
    jv   *aval    = &((*aentry)["V"]);
    // NOT_ARRAY OR LHN[0,0] -> NO-OP
    if (!zh_jv_is_member(aval, "<")) continue;
    jv    lhn     = (*aval)["<"]; // NOTE: NOT POINTER
    if (((lhn["_"].asUInt64() == 0) && (lhn["#"].asUInt64() == 0))) continue;
    jv   *op_path = &((*aentry)["op_path"]);
    jv   *clm     = find_nested_element_base(fcrdtd, op_path);
    if (!clm) continue;
    jv   *pclm    = find_parent_element(fcrdtd, op_path);
    uint  lk      = get_last_key_array_adding(clm, aval, op_path);
    if (!lk) {
      update_agent_delta_lhn(jugcsumms, aentry, pclm, clm);
    }
  }
  LD("zmerge_freshen_agent_delta: GCV: " << gcv);
  rdentry["delta"]["_meta"]["GC_version"] = gcv;
  return rdentry;
}

sqlres zmerge_freshen_agent_delta(string &kqk, jv *jdentry,
                                  jv *jocrdt, UInt64 gcv) {
  UInt64 dgcv = zh_get_dentry_gcversion(jdentry);
  LT("zmerge_freshen_agent_delta: DGCV: " << dgcv << " GCV: " << gcv);
  if (dgcv >= gcv) {
    RETURN_SQL_RESULT_JSON(*jdentry)
  } else {
    sqlres  sr       = zgc_get_unioned_gcv_summary(kqk, dgcv, gcv);
    RETURN_SQL_ERROR(sr)
    jv     *jugcsumm = &(sr.jres);
    if (zjn(jugcsumm)) {
      LE("FAIL: FreshenAgentDelta: K: " << kqk << " DGCV: " << dgcv);
      RETURN_EMPTY_SQLRES
    } else {
      jv jndentry = do_freshen_agent_delta(jdentry, jocrdt, jugcsumm, gcv);
      RETURN_SQL_RESULT_JSON(jndentry)
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD REORDER DELTA TO REFERENCE DELTA --------------------------------------

static jv create_provisional_reorder_delta(jv *rometa, jv *reorder) {
  jv rodentry;
  rodentry["delta"]            = JOBJECT;
  rodentry["delta"]["_meta"]   = *rometa;
  rodentry["delta"]["reorder"] = *reorder;
  return rodentry;
}

jv zmerge_add_reorder_to_reference(jv *rodentry, jv *rfdentry) {
  jv  ndentry = *rfdentry;
  jv *rometa  = &((*rodentry)["delta"]["_meta"]);
  bool rignore = zh_get_bool_member(rometa, "reference_ignore");
  if (rignore) { // Used in ZAD.do_apply_delta()
    LD("REFERENCE_IGNORE -> DO_IGNORE");
    ndentry["delta"]["_meta"]["DO_IGNORE"] = true;
  } else {       // Used in ZAD.do_apply_delta()
    ndentry["delta"]["reorder"]            = (*rodentry)["delta"]["reorder"];
    ndentry["delta"]["_meta"]["REORDERED"] = true;
    // GC_Version used in REORDER-REMOVE
    ndentry["delta"]["_meta"]["GC_version"] = (*rometa)["GC_version"];
  }
  return ndentry;
}


//TODO END move to zgc.js

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CREATE VIRGIN JSON FROM OPLOG ---------------------------------------------

/* NOTE: Used in AgentStatelessCommit() & AgentMemcacheCommit()
         ZMerge.CreateVirginJsonFromOplog() CASES:
           A.) SET       a.b.c.d 22   -> OBJECT-SET
           B.) SET       a.b.c.2 33   -> ARRAY-SET
           C.) INCR/DECR a.b.c.d 44   -> OBJECT-INCR
           D.) INSERT    a.b.c.d 1 55 -> ARRAY-INSERT
*/

jv zmerge_create_virgin_json_from_oplog(jv *jks, jv *jrchans, jv *joplog) {
  jv jdata;
  jdata["_id"]       = (*jks)["key"].asString();
  jdata["_channels"] = (*jrchans);
  for (uint i = 0; i < joplog->size(); i++) {
    jv             *op      = &((*joplog)[i]);
    string          opname  = (*op)["name"].asString();
    string          op_path = (*op)["path"].asString();
    vector<string>  keys    = zh_parse_op_path(op_path);
    if (!opname.compare("delete")) { // NO-OP
    } else {                         // OP: [set, insert, increment, decrement]
      string  lkey     = keys[(keys.size() - 1)];
      bool    lk_isnan = zh_isNaN(lkey);
      jv     *jd       = &jdata;
      if (keys.size() > 2) { // e.g. 'a.b.c.d' -> {a:{b:{}}
        for (uint j = 0; j < (keys.size() - 2); j++) {
          string k = keys[j];
          (*jd)[k] = JOBJECT;
          jd       = &((*jd)[k]);
        }
      }
      if (keys.size() > 1) {
        string slkey = keys[(keys.size() - 2)];
        if (!lk_isnan) { // e.g. 'a.b.c.2 -> {a:{b:c:[]}} (B)
          (*jd)[slkey] = JARRAY;
        } else {         // e.g. 'a.b.c.d -> {a:{b:c:{}}} (A)
          (*jd)[slkey] = JOBJECT;
        }
      }
      if (!opname.compare("set")) { // NO-OP (e.g. SET a.b.d.c 4) (A,B)
      } else if (!opname.compare("increment") || !opname.compare("decrement")) {
        // e.g. INCR a.b.c.d 5 -> {a:{b:c:{d:0}}} (C)
        (*jd)[lkey] = 0;
      } else { // OP: [insert]
        // e.g. INSERT a.b.c.d 1 6 -> {a:{b:c:{d:[]}}} (D)
        (*jd)[lkey] = JARRAY;
      }
    }
  }
  return jdata;
}


