#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>

extern "C" {
  #include <limits.h>
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
#include "merge.h"
#include "deltas.h"
#include "ooo_replay.h"
#include "gc.h"
#include "storage.h"

using namespace std;

extern Int64  MyUUID;
extern UInt64 MaxDataCenterUUID;

extern jv zh_nobody_auth;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FORWARD DECLARATIONS ------------------------------------------------------

static sqlres save_gcv_summary(string &kqk, UInt64 gcv, jv *jgc);

static sqlres forward_reorders (string &kqk, jv *jcrdt, jv *reo);
static sqlres backward_reorders(string &kqk, jv *jcrdt, jv *reo);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GCV SUMMARIES -------------------------------------------------------------

sqlres zgc_remove_gcv_summary(string &kqk, UInt64 gcv) {
  LD("zgc_remove_gcv_summary: K: " << kqk << " GCV: " << gcv);
  sqlres sr = remove_gcv_summary(kqk, gcv);
  RETURN_SQL_ERROR(sr)
  return remove_gc_summary_version(kqk, gcv);
}

sqlres zgc_remove_gcv_summaries(string &kqk) { LT("zgc_remove_gcv_summaries");
  sqlres sr = fetch_gc_summary_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv *jgcvs = &(sr.jres);
  for (int i = 0; i < (int)jgcvs->size(); i++) {
    UInt64 gcv = (*jgcvs)[i].asUInt64();
    sqlres sr  = zgc_remove_gcv_summary(kqk, gcv);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres save_gcv_summary(string &kqk, UInt64 gcv, jv *jgcsumm) {
  LD("save_gcv_summary: K: " << kqk << " GCV: " << gcv);
  (*jgcsumm)["gcv"] = gcv; // Used in ZMerge.forward_crdt_gc_version()
  sqlres sr         = persist_gcv_summary(kqk, gcv, jgcsumm);
  RETURN_SQL_ERROR(sr)
  return persist_gc_summary_version(kqk, gcv);
}

sqlres zgc_save_gcv_summaries(string &kqk, jv *jgcsumms) {
  LT("zgc_save_gcv_summaries");
  vector<string> mbrs = zh_jv_get_member_names(jgcsumms);
  for (uint i = 0; i < mbrs.size(); i++) {
    UInt64  gcv     = strtoul(mbrs[i].c_str(), NULL, 10);
    jv     *jgcsumm = &((*jgcsumms)[mbrs[i]]);
    sqlres  sr      = save_gcv_summary(kqk, gcv, jgcsumm);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres get_gcv_summary_range(string &kqk, UInt64 bgcv, UInt64 egcv) {
  LT("get_gcv_summary_range");
  jv jagcsumms = JARRAY;
  for (uint i = bgcv; i <= egcv; i++) { // NOTE: INCLUDING(egcv)
    sqlres sr       = fetch_gcv_summary(kqk, i);
    RETURN_SQL_ERROR(sr)
    jv     *jgcsumm = &(sr.jres);
    if (zjd(jgcsumm)) zh_jv_append(&jagcsumms, jgcsumm);
  }
  RETURN_SQL_RESULT_JSON(jagcsumms)
}

static void union_tombstone_summaries(jv *dest, jv *src) {
  vector<string> mbrs = zh_jv_get_member_names(src);
  for (uint i = 0; i < mbrs.size(); i++) {
    string key = mbrs[i];
    if (!zh_jv_is_member(dest, key)) (*dest)[key] = (*src)[key];
    else {
      jv             *sval  = &((*src)[key]);
      jv             *dval  = &((*dest)[key]);
      vector<string>  smbrs = zh_jv_get_member_names(sval);
      for (uint j = 0; j < smbrs.size(); j++) {
        string skey   = smbrs[j];
        (*dval)[skey] = (*sval)[skey];
      }
    }
  }
}

static void union_objects(jv *dest, jv *src) {
  vector<string> mbrs = zh_jv_get_member_names(src);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  key  = mbrs[i];
    (*dest)[key] = (*src)[key];
  }
}

jv zgc_union_gcv_summaries(jv *jagcsumms) {
  LT("zgc_union_gcv_summaries");
  jv stsumm = JOBJECT;
  jv slreo  = JOBJECT;
  jv sulreo = JOBJECT;
  for (int i = (int)(jagcsumms->size() - 1); i >= 0; i--) { // DESENDING ORDER
    jv *jgcsumm = &((*jagcsumms)[i]);
    jv *tsumm   = &((*jgcsumm)["tombstone_summary"]);
    jv *lreo    = &((*jgcsumm)["lhn_reorder"]);
    jv *ulreo   = &((*jgcsumm)["undo_lhn_reorder"]);
    union_tombstone_summaries(&stsumm, tsumm); // EARLIEST GCV overwrites
    union_objects(&slreo, lreo);
    union_objects(&sulreo, ulreo);
  }
  jv ugcsumm                   = JOBJECT;
  ugcsumm["tombstone_summary"] = stsumm;
  ugcsumm["lhn_reorder"]       = slreo;
  ugcsumm["undo_lhn_reorder"]  = sulreo;
  zh_debug_json_value("zgc_union_gcv_summaries: ugcsumm", &ugcsumm);
  return ugcsumm;
}

sqlres zgc_get_gcv_summary_range_check(string &kqk, UInt64 bgcv, UInt64 egcv) {
  sqlres  sr       = get_gcv_summary_range(kqk, bgcv, egcv);
  RETURN_SQL_ERROR(sr)
  jv     *jagcsumms = &(sr.jres);
  UInt64  range     = egcv - bgcv;
  UInt64  ngcsumms  = jagcsumms->size();
  if (ngcsumms < range) {
    LE("ERROR: ZGC.GetGCVSummaryRangeCheck: RANGE: " << range <<
       " GOT: " << ngcsumms);
    RETURN_EMPTY_SQLRES
  } else {
    RETURN_SQL_RESULT_JSON(jagcsumms)
  }
}

sqlres zgc_get_unioned_gcv_summary(string &kqk, UInt64 bgcv, UInt64 egcv) {
  LT("zgc_get_unioned_gcv_summary");
  sqlres  sr        = zgc_get_gcv_summary_range_check(kqk, bgcv, egcv);
  RETURN_SQL_ERROR(sr)
  jv     *jagcsumms = &(sr.jres);
  if (zjn(jagcsumms)) RETURN_EMPTY_SQLRES
  else {
    jv jugcsumms = zgc_union_gcv_summaries(jagcsumms);
    RETURN_SQL_RESULT_JSON(jugcsumms)
  }
}

UInt64 zgc_get_min_gcv(jv *jgcsumms) {
  UInt64 min_gcv = ULLONG_MAX;
  vector<string> mbrs = zh_jv_get_member_names(jgcsumms);
  for (uint i = 0; i < mbrs.size(); i++) {
    UInt64 gcv = strtoul(mbrs[i].c_str(), NULL, 10);
    if (gcv < min_gcv) min_gcv = gcv;
  }
  return (min_gcv == ULLONG_MAX) ? 0 : min_gcv;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD REORDER TO GC-SUMMARIES -----------------------------------------------

static sqlres do_add_reorder_to_gc_summaries(string &kqk, jv *reorder) {
  for (uint i = 0; i < reorder->size(); i++) {
    jv     *reo    = &((*reorder)[i]);
    bool    skip   = zh_get_bool_member(reo, "skip");
    if (skip) continue;
    UInt64  gcv    = (*reo)["gcv"].asUInt64();
    LD("do_add_reorder_to_gc_summaries: K: " << kqk << " GCV: " << gcv);
    sqlres  sr     = fetch_gcv_summary(kqk, gcv);
    RETURN_SQL_ERROR(sr)
    jv     *jgcs   = &(sr.jres);
    jv     *rtsumm = &((*reo)["tombstone_summary"]);
    jv     *rlreo  = &((*reo)["lhn_reorder"]);
    jv     *rulreo = &((*reo)["undo_lhn_reorder"]);
    jv     *gtsumm = &((*jgcs)["tombstone_summary"]);
    jv     *glreo  = &((*jgcs)["lhn_reorder"]);
    jv     *gulreo = &((*jgcs)["undo_lhn_reorder"]);
    vector<string> mbrs = zh_jv_get_member_names(rtsumm);
    for (uint i = 0; i < mbrs.size(); i++) {
      string pid = mbrs[i];
      if (!zh_jv_is_member(gtsumm, pid)) {
       (*gtsumm)[pid] = (*rtsumm)[pid];
      } else {
        jv *rtmbs = &((*rtsumm)[pid]);
        jv *gtmbs = &((*gtsumm)[pid]);
        vector<string> tmbrs = zh_jv_get_member_names(rtmbs);
        for (uint j = 0; j < tmbrs.size(); j++) {
          string cid    = tmbrs[j];
          (*gtmbs)[cid] = (*rtmbs)[cid];
        }
      }
    }
    mbrs = zh_jv_get_member_names(rlreo);
    for (uint i = 0; i < mbrs.size(); i++) {
      string rid    = mbrs[i];
      (*glreo)[rid] = (*rlreo)[rid];
    }
    mbrs = zh_jv_get_member_names(rulreo);
    for (uint i = 0; i < mbrs.size(); i++) {
      string rid     = mbrs[i];
      (*gulreo)[rid] = (*rulreo)[rid];
    }
    sr = save_gcv_summary(kqk, gcv, jgcs);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

sqlres zgc_add_reorder_to_gc_summaries(string &kqk, jv *reorder) {
  jv creorder = *reorder; // NOTE: gets modified
  return do_add_reorder_to_gc_summaries(kqk, &creorder);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD OOO-GCW TO GC-SUMMARY -------------------------------------------------

static sqlres do_add_ooo_gcw_to_gc_summary(jv *pc, UInt64 gcv,
                                           jv *rometa, jv *reorder) {
  jv     *jks  = &((*pc)["ks"]);
  string  kqk  = (*jks)["kqk"].asString();
  sqlres  sr   = fetch_gcv_summary(kqk, gcv);
  RETURN_SQL_ERROR(sr)
  jv     *jgcs = &(sr.jres);
  if (zjn(jgcs)) RETURN_EMPTY_SQLRES
  if (!zh_jv_is_member(jgcs, "agent_reorder_deltas")) {
    (*jgcs)["agent_reorder_deltas"] = JARRAY;
  }
  jv ard;
  ard["rometa"]  = *rometa;
  ard["reorder"] = *reorder;
  zh_jv_append(&((*jgcs)["agent_reorder_deltas"]), &ard);
  return save_gcv_summary(kqk, gcv, jgcs);
}

static sqlres apply_before_current_gcv(jv *pc, jv *breo) {
  jv     *jks  = &((*pc)["ks"]);
  jv     *md   = &((*pc)["extra_data"]["md"]);
  string  kqk  = (*jks)["kqk"].asString();
  LE("apply_before_current_gcv: K: " << kqk << "  BREO[]: " << *breo);
  if (!zh_jv_is_member(pc, "ncrdt")) {
    zh_set_new_crdt(pc, md, &((*md)["ocrdt"]), false);
  }
  sqlres  sr   = backward_reorders(kqk, &((*pc)["ncrdt"]), breo);
  RETURN_SQL_ERROR(sr)
  return forward_reorders(kqk, &((*pc)["ncrdt"]), breo);
}

// IF Reorder[GCV] is BEFORE OGCV apply_reorder(forwards & backwards)
static sqlres handle_before_current_gcv(jv *pc, jv *reorder, UInt64 ogcv) {
  jv breo = JARRAY;
  for (uint i = 0; i < reorder->size(); i++) {
    jv     *reo  = &((*reorder)[i]);
    UInt64  rgcv = (*reo)["gcv"].asUInt64();
    if (rgcv <= ogcv) zh_jv_append(&breo, reo);
  }
  if (!breo.size()) RETURN_EMPTY_SQLRES
  else              return apply_before_current_gcv(pc, &breo);
}

sqlres zgc_add_ooogcw_to_gc_summary(jv *pc, UInt64 gcv, 
                                    jv *rometa, jv *reorder) {
  jv     *jks  = &((*pc)["ks"]);
  jv     *md   = &((*pc)["extra_data"]["md"]);
  string  kqk  = (*jks)["kqk"].asString();
  UInt64  ogcv = (*md)["ogcv"].asUInt64();
  LE("ZGC.AddOOOGCWToGCSummary: K: " << kqk << " GCV: " << gcv);
  sqlres  sr   = handle_before_current_gcv(pc, reorder, ogcv);
  RETURN_SQL_ERROR(sr)
  return do_add_ooo_gcw_to_gc_summary(pc, gcv, rometa, reorder);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT GC-WAIT -------------------------------------------------------------

sqlres zgc_finish_agent_gc_wait(jv *pc, UInt64 bgcv, UInt64 egcv) {
  jv     *jks       = &((*pc)["ks"]);
  string  kqk       = (*jks)["kqk"].asString();
  LE("FINISH-AGENT-GC-WAIT: K: " << kqk << " [" << bgcv << "-" << egcv << "]");
  sqlres  sr        = get_gcv_summary_range(kqk, bgcv, egcv);
  RETURN_SQL_ERROR(sr)
  jv     *jagcsumms = &(sr.jres); // ARRAY
  return zmerge_forward_crdt_gc_version(pc, jagcsumms);
}

sqlres zgc_cancel_agent_gc_wait(string &kqk) {
  LE("ZGC.CancelAgentGCWait: (GC-WAIT) K: " << kqk);
  return remove_all_agent_key_gcv_needs_reorder(kqk);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPER DO_GARBAGE_COLLECTION ----------------------------------------------

string zgc_get_array_id(jv *jcval) {
  UInt64 duuid = (*jcval)["_"].asUInt64();
  UInt64 step  = (*jcval)["#"].asUInt64();
  UInt64 ts    = (*jcval)["@"].asUInt64();
  return to_string(duuid) + "|" + to_string(step) + "|" + to_string(ts);
}

string zgc_get_tombstone_id(jv *jcval) { //LT("zgc_get_tombstone_id");
  UInt64 duuid = (*jcval)["_"].asUInt64();
  UInt64 step  = (*jcval)["#"].asUInt64();
  return to_string(duuid) + "|" + to_string(step);
}

static jv build_gcm_from_tombstone_summary(jv *tsumm) {
  jv gcm = JOBJECT;
  vector<string> mbrs = zh_jv_get_member_names(tsumm);
  for (uint i = 0; i < mbrs.size(); i++) {
    jv             *tmbs  = &((*tsumm)[mbrs[i]]);
    vector<string>  tmbrs = zh_jv_get_member_names(tmbs);
    for (uint j = 0; j < tmbrs.size(); j++) {
      string tid = tmbrs[j];
      gcm[tid]   = (*tmbs)[tid];
    }
  }
  return gcm;
}

static string get_tombstone_type(jv *tmb) {
  return (*tmb)[3].asString();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DO_GARBAGE_COLLECTION------------------------------------------------------

static void debug_delete_has_dangling_lhn(jv *jcval, jv *jchild, UInt64 i,
                                          jv *rchild, jv *gcm,
                                          jv *jlhn_reorder) {
  LE("GC-WAIT-INCOMPLETE: CHILD: " << *jchild << " HIT: " << i <<
     " RCHILD: " << *rchild << " CVAL: " << *jcval << " GCM: " << *gcm <<
     " LHNREORDER: " << *jlhn_reorder);
}

static void debug_do_gc_pre_lhn_reorder(jv *jchild, jv *jlhnr) {
  LD("PRE-REORDER: CHILD: " << *jchild);
  LD("PRE-REORDER: LHNR: "  << *jlhnr);
}

static void debug_do_gc_post_lhn_reorder(jv *jchild) {
  LD("POST-REORDER: CHILD: " << *jchild);
}

static bool delete_has_dangling_lhn(jv *jcval, uint spot,
                                    jv *gcm, jv *jlhn_reorder) {
  jv   *jchild = &((*jcval)[spot]);
  bool  hit    = false;
  // Search RIGHT: matching LHN
  for (uint i = (spot + 1); i < jcval->size(); i++) {
    jv     *rchild = &((*jcval)[i]);
    string  rcid   = zgc_get_tombstone_id(rchild);
    bool    rtodel = (zh_jv_is_member(rchild, "X") &&
                     zh_jv_is_member(gcm, rcid));
    jv     *rlhn   = &((*rchild)["<"]);
    bool    rroot  = ((*rlhn)["_"].asUInt64() == 0) &&
                     ((*rlhn)["#"].asUInt64() == 0);
    if (rtodel || rroot) continue; // SKIP TO-DELETE & ORIGINAL-ROOTS
    if ((*jchild)["_"].asUInt64() == (*rlhn)["_"].asUInt64() &&
        (*jchild)["#"].asUInt64() == (*rlhn)["#"].asUInt64()) {
      bool has_rlhnr = zh_jv_is_member(jlhn_reorder, rcid);
      if (!has_rlhnr) {
        hit = true;
        debug_delete_has_dangling_lhn(jcval, jchild, i, rchild,
                                      gcm, jlhn_reorder);
        break;
      }
    }
  }
  if (!hit) return false; // NO HIT -> OK
  return true;
}

static bool do_garbage_collection(jv *jcrdtd, jv *gcm, jv *jlhn_reorder) {
  jv     *jcval = &((*jcrdtd)["V"]);
  string  ctype = (*jcrdtd)["T"].asString();
  if (!ctype.compare("A")) {
    vector<int> tor;
    bool        reordered = false;
    for (uint i = 0; i < jcval->size(); i++) {
      jv     *jchild = &((*jcval)[i]);
      string  cid    = zgc_get_tombstone_id(jchild);
      bool    todel  = (zh_jv_is_member(jchild, "X") &&
                       zh_jv_is_member(gcm, cid));
      bool    hit    = zh_jv_is_member(jlhn_reorder, cid);
      if (hit) {
        jv *jlhnr           = &((*jlhn_reorder)[cid]);
        debug_do_gc_pre_lhn_reorder(jchild, jlhnr);
        (*jchild)["S"]      = (*jlhnr)[0];
        (*jchild)["<"]["_"] = (*jlhnr)[1];
        (*jchild)["<"]["#"] = (*jlhnr)[2];
        debug_do_gc_post_lhn_reorder(jchild);
        jlhn_reorder->removeMember(cid);
        reordered           = true;
      }
      if (todel) {
        jv     *tmb      = &((*gcm)[cid]);
        string  ttype    = get_tombstone_type(tmb);
        bool    isanchor = !ttype.compare("A");
        bool    check    = !isanchor;
        if (check && delete_has_dangling_lhn(jcval, i, gcm, jlhn_reorder)) {
          return false;
        }
        if (isanchor) (*jchild)["A"] = true; // MARK AS ANCHOR
        else          tor.push_back(i);      // REMOVE TOMBSTONE
        gcm->removeMember(cid);
      }
      bool ok = do_garbage_collection(jchild, gcm, jlhn_reorder);
      if (!ok) return false;
    }
    if (tor.size()) {
      LD("GC: removing: " << tor.size());
    }
    for (int i = (int)(tor.size() - 1); i >= 0; i--) {
      zh_jv_splice(jcval, tor[i]);
    }
    if (reordered) {
      zmerge_do_array_merge(jcval);
    }
    // NOTE: removing ORIGINAL-S/L [OS,OL] only done at CENTRAL
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv   *jchild = &((*jcval)[mbrs[i]]);
      bool  ok     = do_garbage_collection(jchild, gcm, jlhn_reorder);
      if (!ok) return false;
    }
  }
  return true;
}

bool zgc_do_garbage_collection(jv *pc, UInt64 gcv, jv *jtsumm, jv *jlhnr) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *md      = &((*pc)["extra_data"]["md"]);
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jcrdt   = &((*md)["ocrdt"]);
  jv     *jcmeta  = &((*jcrdt)["_meta"]);
  jv     *jcrdtd  = &((*jcrdt)["_data"]);
  LD("ZGC.DoGarbageCollection: K: " << kqk << " GCV: " << gcv);
  jv      gcm     = build_gcm_from_tombstone_summary(jtsumm);
  // NOTE: no ZDT.RunDatatypeFunctions() needed when removing tombstones
  bool    ok      = do_garbage_collection(jcrdtd, &gcm, jlhnr);
  if (!ok) return false;
  (*md)["gcv"]            = gcv; // NOTE: SET MD.GCV
  (*jcmeta)["GC_version"] = gcv; // Used in zh_set_new_crdt()
  zmerge_debug_crdt(jcrdt);
  return true;
}

// NOTE: returns CRDT
sqlres run_gc(jv *pc, jv *jdentry) {
  jv     *jks       = &((*pc)["ks"]);
  jv     *md        = &((*pc)["extra_data"]["md"]);
  string  kqk       = (*jks)["kqk"].asString();
  jv     *jcrdt     = &((*md)["ocrdt"]);
  (*jcrdt)["_meta"] = (*jdentry)["delta"]["_meta"];
  UInt64  gcv       = (*jcrdt)["_meta"]["GC_version"].asUInt64();
  // NOTE: delta.gc modified in do_garbage_collection()
  jv      jdgc      = (*jdentry)["delta"]["gc"];
  jv     *jtsumm    = &(jdgc["tombstone_summary"]);
  jv     *jlhnr     = &(jdgc["lhn_reorder"]);
  LD("ZGC.GarbageCollect: K: " << kqk << " SET-GCV: " << gcv);
  bool    ok        = zgc_do_garbage_collection(pc, gcv, jtsumm, jlhnr);
  if (ok) RETURN_SQL_RESULT_JSON((*md)["ocrdt"])
  else {
    sqlres sr = zoor_set_gc_wait_incomplete(kqk);
    RETURN_SQL_ERROR(sr)
    RETURN_EMPTY_SQLRES // NULL CRDT -> GC-WAIT-INCOMPLETE -> DONT STORE CRDT
  }
}

static void debug_diff_dependencies(jv *jdeps, jv *mdeps, jv *jdiffs) {
  LD("diff_dependencies: DEPS: "  << *jdeps);
  LD("diff_dependencies: MDEPS: " << *mdeps);
  LD("diff_dependencies: DIFFS: " << *jdiffs);
}

static sqlres diff_dependencies(string &kqk, jv *jdeps) {
  jv      jdiffs = JOBJECT;
  sqlres  sr     = zdelt_get_key_subscriber_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *mdeps  = &(sr.jres);
  vector<string> mbrs = zh_jv_get_member_names(mdeps);
  for (uint i = 0; i < mbrs.size(); i++) {
    string suuid  = mbrs[i];
    UInt64 isuuid = strtoul(suuid.c_str(), NULL, 10);
    if (isuuid <= MaxDataCenterUUID) continue;
    string mavrsn = (*mdeps)[suuid].asString();
    string davrsn = (*jdeps)[suuid].asString();
    UInt64 mavnum = zh_get_avnum(mavrsn);
    UInt64 davnum = zh_get_avnum(davrsn);
    if (mavnum > davnum) {
      jv jdiff;
      jdiff["mavrsn"] = mavrsn;
      jdiff["davrsn"] = davrsn;
      jdiffs[suuid]   = jdiff;
    }
  }
  debug_diff_dependencies(jdeps, mdeps, &jdiffs);
  RETURN_SQL_RESULT_JSON(jdiffs)
}

// NOTE: returns CRDT
static sqlres analyze_agent_gc_wait(jv *pc, jv *jdentry, jv *jdiffs) {
  jv     *jks = &((*pc)["ks"]);
  jv     *md  = &((*pc)["extra_data"]["md"]);
  UInt64  gcv = (*md)["gcv"].asUInt64();
  sqlres  sr  = zoor_analyze_key_gcv_needs_reorder_range(jks, gcv, jdiffs);
  RETURN_SQL_ERROR(sr)
  bool    ok  = sr.ures ? true : false;
  if (ok) return run_gc(pc, jdentry);
  else    RETURN_EMPTY_SQLRES // NULL CRDT -> GC-WAIT -> DONT STORE CRDT
}

// NOTE: returns CRDT
static sqlres agent_run_gc(jv *pc, jv *jdentry) {
  jv     *jks      = &((*pc)["ks"]);
  UInt64  gcv      = zh_get_dentry_gcversion(jdentry);
  string  kqk      = (*jks)["kqk"].asString();
  sqlres  sr       = zoor_agent_in_gc_wait(kqk);
  RETURN_SQL_ERROR(sr)
  bool    gwait    = sr.ures;
  LD("GC: (GC-WAIT) GET-KEY-GCV-NREO: K: " << kqk << " GCV: " << gcv <<
     " GW: " << gwait);
  if (gwait) {
    LE("DO_GC: K: " << kqk << " DURING AGENT-GC-WAIT -> NO-OP");
    RETURN_EMPTY_SQLRES // NULL CRDT -> GC-WAIT -> DONT STORE CRDT
  } else {
    jv     *jdeps  = &((*jdentry)["delta"]["_meta"]["dependencies"]);
    sqlres  sr     = diff_dependencies(kqk, jdeps);
    RETURN_SQL_ERROR(sr)
    jv     *jdiffs = &(sr.jres);
    bool    ok     = (jdiffs->size() == 0);
    LD("agent_run_gc: OK: " << ok);
    if (ok) return run_gc(pc, jdentry);
    else    return analyze_agent_gc_wait(pc, jdentry, jdiffs);
  }
}

// NOTE: returns CRDT
sqlres zgc_garbage_collect(jv *pc, jv *jdentry) {
  jv     *jks    = &((*pc)["ks"]);
  string  kqk    = (*jks)["kqk"].asString();
  UInt64  gcv    = zh_get_dentry_gcversion(jdentry);
  jv      tentry = (*jdentry)["delta"]["gc"];
  sqlres  sr     = save_gcv_summary(kqk, gcv, &tentry);
  RETURN_SQL_ERROR(sr)
  return agent_run_gc(pc, jdentry);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// UPDATE LEFT HAND NEIGHBOR -------------------------------------------------

void zgc_agent_post_merge_update_lhn(jv *aentry, jv *clm) {
  jv   *aval    = &((*aentry)["V"]);
  jv   *op_path = &((*aentry)["op_path"]);
  uint  lk      = zmerge_get_last_key_array_adding(clm, aval, op_path);
  if (!lk) zh_fatal_error("LOGIC(update_lhn)");
  jv  *lval    = aval;
  int  hit     = -1;
  deque<jv> js;
  js.push_front((*aval)["S"]);
  for (int i = (int)(lk -1); i >= 0; i--) { // Search Backwards
    jv *cval = &((*clm)[i]);
    // Find LHN (CHAIN)
    if ((*lval)["<"]["_"].asUInt64() == (*cval)["_"].asUInt64() &&
        (*lval)["<"]["#"].asUInt64() == (*cval)["#"].asUInt64()) {
      hit       = i;
      bool dhit = (zh_jv_is_member(cval, "A") || !zh_jv_is_member(cval, "X"));
      if (dhit) break; // DIRECT-HIT -> BREAK
      else {           // INDIRECT HIT (TOMBSTONE) -> CONTINUE
        js.push_front((*cval)["S"]);
        lval = cval;
      }
    }
  }
  if (hit == -1) {
    LD("NO NON-TOMBSTONE LHN FOUND: lk: " << lk);
  } else {
    jv s = JARRAY;
    for (uint i = 0; i < js.size(); i++) {
      zh_jv_append(&s, &(js[i]));
    }
    jv *nlhn          = &((*clm)[hit]);
    (*aval)["S"]      = s;
    (*aval)["<"]["_"] = (*nlhn)["_"];
    (*aval)["<"]["#"] = (*nlhn)["#"];
    LD("END: update_lhn: aval: " << *aval);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REORDER DELTA -------------------------------------------------------------

static sqlres apply_backward_undo_lhn_reorder(jv *jcrdtd, jv *julhnr) {
  jv     *jcval = &((*jcrdtd)["V"]);
  string  ctype = (*jcrdtd)["T"].asString();
  if        (!ctype.compare("A")) {
    for (uint i = 0; i < jcval->size(); i++) {
      jv *jchild = &((*jcval)[i]);
      zmerge_check_apply_reorder_element(jchild, julhnr);
      apply_backward_undo_lhn_reorder(jchild, julhnr);
    }
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv *jchild = &((*jcval)[mbrs[i]]);
      apply_backward_undo_lhn_reorder(jchild, julhnr);
    }
  }
  RETURN_EMPTY_SQLRES
}

static sqlres forward_reorders(string &kqk, jv *jcrdt, jv *reorder) {
  jv  gcm    = JOBJECT; //NOTE: EMPTY
  jv *jcrdtd = &((*jcrdt)["_data"]);
  for (uint i = 0; i < reorder->size(); i++) {
    jv     *reo    = &((*reorder)[i]);
    bool    skip   = zh_get_bool_member(reo, "skip");
    if (skip) continue;
    UInt64  gcv    = (*reo)["gcv"].asUInt64();
    LD("forward_reorder: K: " << kqk << " GCV: " << gcv);
    jv     *jlhnr  = &((*reo)["lhn_reorder"]);
    jv      gcm    = JOBJECT;
    // NOTE: ReorderDelta can NOT FAIL due to GC-WAIT-INCOMPLETE
    do_garbage_collection(jcrdtd, &gcm, jlhnr);
    (*jcrdt)["_meta"]["GC_version"] = gcv; // Used in ZH.SetNewCrdt()
  }
  RETURN_EMPTY_SQLRES
}

static sqlres backward_reorders(string &kqk, jv *jcrdt, jv *reorder) {
  jv *jcrdtd = &((*jcrdt)["_data"]);
  for (uint i = 0; i < reorder->size(); i++) {
    jv     *reo    = &((*reorder)[i]);
    bool    skip   = zh_get_bool_member(reo, "skip");
    if (skip) continue;
    UInt64  gcv    = (*reo)["gcv"].asUInt64();
    LD("backward_reorder: K: " << kqk << " GCV: " << gcv);
    jv     *julhnr = &((*reo)["undo_lhn_reorder"]);
    sqlres  sr     = apply_backward_undo_lhn_reorder(jcrdtd, julhnr);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

// NOTE: returns CRDT
sqlres zgc_reorder(string &kqk, jv *jcrdt, jv *jdentry) {
  LT("ZGC.Reorder: K: " << kqk);
  if (zjn(jcrdt)) RETURN_EMPTY_SQLRES // NOTE: CRDT is NULL (REMOVE)
  else {
    UInt64  gcv             = (*jcrdt)["_meta"]["GC_version"].asUInt64();
    (*jcrdt)["_meta"]       = (*jdentry)["delta"]["_meta"];
    jv     *jcmeta          = &((*jcrdt)["_meta"]);
    (*jcmeta)["GC_version"] = gcv; // OCRDT GCV is correct
    jv      reorder         = (*jdentry)["delta"]["reorder"]; // gets modified
    sqlres  sr              = backward_reorders(kqk, jcrdt, &reorder);
    RETURN_SQL_ERROR(sr)
    sr                      = forward_reorders (kqk, jcrdt, &reorder);
    RETURN_SQL_ERROR(sr)
    jv     *jcrdtd          = &((*jcrdt)["_data"]);
    jv     *jcrdtdv         = &((*jcrdtd)["V"]);
    zmerge_deep_debug_merge_crdt(jcmeta, jcrdtdv, false);
    RETURN_SQL_RESULT_JSON(*jcrdt)
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

sqlres zgc_get_gcversion(string &kqk) {
  return fetch_gc_version(kqk);
}


