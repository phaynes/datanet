
#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>
#include <iostream>

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
#include "external_hooks.h"
#include "shared.h"
#include "merge.h"
#include "deltas.h"
#include "subscriber_delta.h"
#include "ooo_replay.h"
#include "apply_delta.h"
#include "activesync.h"
#include "gc.h"
#include "agent_daemon.h"
#include "datastore.h"
#include "trace.h"
#include "storage.h"

using namespace std;

extern Int64 MyUUID;


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
// DEBUG ---------------------------------------------------------------------

static string debug_dentries_agent_versions(jv *rdentries) {
  string ret;
  for (uint i = 0; i < rdentries->size(); i++) {
    jv     *jdentry  = &((*rdentries)[i]);
    jv     *jauthor  = &((*jdentry)["delta"]["_meta"]["author"]);
    string  avrsn    = (*jauthor)["agent_version"].asString();
    if (ret.size()) ret += ", ";
    ret             += avrsn;
  }
  return ret;
}

static void debug_replay_merge_dentries(string &kqk, jv *rdentries) {
  if (!rdentries->size()) {
    LE("ZOOR.replay_local_deltas: K: " << kqk << " ZERO DENTRIES");
  } else {
    LE("ZOOR.replay_local_deltas: K: " << kqk << " rdentries[AV]: [" <<
         debug_dentries_agent_versions(rdentries) << "]");
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static bool DebugCmpDeltaAgentVersion = false;

static bool cmp_delta_agent_version(jv *javrsn1, jv *javrsn2) {
  string         avrsn1  = javrsn1->asString();
  vector<string> res1    = split(avrsn1, '|');
  UInt64         auuid1  = strtoul(res1[0].c_str(), NULL, 10);
  UInt64         avnum1  = strtoul(res1[1].c_str(), NULL, 10);
  string         avrsn2  = javrsn2->asString();
  vector<string> res2    = split(avrsn2, '|');
  UInt64         auuid2  = strtoul(res2[0].c_str(), NULL, 10);
  UInt64         avnum2  = strtoul(res2[1].c_str(), NULL, 10);
  if (DebugCmpDeltaAgentVersion && (auuid1 == auuid2) && (avnum1 == avnum2)) {
    string ferr = "cmp_delta_agent_version: REPEAT AV: AV1: " + avrsn1 +
                  " AV2: " + avrsn2;
    zh_fatal_error(ferr.c_str());
  }
  if (auuid1 != auuid2) return (auuid1 < auuid2);
  else                  return (avnum1 < avnum2);
}

static jv create_reference_author(jv *jdentry) {
  jv     *jdmeta   = &((*jdentry)["delta"]["_meta"]);
  UInt64  rfauuid  = (*jdmeta)["reference_uuid"].asUInt64();
  string  rfavrsn  = (*jdmeta)["reference_version"].asString();
  jv      rfauthor = JOBJECT;
  rfauthor["agent_uuid"]    = rfauuid;
  rfauthor["agent_version"] = rfavrsn;
  return rfauthor;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GC-WAIT_INCOMPLETE --------------------------------------------------------

sqlres zoor_set_gc_wait_incomplete(string &kqk) {
  LD("START: GC-WAIT_INCOMPLETE: K: " << kqk);
  return persist_agent_key_gc_wait_incomplete(kqk);
}

sqlres zoor_end_gc_wait_incomplete(string &kqk) {
  LD("END: GC-WAIT_INCOMPLETE: K: " << kqk);
  return remove_agent_key_gc_wait_incomplete(kqk);
}

sqlres zoor_agent_in_gc_wait(string &kqk) {
  sqlres  sr       = fetch_all_agent_key_gcv_needs_reorder(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jgavrsns = &(sr.jres);
  bool    empty    = (zjn(jgavrsns) || jgavrsns->size() == 0);
  bool    gwait    = !empty;
  if (gwait) RETURN_SQL_RESULT_UINT(1)
  sr               = fetch_agent_key_gc_wait_incomplete(kqk);
  RETURN_SQL_ERROR(sr)
  bool    gcwi     = sr.ures ? true : false;
  LD("REO: (GC-WAIT) K: " << kqk << " GCW(INCOMPLETE): " << gcwi);
  if (gcwi) RETURN_SQL_RESULT_UINT(1) // POSSIBLE END-GC-WAIT-INCOMPLETE
  else      RETURN_EMPTY_SQLRES
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET/SET/DECREMENT KEY-GCV-NREO --------------------------------------------

static sqlres get_num_agent_key_gcv_needs_reorder(string &kqk, UInt64 gcv) {
  sqlres  sr      = fetch_agent_key_gcv_needs_reorder(kqk, gcv);
  RETURN_SQL_ERROR(sr)
  jv     *javrsns = &(sr.jres);
  UInt64  nreo    = javrsns->size();
  RETURN_SQL_RESULT_UINT(nreo);
}

static sqlres do_remove_agent_key_gcv_needs_reorder(string &kqk, UInt64 gcv) {
  LD("REMOVE-KEY-GCV-NREO: (GC-WAIT): K: " << kqk << " GCV: " << gcv);
  sqlres  sr       = fetch_all_agent_key_gcv_needs_reorder(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jgavrsns = &(sr.jres);
  jgavrsns->removeMember("_id");
  bool    empty    = true;
  vector<string> mbrs = zh_jv_get_member_names(jgavrsns);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  sgcv    = mbrs[i];
    jv     *javrsns = &((*jgavrsns)[sgcv]);
    UInt64  nreo    = javrsns->size();
    if (nreo > 0) {
      empty = false;
      break;
    }
  }
  LD("do_remove_agent_key_gcv_needs_reorder: empty: " << empty);
  if (empty) return remove_all_agent_key_gcv_needs_reorder(kqk);
  else       return remove_agent_key_gcv_needs_reorder(kqk, gcv);
}

static void debug_remove_agent_version_agent_key_gcv_nreo(string avrsn,
                                                          jv *javrsns,
                                                          UInt64 nreo) {
  LD("REMOVE-AV-KGNREO: AV: " << avrsn << " NREO: " << nreo <<
     " AVS[]; " << *javrsns);
}

static sqlres remove_agent_version_agent_key_gcv_needs_reorder(string &kqk,
                                                               UInt64 gcv,
                                                               string avrsn) {
  sqlres  sr      = fetch_agent_key_gcv_needs_reorder(kqk, gcv);
  RETURN_SQL_ERROR(sr)
  jv     *javrsns = &(sr.jres);
  javrsns->removeMember(avrsn); // REMOVE SINGLE AGENT-VERSION
  UInt64  nreo    = javrsns->size();
  debug_remove_agent_version_agent_key_gcv_nreo(avrsn, javrsns, nreo);
  sr              = persist_agent_key_gcv_needs_reorder(kqk, gcv, javrsns);
  RETURN_SQL_ERROR(sr)
  sr              = remove_agent_delta_gcv_needs_reorder(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  RETURN_SQL_RESULT_UINT(nreo)
}

static sqlres decrement_agent_key_gcv_needs_reorder(string &kqk, UInt64 gcv,
                                                    string avrsn) {
  LD("decrement_agent_key_gcv_needs_reorder: K: " << kqk << " GCV: " << gcv);
  sqlres sr   = get_num_agent_key_gcv_needs_reorder(kqk, gcv);
  RETURN_SQL_ERROR(sr)
  UInt64 nreo = sr.ures;
  LD("REO: (GC-WAIT) GET-KEY-GCV-NREO: K: " << kqk << " GCV: " << gcv <<
     " N: " << nreo);
  if (!nreo) RETURN_EMPTY_SQLRES   // RETURN FALSE
  else {
    string  sgcv  = to_string(gcv);
    sqlres  sr    = remove_agent_version_agent_key_gcv_needs_reorder(kqk, gcv,
                                                                     avrsn);
    RETURN_SQL_ERROR(sr)
    UInt64  nnreo = sr.ures;
    LD("DECREMENT-KEY-GCV-NREO: (GC-WAIT) K: " << kqk << " GCV: " << gcv <<
       " (N)NREO: " << nnreo);
    if (nnreo) RETURN_EMPTY_SQLRES // RETURN FALSE
    else {
      sqlres sr = do_remove_agent_key_gcv_needs_reorder(kqk, gcv);
      RETURN_SQL_ERROR(sr)
      RETURN_SQL_RESULT_UINT(1)    // RETURN TRUE
    }
  }
}

static jv get_per_gcv_number_key_gcv_needs_reorder(string &kqk, UInt64 xgcv,
                                                   jv *jdentries) {
  jv gavrsns = JOBJECT;
  for (uint i = 0; i < jdentries->size(); i++) {
    jv     *jdentry   = &((*jdentries)[i]);
    UInt64  dgcv      = zh_get_dentry_gcversion(jdentry);
    jv     *jdmeta    = &((*jdentry)["delta"]["_meta"]);
    string  avrsn     = (*jdmeta)["author"]["agent_version"].asString();
    bool    fcentral  = zh_get_bool_member(jdmeta, "from_central");
    bool    gmismatch = (xgcv != dgcv);
    bool    do_ignore = zh_get_bool_member(jdmeta, "DO_IGNORE");
    // (AGENT-DELTA) AND (DELTA(GCV) mismatch XCRDT) AND (NOT AN IGNORE)
    if (!fcentral && gmismatch && !do_ignore) {
      UInt64 gc_gcv  = dgcv + 1; // NEXT GC_version will be GC-WAIT
      string sgc_gcv = to_string(gc_gcv);
      if (!zh_jv_is_member(&gavrsns, sgc_gcv)) gavrsns[sgc_gcv] = JOBJECT;
      gavrsns[sgc_gcv][avrsn] = true;
    }
  }
  return gavrsns;
}

static UInt64 get_total_number_key_gcv_needs_reorder(string &kqk, UInt64 xgcv,
                                                     jv *jdentries) {
  jv     gavrsns = get_per_gcv_number_key_gcv_needs_reorder(kqk, xgcv,
                                                            jdentries);
  UInt64 tot     = 0;
  vector<string> mbrs = zh_jv_get_member_names(&gavrsns);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  sgcv     = mbrs[i];
    jv     *javrsns  = &(gavrsns[sgcv]);
    UInt64  nreo     = javrsns->size();
    tot             += nreo;
  }
  return tot;
}

static sqlres set_delta_gcv_needs_reorder(string &kqk, jv *javrsns) {
  vector<string> mbrs = zh_jv_get_member_names(javrsns);
  for (uint i = 0; i < mbrs.size(); i++) {
    string avrsn = mbrs[i];
    sqlres sr    = persist_agent_delta_gcv_needs_reorder(kqk, avrsn);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static void debug_set_per_gcv_key_gcv_needs_reorder(string &kqk, UInt64 gcv,
                                                    jv *javrsns, UInt64 nreo) {
  LE("(GC-WAIT) SET-KEY-GCV-NREO: K: " << kqk << " GCV: " << gcv <<
     " N: " << nreo);
  LD("(GC-WAIT) SET-KEY-GCV-NREO: K: " << kqk << " AVS[]: " << *javrsns);
}

static sqlres set_key_gcv_needs_reorder(string &kqk, UInt64 xgcv,
                                        jv *jdentries) {
  jv gavrsns = get_per_gcv_number_key_gcv_needs_reorder(kqk, xgcv, jdentries);
  vector<string> mbrs = zh_jv_get_member_names(&gavrsns);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  sgcv    = mbrs[i];
    UInt64  gcv     = strtoul(sgcv.c_str(), NULL, 10);
    jv     *javrsns = &(gavrsns[sgcv]);
    UInt64  nreo    = javrsns->size();
    debug_set_per_gcv_key_gcv_needs_reorder(kqk, gcv, javrsns, nreo);
    sqlres  sr      = persist_agent_key_gcv_needs_reorder(kqk, gcv, javrsns);
    RETURN_SQL_ERROR(sr)
    sr              = set_delta_gcv_needs_reorder(kqk, javrsns);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ANALYZE KEY-GCV-NREO ------------------------------------------------------

static bool analyze_pending_deltas_on_do_gc(UInt64 cgcv, jv *jdentries) {
  vector<int> tor;
  for (uint i = 0; i < jdentries->size(); i++) {
    jv     *jdentry = &((*jdentries)[i]);
    UInt64  dgcv    = zh_get_dentry_gcversion(jdentry);
    if (dgcv < cgcv) {
      jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
      string  avrsn   = (*jauthor)["agent_version"].asString();
      LD("analyze_pending_deltas_on_do_gc: IGNORE: AV: " << avrsn <<
           " CGCV: " << cgcv << " DGCV: " << dgcv);
      tor.push_back(i);
    }
  }
  for (int i = (int)(tor.size() - 1); i >= 0; i--) {
    zh_jv_splice(jdentries, tor[i]);
  }
  bool ok = (jdentries->size() == 0); // IGNORE ALL DELTAS -> DO RUN-GC
  LD("analyze_pending_deltas_on_do_gc: OK: " << ok);
  return ok;
}

sqlres zoor_analyze_key_gcv_needs_reorder_range(jv *jks, UInt64 cgcv,
                                                jv *jdiffs) {
  LD("ZOOR.AnalyzeKeyGCVNeedsReorderRange: (C)GCV: " << cgcv <<
       " DIFFS: " << *jdiffs);
  string kqk     = (*jks)["kqk"].asString();
  UInt64 xgcv    = -1; // NOTE: JERRY-RIG to IGNORE gmismatch check
  jv     authors = JARRAY;
  vector<string> mbrs = zh_jv_get_member_names(jdiffs);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  suuid  = mbrs[i];
    UInt64  isuuid = strtoul(suuid.c_str(), NULL, 10);
    jv     *jdiff  = &((*jdiffs)[suuid]);
    string  mavrsn = (*jdiff)["mavrsn"].asString();
    string  davrsn = (*jdiff)["davrsn"].asString();
    UInt64  mavnum = zh_get_avnum(mavrsn);
    UInt64  davnum = zh_get_avnum(davrsn);
    // DAVNUM is OK(SKIP), MAVNUM NEEDS REORDER(INCLUDE)
    for (uint avnum = (davnum + 1); avnum <= mavnum; avnum++) {
      string avrsn            = zh_create_avrsn(isuuid, avnum);
      jv     author           = JOBJECT;
      author["agent_uuid"]    = isuuid;
      author["agent_version"] = avrsn;
      zh_jv_append(&authors, &author);
    }
  }
  jv mdentries = JARRAY;
  for (uint i = 0; i < authors.size(); i++) {
    jv     *author  = &(authors[i]);
    sqlres  sr      = zsd_fetch_subscriber_delta(kqk, author);
    RETURN_SQL_ERROR(sr)
    jv     *jdentry = &(sr.jres);
    if (zjd(jdentry)) zh_jv_append(&mdentries, jdentry);
  }
  bool ok = analyze_pending_deltas_on_do_gc(cgcv, &mdentries);
  if (ok) RETURN_SQL_RESULT_UINT(1) // OK -> DO RUN-GC
  else {
    sqlres sr = set_key_gcv_needs_reorder(kqk, xgcv, &mdentries);
    RETURN_SQL_ERROR(sr)
    RETURN_EMPTY_SQLRES             // NOT OK -> DO NOT RUN-GC -> GC-WAIT
  }
}

sqlres get_max_zero_level_needs_reorder(string &kqk, UInt64 gcv, UInt64 rogcv) {
  LD("get_max_zero_level_needs_reorder: K: " << kqk << " GCV: " << gcv <<
     " ROGCV: " << rogcv);
  sqlres sr   = get_num_agent_key_gcv_needs_reorder(kqk, gcv);
  RETURN_SQL_ERROR(sr)
  UInt64 nreo = sr.ures;
  LD("GET_ZERO_LEVEL: (GC-WAIT) GET-KEY-GCV-NREO: K: " << kqk <<
     " GCV: " << gcv << " N: " << nreo);
  if (nreo) { // NOT ZERO LEVEL, RETURN PREVIOUS GCV
    RETURN_SQL_RESULT_UINT((gcv - 1))
  } else {
    if (gcv == rogcv) { // MAX POSSIBLE GCV
      RETURN_SQL_RESULT_UINT(gcv);
    } else {
      gcv = gcv + 1; // CHECK NEXT GCV
      return get_max_zero_level_needs_reorder(kqk, gcv, rogcv);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REORDER SUBSCRIBER DELTA --------------------------------------------------

static sqlres do_process_reorder_delta(jv *pc, bool is_sub) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jdentry = &((*pc)["dentry"]);
  jv     *md      = &((*pc)["extra_data"]["md"]);
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
  string  avrsn   = (*jauthor)["agent_version"].asString();
  LE("ZOOR.REORDER: APPLY: K: " << kqk << " (R)AV: " << avrsn);
  sqlres  sr      = zsd_internal_handle_subscriber_delta(jks, jdentry, md);
  RETURN_SQL_ERROR(sr)
  bool    ok      = sr.ures ? true : false;
  jv     *jncrdt  = &(sr.jres);
  LD("ZOOR.do_process_reorder_delta: OK: " << ok);
  if (!ok) RETURN_EMPTY_SQLRES // INTERNAL NO-OP
  else {                       // SUCCESFFULY APPLIED -> NO LONGER OOO
    if (zjd(jncrdt)) zh_set_new_crdt(pc, md, jncrdt, false);
    sqlres sr = zoor_unset_ooo_gcv_delta(kqk, jauthor);
    RETURN_SQL_ERROR(sr)
    return zoor_do_remove_ooo_delta(kqk, jauthor);
  }
}

static sqlres process_reorder_delta(jv *pc, jv *rdentry, bool is_sub) {
  LT("process_reorder_delta");
  jv      rodentry = (*pc)["dentry"]; // FROM SUBSCRIBER-DELTA
  jv      rrdentry = zmerge_add_reorder_to_reference(&rodentry, rdentry);
  (*pc)["dentry"]  = rrdentry;         // [REORDER + REFERENCE] DELTA
  sqlres  sr       = do_process_reorder_delta(pc, is_sub);
  RETURN_SQL_ERROR(sr)
  sr               = zoor_check_replay_ooo_deltas(pc, true, is_sub);
  RETURN_SQL_ERROR(sr)
  (*pc)["dentry"]  = rodentry;         // FROM SUBSCRIBER-DELTA
  RETURN_EMPTY_SQLRES
}

static void debug_post_check_agent_gc_wait(bool fgcw, bool gcwi, bool zreo,
                                           UInt64 rfgcv, UInt64 ogcv) {
  LD("post_check_agent_gc_wait: (GC-WAIT): FIN-GCW: " << fgcw <<
     " GCWI: "  << gcwi  << " ZREO: " << zreo <<
     " RFGCV: " << rfgcv << " OGCV: " << ogcv);
}

static sqlres post_check_agent_gc_wait(jv *pc, bool zreo) {
  jv     *jks      = &((*pc)["ks"]);
  jv     *rodentry = &((*pc)["dentry"]);
  jv     *md       = &((*pc)["extra_data"]["md"]);
  string  kqk      = (*jks)["kqk"].asString();
  jv     *rometa   = &((*rodentry)["delta"]["_meta"]);
  UInt64  ogcv     = (*md)["ogcv"].asUInt64();
  UInt64  rogcv    = zh_get_dentry_gcversion(rodentry);
  UInt64  rfgcv    = (*rometa)["reference_GC_version"].asUInt64();
  sqlres  sr       = fetch_agent_key_gc_wait_incomplete(kqk);
  RETURN_SQL_ERROR(sr)
  bool    gcwi     = sr.ures ? true : false;
  bool    fgcw     = gcwi || (zreo && (rfgcv == ogcv)); // OGCV governs FIN-GCW
  debug_post_check_agent_gc_wait(fgcw, gcwi, zreo, rfgcv, ogcv);
  if (!fgcw) RETURN_EMPTY_SQLRES
  else {
    UInt64 bgcv;
    if (gcwi) bgcv = ogcv  + 1; // OGCV already applied
    else      bgcv = rfgcv + 1; // REFERENCE-DELTA-GCV already applied
    if (!zh_jv_is_member(pc, "ncrdt")) {
      zh_set_new_crdt(pc, md, &((*md)["ocrdt"]), false);
    }
    sqlres sr   = get_max_zero_level_needs_reorder(kqk, bgcv, rogcv);
    RETURN_SQL_ERROR(sr)
    UInt64 egcv = sr.ures;
    if (egcv >= bgcv) return zgc_finish_agent_gc_wait(pc, bgcv, egcv);
    else {
      LE("FINISH-AGENT-GC-WAIT: NON-MINIMUM-GCV: BGCV: " << bgcv <<
               " EGCV: " << egcv << "-> NO-OP");
      RETURN_EMPTY_SQLRES
    }
  }
}

sqlres zoor_decrement_reorder_delta(jv *pc) {
  jv     *jks      = &((*pc)["ks"]);
  string  kqk      = (*jks)["kqk"].asString();
  jv     *rodentry = &((*pc)["dentry"]);
  jv     *rometa   = &((*rodentry)["delta"]["_meta"]);
  string  rfavrsn  = (*rometa)["reference_version"].asString();
  UInt64  rfgcv    = (*rometa)["reference_GC_version"].asUInt64();
  UInt64  gc_gcv   = rfgcv + 1; // NEXT GC_version will be GC-WAIT
  LD("ZOOR.DecrementReorderDelta: K: " << kqk << " GCV: " << gc_gcv);
  sqlres  sr      = decrement_agent_key_gcv_needs_reorder(kqk, gc_gcv, rfavrsn);
  RETURN_SQL_ERROR(sr)
  bool    zreo    = sr.ures ? true : false;
  return post_check_agent_gc_wait(pc, zreo);
}

static sqlres agent_post_reorder_delta(jv *pc) {
  jv     *jks = &((*pc)["ks"]);
  string  kqk = (*jks)["kqk"].asString();
  LD("agent_post_reorder_delta: K: " << kqk);
  return zoor_decrement_reorder_delta(pc);
}

static sqlres set_wait_on_reference_delta(jv *pc, jv *roauthor, jv *rfauthor) {
  LE("set_wait_on_reference_delta: REFERENCE-DELTA NOT YET ARRIVED");
  jv     *jks = &((*pc)["ks"]);
  string  kqk = (*jks)["kqk"].asString();
  // PERSIST BOTH NEED_REFERENCE & NEED_REORDER
  sqlres  sr  = persist_delta_need_reference(kqk, roauthor, rfauthor);
  RETURN_SQL_ERROR(sr)
  return persist_delta_need_reorder(kqk, rfauthor, roauthor);
}

static sqlres get_need_apply_reorder_delta(string &kqk, jv *rfauthor,
                                           bool is_sub) {
  UInt64  rauuid  = (*rfauthor)["agent_uuid"].asUInt64();
  string  ravrsn  = (*rfauthor)["agent_version"].asString();
  bool    rselfie = ((int)rauuid == MyUUID);
  if (rselfie) {
    LD("get_need_apply_reorder_delta: RSELFIE: NEED: true");
    RETURN_SQL_RESULT_UINT(1) // RETURN TRUE
  }
  sqlres  sr      = zdelt_get_key_subscriber_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *mavrsns = &(sr.jres);
  if (zjn(mavrsns)) {
    LD("get_need_apply_reorder_delta: NO SAVRSNS[]: NEED: false");
    RETURN_EMPTY_SQLRES       // RETURN FALSE
  } else {
    string srauuid = to_string(rauuid);
    string mavrsn  = (*mavrsns)[srauuid].asString();
    UInt64 mavnum  = zh_get_avnum(mavrsn);
    UInt64 ravnum  = zh_get_avnum(ravrsn);
    bool   need    = (mavnum && mavnum < ravnum);
    LD("get_need_apply_reorder_delta: RUUID: " << rauuid <<
       " SAVRSN: " << mavrsn << " RAVRSN: " << ravrsn << " NEED: " << need);
    uint   uneed   = need ? 1 : 0;
    RETURN_SQL_RESULT_UINT(uneed)
  }
}

static sqlres persist_set_wait_on_reference_delta(jv *pc, jv *rodentry,
                                                  jv *rfauthor) {
  LE("FINISH-GC-WAIT-WAIT-ON-REFERENCE");
  jv     *jks      = &((*pc)["ks"]);
  string  kqk      = (*jks)["kqk"].asString();
  sqlres  sr       = zsd_conditional_persist_subscriber_delta(kqk, rodentry);
  RETURN_SQL_ERROR(sr)
  jv     *rometa   = &((*rodentry)["delta"]["_meta"]);
  jv     *roauthor = &((*rometa)["author"]);
  return set_wait_on_reference_delta(pc, roauthor, rfauthor);
}

static sqlres do_finish_gc_wait_apply_reorder_delta(jv *pc, jv *rodentry,
                                                    jv *rfdentry) {
  bool    is_sub  = true;
  jv      odentry = (*pc)["dentry"]; // ORIGINAL DENTRY that CAUSED ForwardGCV
  (*pc)["dentry"] = *rodentry;
  LE("FINISH-GC-WAIT-APPLY-REORDER-DELTA");
  // NOTE: APPLY [RODENTRY & RFDENTRY] to CRDT
  sqlres  sr      = process_reorder_delta(pc, rfdentry, is_sub);
  RETURN_SQL_ERROR(sr)
  (*pc)["dentry"] = odentry;         // REPLACE WITH ORIGINAL DENTRY
  RETURN_EMPTY_SQLRES
}

sqlres zoor_forward_crdt_gc_version_apply_reference_delta(jv *pc,
                                                          jv *rodentry) {
  LT("zoor_forward_crdt_gc_version_apply_reference_delta");
  bool    is_sub   = true;
  jv     *jks      = &((*pc)["ks"]);
  string  kqk      = (*jks)["kqk"].asString();
  jv      rfauthor = create_reference_author(rodentry);
  sqlres  sr       = get_need_apply_reorder_delta(kqk, &rfauthor, is_sub);
  RETURN_SQL_ERROR(sr)
  bool    need     = sr.ures ? true : false;
  if (!need) RETURN_EMPTY_SQLRES
  else {
    sqlres  sr       = zsd_fetch_subscriber_delta(kqk, &rfauthor);
    RETURN_SQL_ERROR(sr)
    jv     *rfdentry = &(sr.jres);
    if (zjd(rfdentry)) {
      return do_finish_gc_wait_apply_reorder_delta(pc, rodentry, rfdentry);
    } else {
      return persist_set_wait_on_reference_delta(pc, rodentry, &rfauthor);
    }
  }
}

static sqlres apply_reference_delta(jv *pc, jv *rfdentry, bool is_sub) {
  if (zjd(rfdentry)) { // NORMAL REORDER DELTA PROCESSING
    return process_reorder_delta(pc, rfdentry, is_sub);
  } else { // REFERENCE-DENTRY NOT YET ARRIVED -> 
    jv *rodentry = &((*pc)["dentry"]);
    jv *rometa   = &((*rodentry)["delta"]["_meta"]);
    jv *roauthor = &((*rometa)["author"]);
    jv  rfauthor = create_reference_author(rodentry);
    return set_wait_on_reference_delta(pc, roauthor, &rfauthor);
  }
}

static sqlres do_apply_reorder_delta(jv *pc, bool is_sub) {
  LT("do_apply_reorder_delta");
  jv     *jks      = &((*pc)["ks"]);
  jv     *rodentry = &((*pc)["dentry"]);
  string  kqk      = (*jks)["kqk"].asString();
  jv     *rometa   = &((*rodentry)["delta"]["_meta"]);
  jv      rfauthor = create_reference_author(rodentry);
  sqlres  sr       = get_need_apply_reorder_delta(kqk, &rfauthor, is_sub);
  RETURN_SQL_ERROR(sr)
  bool    need     = sr.ures ? true : false;
  if (!need) { // APPLY REORDER[] but NOT REFERENCE_DELT
    bool rignore = zh_get_bool_member(rometa, "reference_ignore");
    if (rignore) { // Used in ZAD.do_apply_delta()
      (*rometa)["DO_IGNORE"] = true;
    } else {       // Used in ZAD.do_apply_delta()
      (*rometa)["REORDERED"] = true;
    }
    return zad_apply_subscriber_delta(pc);
  } else {
    sqlres  sr      = zsd_fetch_subscriber_delta(kqk, &rfauthor);
    RETURN_SQL_ERROR(sr)
    jv     *rfdentry = &(sr.jres);
    return apply_reference_delta(pc, rfdentry, is_sub);
  }
}

static sqlres normal_agent_apply_reorder_delta(jv *pc) {
  bool    is_sub   = true;
  jv     *jks      = &((*pc)["ks"]);
  jv     *rodentry = &((*pc)["dentry"]);
  string  kqk      = (*jks)["kqk"].asString();
  jv     *reorder  = &((*rodentry)["delta"]["reorder"]);
  LD("normal_agent_apply_reorder_delta: K: " << kqk);
  sqlres  sr       = zgc_add_reorder_to_gc_summaries(kqk, reorder);
  RETURN_SQL_ERROR(sr)
  sr               = do_apply_reorder_delta(pc, is_sub);
  RETURN_SQL_ERROR(sr)
  return agent_post_reorder_delta(pc);
}

static sqlres gc_wait_agent_apply_reorder_delta(jv *pc) {
  jv     *jks      = &((*pc)["ks"]);
  jv     *rodentry = &((*pc)["dentry"]);
  string  kqk      = (*jks)["kqk"].asString();
  UInt64  rogcv    = zh_get_dentry_gcversion(rodentry);
  jv     *rometa   = &((*rodentry)["delta"]["_meta"]);
  jv     *reorder  = &((*rodentry)["delta"]["reorder"]);
  LE("DO_REORDER: OOOGCW: K: " << kqk << " DURING AGENT-GC-WAIT");
  sqlres  sr      = zgc_add_reorder_to_gc_summaries(kqk, reorder);
  RETURN_SQL_ERROR(sr)
  sr               = zgc_add_ooogcw_to_gc_summary(pc, rogcv, rometa, reorder);
  RETURN_SQL_ERROR(sr)
  return agent_post_reorder_delta(pc);
}

static sqlres agent_apply_reorder_delta(jv *pc) {
  jv     *jks      = &((*pc)["ks"]);
  string  kqk      = (*jks)["kqk"].asString();
  sqlres  sr       = zoor_agent_in_gc_wait(kqk);
  RETURN_SQL_ERROR(sr)
  bool    gwait    = sr.ures;
  LD("REO: (GC-WAIT) GET-KEY-GCV-NREO: K: " << kqk << " GW: " << gwait);
  if (!gwait) return normal_agent_apply_reorder_delta (pc);
  else        return gc_wait_agent_apply_reorder_delta(pc);
}

// NOTE: returns CRDT
sqlres zoor_apply_reorder_delta(jv *pc, bool is_sub) {
  LT("zoor_apply_reorder_delta");
  ztrace_start("ZOOR.agent_apply_reorder_delta");
  sqlres sr = agent_apply_reorder_delta(pc);
  ztrace_finish("ZOOR.agent_apply_reorder_delta");
  return sr;
}

// RETURNS CRDT
static sqlres internal_apply_reorder_delta(jv *jks, jv *jdentry, jv *md,
                                           bool is_sub) {
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
  string  avrsn   = (*jauthor)["agent_version"].asString();
  LD("START: internal_apply_reorder_delta: K: " << kqk << " AV: " << avrsn);
  sqlres sr       = zsd_create_sd_pc(jks, jdentry, md);
  RETURN_SQL_ERROR(sr)
  jv     pc       = sr.jres;
  sr              = zoor_apply_reorder_delta(&pc, is_sub);
  RETURN_SQL_ERROR(sr)
  LD("END: internal_apply_reorder_delta: K: " << kqk << " AV: " << avrsn);
  zh_override_pc_ncrdt_with_md_gcv(&pc); // GCV used in save_new_crdt()
  RETURN_SQL_RESULT_JSON(pc["ncrdt"])
}

static sqlres save_ooo_gcv_delta(jv *jks, jv *jdentry, bool is_sub) {
  string  kqk      = (*jks)["kqk"].asString();
  jv     *jdmeta   = &((*jdentry)["delta"]["_meta"]);
  jv     *jauthor  = &((*jdmeta)["author"]);
  UInt64  auuid    = (*jauthor)["agent_uuid"].asUInt64();
  sqlres  sr       = persist_agent_key_ooo_gcv(kqk, auuid);
  RETURN_SQL_ERROR(sr)
  jv      einfo    = JOBJECT;
  einfo["is_sub"]  = is_sub;
  einfo["kosync"]  = false;
  einfo["knreo"]   = true;
  einfo["central"] = zh_get_bool_member(jdmeta, "from_central");
  return zoor_persist_ooo_delta(jks, jdentry, &einfo);
}

// RETURNS CRDT
sqlres zoor_handle_ooo_gcv_delta(jv *jks, jv *jdentry, jv *md, bool is_sub) {
  string  kqk      = (*jks)["kqk"].asString();
  jv     *jauthor  = &((*jdentry)["delta"]["_meta"]["author"]);
  LD("ZOOR.HandleOOOGCVDelta: K: " << kqk);
  sqlres  sr       = save_ooo_gcv_delta(jks, jdentry, is_sub);
  RETURN_SQL_ERROR(sr)
  sr               = fetch_delta_need_reorder(kqk, jauthor);
  RETURN_SQL_ERROR(sr)
  jv     *roauthor = &(sr.jres);
  if (zjn(roauthor)) RETURN_EMPTY_SQLRES
  else {
    LE("ZOOR.HandleOOOGCVDelta: REFERENCE FOUND: K: " << kqk <<
       " REO-AUTHOR: " << *roauthor);
    sqlres  sr      = zsd_fetch_subscriber_delta(kqk, roauthor);
    RETURN_SQL_ERROR(sr)
    jv     *rodentry = &(sr.jres);
    return internal_apply_reorder_delta(jks, rodentry, md, is_sub);
  }
}


sqlres zoor_unset_ooo_gcv_delta(string &kqk, jv *jauthor) {
  UInt64  auuid    = (*jauthor)["agent_uuid"].asUInt64();
  string  avrsn    = (*jauthor)["agent_version"].asString();
  LD("ZOOR.UnsetOOOGCVDelta: K: " << kqk << " AV: " << avrsn);
  sqlres  sr       = remove_agent_key_ooo_gcv(kqk, auuid);
  RETURN_SQL_ERROR(sr)
  sr               = fetch_delta_need_reorder(kqk, jauthor);
  RETURN_SQL_ERROR(sr)
  jv     *roauthor = &(sr.jres);
  if (zjn(roauthor)) RETURN_EMPTY_SQLRES
  else { // REMOVE BOTH NEED_REFERENCE & NEED_REORDER
    sqlres sr = remove_delta_need_reference(kqk, roauthor);
    RETURN_SQL_ERROR(sr)
    return remove_delta_need_reorder(kqk, jauthor);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE OOO SUBSCRIBER DELTAS -----------------------------------------------

#define THRESHOLD_UNEXPECTED_DELTA_TIMEOUT 5000 /* 5 seconds */
#define UNEXPECTED_DELTA_TIMEOUT           2000 /* 2 seconds */

jv UnexpectedDeltaPerAgent = JOBJECT; // PER [KQK,AUUID}

sqlres zoor_handle_unexpected_subscriber_delta(const char *ckqk,
                                               const char *sectok,
                                               UInt64      auuid) {
  string  kqk(ckqk);
  bool    fire = zh_jv_is_member(&UnexpectedDeltaPerAgent, kqk);
  LE("zoor_handle_unexpected_subscriber_delta: (UNEXSD) K: " << kqk <<
     " AU: " << auuid << " -> FIRE: " << fire);
  if (!fire) RETURN_EMPTY_SQLRES
  else {
    jv jks   = zh_create_ks(kqk, sectok);
    jv einfo = JOBJECT; // NOTE: not used
    zsd_handle_unexpected_ooo_delta(&jks, &einfo);
    UnexpectedDeltaPerAgent.removeMember(kqk);
    RETURN_EMPTY_SQLRES
  }
}

static sqlres set_timer_unexpected_subscriber_delta(jv *jks, UInt64 auuid,
                                                    UInt64 now) {
  string kqk    = (*jks)["kqk"].asString();
  string sectok = (*jks)["security_token"].asString();
  LE("UNEXPECTED OOO-SD K: " << kqk << " -> SET-TIMER: (UNEXSD)");
  UInt64 to     = UNEXPECTED_DELTA_TIMEOUT;
  if (!zexh_set_timer_unexpected_subscriber_delta(kqk, sectok, auuid, to)) {
    RETURN_SQL_RESULT_ERROR("zexh_set_timer_unexpected_subscriber_delta");
  } else {
    if (!zh_jv_is_member(&UnexpectedDeltaPerAgent, kqk)) {
      UnexpectedDeltaPerAgent[kqk] = JOBJECT;
    }
    string sauuid = to_string(auuid);
    UnexpectedDeltaPerAgent[kqk][sauuid] = now;
    RETURN_EMPTY_SQLRES
  }
}

UInt64 get_lowest_timestamp_unexpected_subscriber_delta(string &kqk) {
  if (!zh_jv_is_member(&UnexpectedDeltaPerAgent, kqk)) return 0;
  else {
    UInt64  mts  = ULLONG_MAX;
    jv     *a_ts = &(UnexpectedDeltaPerAgent[kqk]);
    vector<string> mbrs = zh_jv_get_member_names(a_ts);
    for (uint i = 0; i < mbrs.size(); i++) {
      string auuid = mbrs[i];
      jv     jts   = (*a_ts)[auuid];
      UInt64 ts    = jts.asUInt64();
      if (ts < mts) mts = ts;
    }
    return mts;
  }
}

static sqlres action_ooo_delta(jv *jks, jv *einfo, UInt64 auuid) {
  string kqk = (*jks)["kqk"].asString();
  bool   ok  = (*einfo)["kosync"].asBool();
  if (!ok) ok = ((*einfo)["ooodep"].asBool() && !(*einfo)["oooav"].asBool());
  if (!ok) ok = ((*einfo)["knreo"].asBool()  && !(*einfo)["central"].asBool());
  LE("action_ooo_delta: K: " << kqk << " EINFO: " << *einfo << " OK: "  << ok);
  if (ok) RETURN_EMPTY_SQLRES
  else{
    UInt64 now  = zh_get_ms_time();
    UInt64 ts   = get_lowest_timestamp_unexpected_subscriber_delta(kqk);
    Int64  diff = (now - ts);
    bool   sane = (diff > 0);
    LE("action_ooo_delta: TS: " << ts << " NOW: " << now << " DIFF: " << diff);
    if (ts && sane && (diff < THRESHOLD_UNEXPECTED_DELTA_TIMEOUT)) {
      LE("UNEXPECTED OOO-SD K: " << kqk << " (TIMER-SET) -> NO-OP (UNEXSD)");
      RETURN_EMPTY_SQLRES
    } else {
      return set_timer_unexpected_subscriber_delta(jks, auuid, now);
    }
  }
}

static sqlres do_persist_ooo_delta(jv *jks, jv *jdentry, jv *einfo) {
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
  UInt64  auuid   = (*jauthor)["agent_uuid"].asUInt64();
  string  avrsn   = (*jauthor)["agent_version"].asString();
  LD("ZOOR.PersistOOODelta: K: " << kqk << " AV: " << avrsn);
  sqlres  sr      = increment_ooo_key(kqk, 1);
  RETURN_SQL_ERROR(sr)
  sr              = persist_ooo_delta(kqk, jauthor);
  RETURN_SQL_ERROR(sr)
  sr              = persist_ooo_key_delta_version(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  sr              = zsd_conditional_persist_subscriber_delta(kqk, jdentry);
  RETURN_SQL_ERROR(sr)
  return action_ooo_delta(jks, einfo, auuid);
}

sqlres zoor_persist_ooo_delta(jv *jks, jv *jdentry, jv *einfo) {
  LT("zoor_persist_ooo_delta");
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
  sqlres  sr      = fetch_ooo_delta(kqk, jauthor);
  RETURN_SQL_ERROR(sr)
  bool    oooav  = sr.ures ? true : false;
  if (!oooav) return do_persist_ooo_delta(jks, jdentry, einfo);
  else {
    string avrsn = (*jauthor)["agent_version"].asString();
    LE("ZOOR.PersistOOODelta: K: " << kqk <<
             " AV: " << avrsn << " DELTA ALREADY PERSISTED -> NO-OP");
    RETURN_EMPTY_SQLRES
  }
}

static sqlres do_remove_ooo_delta_metadata(string &kqk) {
  return remove_ooo_key(kqk);
  //NOTE: ooo_key_delta_version[] has already been fully removed
}

sqlres zoor_do_remove_ooo_delta(string &kqk, jv *jauthor) {
  LT("zoor_do_remove_ooo_delta");
  sqlres sr    = fetch_ooo_delta(kqk, jauthor);
  RETURN_SQL_ERROR(sr)
  bool   oooav = sr.ures ? true : false;
  if (!oooav) RETURN_EMPTY_SQLRES
  else {
    string avrsn  = (*jauthor)["agent_version"].asString();
    LD("ZOOR.DoRemoveOOODelta: K: " << kqk << " AV: " << avrsn);
    sr            = remove_ooo_delta(kqk, jauthor);
    RETURN_SQL_ERROR(sr)
    sr            = remove_ooo_key_delta_version(kqk, avrsn);
    RETURN_SQL_ERROR(sr)
    sr            = increment_ooo_key(kqk, -1);
    RETURN_SQL_ERROR(sr)
    UInt64 noooav = sr.ures;
    if (noooav) RETURN_EMPTY_SQLRES
    else        return do_remove_ooo_delta_metadata(kqk);
  }
}

sqlres zoor_remove_subscriber_delta(string &kqk, jv *jauthor) {
  LT("zoor_remove_subscriber_delta");
  sqlres sr = zds_remove_subscriber_delta(kqk, jauthor);
  RETURN_SQL_ERROR(sr)
  return zoor_do_remove_ooo_delta(kqk, jauthor);
};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// IRRELEVANT DELTAS (ON MERGE) ----------------------------------------------

static sqlres remove_irrelevant_deltas(jv *jks, jv *authors) {
  LT("remove_irrelevant_deltas");
  for (uint i = 0; i < authors->size(); i++) {
    jv     *jauthor = &((*authors)[i]);
    sqlres  sr      = zas_do_commit_delta(jks, jauthor);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static void add_irrelevant_agent_version(jv *authors, jv *jcavrsns,
                                         jv *javrsns) {
  LT("add_irrelevant_agent_version");
  if (zjn(javrsns)) return;
  for (uint i = 0; i < javrsns->size(); i++) {
    string         avrsn  = (*javrsns)[i].asString();
    vector<string> res    = split(avrsn, '|');
    string         sauuid = res[0];
    string         savnum = res[1];
    UInt64         avnum  = strtoul(savnum.c_str(), NULL, 10);
    UInt64         auuid  = strtoul(sauuid.c_str(), NULL, 10);
    UInt64         cavnum = 0;
    if (zh_jv_is_member(jcavrsns, sauuid)) { // NOTE: STRING (sauuid) is correct
      string cavrsn = (*jcavrsns)[sauuid].asString();
      cavnum        = zh_get_avnum(cavrsn);
    }
    if (avnum <= cavnum) {
      jv author;
      author["agent_uuid"]    = auuid;
      author["agent_version"] = avrsn;
      zh_jv_append(authors, &author);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SORT OOO DELTAS (DEPENDENCIES) --------------------------------------------

static void add_contiguous_agent_version(jv *authors, jv *jcavrsns,
                                         jv *javrsns) {
  LT("add_contiguous_agent_version");
  if (zjn(javrsns)) return;
  for (uint i = 0; i < javrsns->size(); i++) {
    string         avrsn  = (*javrsns)[i].asString();
    vector<string> res    = split(avrsn, '|');
    string         sauuid = res[0];
    string         savnum = res[1];
    UInt64         avnum  = strtoul(savnum.c_str(), NULL, 10);
    UInt64         auuid  = strtoul(sauuid.c_str(), NULL, 10);
    UInt64         cavnum = 0;
    if (zh_jv_is_member(jcavrsns, sauuid)) { // NOTE: STRING (sauuid) is correct
      string cavrsn = (*jcavrsns)[sauuid].asString();
      cavnum        = zh_get_avnum(cavrsn);
    }
    UInt64        eavnum = cavnum + 1;
    if (avnum == eavnum) {
      (*jcavrsns)[sauuid]     = avrsn; // NOTE: modifies cavrsns[]
      jv author;
      author["agent_uuid"]    = auuid;
      author["agent_version"] = avrsn;
      zh_jv_append(authors, &author);
    }
  }
}

static sqlres fetch_author_deltas(string &kqk, bool is_sub, jv *authors,
                                  jv *adentries) {
  LT("fetch_author_deltas");
  UInt64 nd = 0;
  for (uint i = 0; i < authors->size(); i++) {
    jv     *jauthor = &((*authors)[i]);
    string  sauuid  = (*jauthor)["agent_uuid"].asString();
    sqlres  sr      = zsd_fetch_subscriber_delta(kqk, jauthor);
    RETURN_SQL_ERROR(sr)
    jv     *odentry = &(sr.jres);
    if (zjn(odentry)) { //NOTE: odentry missing on RECURSIVE CORNER CASES
      LD("fetch_author_deltas: MISSING DELTA: AUTHOR: " << *jauthor);
    } else {
      if (!zh_jv_is_member(adentries, sauuid)) (*adentries)[sauuid] = JARRAY;
      zh_jv_append(&((*adentries)[sauuid]), odentry);
    }
    nd += 1;
  }
  LD("fetch_author_deltas: #D: " << nd);
  RETURN_EMPTY_SQLRES
}

static bool cmp_dependency(jv *jdentry1, jv *jdentry2) {
  //LT("cmp_dependency");
  bool    ret     = 0;
  jv     *jdmeta1 = &((*jdentry1)["delta"]["_meta"]);
  string  sauuid1 = (*jdmeta1)["author"]["agent_uuid"].asString();
  string  avrsn1  = (*jdmeta1)["author"]["agent_version"].asString();
  jv     *jdeps1  = &((*jdmeta1)["dependencies"]);
  jv     *jdmeta2 = &((*jdentry2)["delta"]["_meta"]);
  string  sauuid2 = (*jdmeta2)["author"]["agent_uuid"].asString();
  string  avrsn2  = (*jdmeta2)["author"]["agent_version"].asString();
  jv     *jdeps2  = &((*jdmeta2)["dependencies"]);
  string  rdep, ldep;
  if (zjd(jdeps2) && zh_jv_is_member(jdeps2, sauuid1)) {
    rdep = (*jdeps2)[sauuid1].asString();
  }
  if (zjd(jdeps1) && zh_jv_is_member(jdeps1, sauuid2)) {
    ldep = (*jdeps1)[sauuid2].asString();
  }
  if (!rdep.size() && !ldep.size()) {
    ret           = 0;
  } else if (rdep.size()) {
    UInt64 avnum1 = zh_get_avnum(avrsn1);
    UInt64 ravnum = zh_get_avnum(rdep);
    ret           = (avnum1 == ravnum) ? 0 : (avnum1 > ravnum);
  } else { // LDEP
    UInt64 avnum2 = zh_get_avnum(avrsn2);
    UInt64 lavnum = zh_get_avnum(ldep);
    ret           = (avnum2 == lavnum) ? 0 : (avnum2 < lavnum);
  }
  return ret;
}

static jv init_dependency_sort_authors(jv *adentries) {
  LT("init_dependency_sort_authors");
  jv fdentries = JARRAY;
  vector<string> mbrs = zh_jv_get_member_names(adentries);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  sauuid    = mbrs[i];
    jv     *jdentries = &((*adentries)[sauuid]);
    jv      dentry    = zh_jv_shift(jdentries); // TAKE FIRST FROM EACH AUUID
    zh_jv_append(&fdentries, &dentry);
  }
  return fdentries;
}

static jv get_next_agent_dentry(jv *adentries, string &sauuid) {
  if (!zh_jv_is_member(adentries, sauuid)) return JNULL;
  jv *jdentries = &((*adentries)[sauuid]);
  jv  dentry    = zh_jv_shift(jdentries);
  return dentry;
}

static void do_dependency_sort_authors(string &kqk, jv *adentries,
                                       jv *rdentries) {
  LT("do_dependency_sort_authors");
  jv fdentries = init_dependency_sort_authors(adentries);
  while (fdentries.size()) {
    zh_jv_sort(&fdentries, cmp_dependency);
    jv      rdentry = zh_jv_shift(&fdentries);
    zh_jv_append(rdentries, &rdentry);
    jv     *jmeta   = &(rdentry["delta"]["_meta"]);
    string  sauuid  = (*jmeta)["author"]["agent_uuid"].asString();
    jv      fdentry = get_next_agent_dentry(adentries, sauuid);
    if (zjd(&fdentry)) zh_jv_append(&fdentries, &fdentry);
  }
  LD("END: do_dependency_sort_authors: #RD: " << rdentries->size());
}

sqlres zoor_dependencies_sort_authors(string &kqk, bool is_sub, jv *jauthors) {
  LT("zoor_dependencies_sort_authors");
  jv     cauthors  = *jauthors; // NOTE: gets modified
  jv     adentries = JOBJECT;
  sqlres sr        = fetch_author_deltas(kqk, is_sub, &cauthors, &adentries);
  RETURN_SQL_ERROR(sr)
  jv     rdentries = JARRAY;
  do_dependency_sort_authors(kqk, &adentries, &rdentries);
  RETURN_SQL_RESULT_JSON(rdentries)
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REPLAY APPLY DELTAS -------------------------------------------------------

static sqlres replay_delta_versions(jv *pc, jv *jxcrdt,
                                    jv *jdentries, bool is_sub) {
  LD("replay_delta_versions: #D: " << jdentries->size());
  jv     *jks    = &((*pc)["ks"]);
  jv     *md     = &((*pc)["extra_data"]["md"]);
  string  kqk    = (*jks)["kqk"].asString();
  (*md)["ocrdt"] = *jxcrdt; // zsd_internal_handle_subscriber_delta on THIS CRDT
  for (uint i = 0; i < jdentries->size(); i++) {
    jv     *jdentry = &((*jdentries)[i]);
    jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
    sqlres  sr      = zsd_internal_handle_subscriber_delta(jks, jdentry, md);
    RETURN_SQL_ERROR(sr)
    bool    ok      = sr.ures ? true : false;
    jv     *jncrdt  = &(sr.jres);
    LD("ZOOR.replay_delta_versions: OK: " << ok);
    if (!ok) continue; // INTERNAL NO-OP
    else {             // SUCCESFFULY APPLIED -> NO LONGER OOO
      if (zjd(jncrdt)) zh_set_new_crdt(pc, md, jncrdt, false);
      sqlres sr = zoor_do_remove_ooo_delta(kqk, jauthor);
      RETURN_SQL_ERROR(sr)
    }
  }
  RETURN_EMPTY_SQLRES
}

static sqlres get_merge_replay_deltas(jv *jks, bool is_sub, jv *jcavrsns) {
  LT("get_merge_replay_deltas");
  string kqk        = (*jks)["kqk"].asString();
  jv     ccavrsns   = *jcavrsns; // NOTE: gets modified
  sqlres sr         = fetch_subscriber_delta_aversions(kqk);
  RETURN_SQL_ERROR(sr)
  jv     javrsns    = sr.jres;
  sr                = fetch_ooo_key_delta_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv     joooavrsns = sr.jres;
  jv     iauthors   = JARRAY;
  jv     authors    = JARRAY;
  if (joooavrsns.size()) {
    for (uint i = 0; i < joooavrsns.size(); i++) {
      zh_jv_append(&javrsns, &(joooavrsns[i])); // APPEND OOOAVRSNS[] TO AVRNS[]
    }
  }
  zh_jv_sort(&javrsns, cmp_delta_agent_version);
  add_irrelevant_agent_version(&iauthors, jcavrsns,  &javrsns);
  add_contiguous_agent_version(&authors,  &ccavrsns, &javrsns);
  sr                = remove_irrelevant_deltas(jks, &iauthors);
  RETURN_SQL_ERROR(sr)
  return zoor_dependencies_sort_authors(kqk, is_sub, &authors);
}

static void adjust_replay_deltas_to_gc_summaries(string &kqk, jv *jdentries,
                                                 UInt64 min_gcv) {
  UInt64 dmin_gcv = min_gcv ? (min_gcv - 1) : 0; // PREVIOUS GCV DELTAS ARE OK
  LD("adjust_replay_deltas_to_gc_summaries: (D)MIN-GCV: " << dmin_gcv);
  for (uint i = 0; i < jdentries->size(); i++) {
    jv     *jdentry = &((*jdentries)[i]);
    UInt64  dgcv    = zh_get_dentry_gcversion(jdentry);
    if (dgcv < dmin_gcv) {
      jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
      string  avrsn   = (*jauthor)["agent_version"].asString();
      LE("SET DO_IGNORE: K: " << kqk << " AV: " << avrsn);
      (*jdentry)["delta"]["_meta"]["DO_IGNORE"] = true;
    }
  }
}

static void adjust_gc_summaries_to_replay_deltas(jv *jgcsumms, jv *jdentries) {
  UInt64 min_gcv = ULLONG_MAX;
  for (uint i = 0; i < jdentries->size(); i++) {
    jv   *jdentry = &((*jdentries)[i]);
    jv   *jdmeta  = &((*jdentry)["delta"]["_meta"]);
    bool  ignore  = zh_get_bool_member(jdmeta, "DO_IGNORE");
    if (!ignore) {
      UInt64 dgcv = zh_get_dentry_gcversion(jdentry);
      if (dgcv < min_gcv) min_gcv = dgcv;
    }
  }
  LD("adjust_gc_summaries_to_replay_deltas: MIN-GCV: " << min_gcv);
  vector<string> mbrs = zh_jv_get_member_names(jgcsumms);
  for (uint i = 0; i < mbrs.size(); i++) {
    string sgcv = mbrs[i];
    UInt64  gcv = strtoul(sgcv.c_str(), NULL, 10);
    if (gcv <= min_gcv) { // LESS THAN OR EQUAL -> ALREADY PROCESSED MIN-GCV
      LD("adjust_gc_summaries_to_replay_deltas: DELETE: GCV: " << sgcv);
      jgcsumms->removeMember(sgcv);
    }
  }
}

// NOTE: ZSD.InternalHandleSubscriberDelta skips OOOGCV check
//       |-> AUTO-DTS can incorrectly advance ncrdt.GCV
sqlres finalize_rewound_subscriber_merge(jv *pc) {
  jv     *md  = &((*pc)["extra_data"]["md"]);
  UInt64  gcv = (*md)["gcv"].asUInt64(); // NOTE: SET MD.OGCV
  LD("ZOOR.replay_subscriber_merge_deltas: (F)GCV: " << gcv);
  (*pc)["ncrdt"]["_meta"]["GC_version"] = gcv;
  RETURN_EMPTY_SQLRES
}

sqlres zoor_replay_subscriber_merge_deltas(jv *pc, bool is_sub) {
  jv     *jks       = &((*pc)["ks"]);
  jv     *md        = &((*pc)["extra_data"]["md"]);
  jv     *jxcrdt    = &((*pc)["xcrdt"]);
  jv     *jcavrsns  = &((*pc)["extra_data"]["cavrsns"]);
  jv      jgcsumms  = (*pc)["extra_data"]["gcsumms"]; // NOTE: GETS MODIFIED
  string  kqk       = (*jks)["kqk"].asString();
  UInt64  xgcv      = (*jxcrdt)["_meta"]["GC_version"].asUInt64();
  zh_set_new_crdt(pc, md, jxcrdt, true);
  LD("zoor_replay_subscriber_merge_deltas: cavrsns: " << *jcavrsns);
  sqlres  sr        = get_merge_replay_deltas(jks, is_sub, jcavrsns);
  RETURN_SQL_ERROR(sr)
  jv      rdentries = sr.jres;
  UInt64  tot       = get_total_number_key_gcv_needs_reorder(kqk, xgcv,
                                                             &rdentries);
  LD("zoor_replay_subscriber_merge_deltas: TOTAL(NUM): " << tot);
  if (tot == 0) {                              // NO REWIND
    if (!rdentries.size()) RETURN_EMPTY_SQLRES // NO-OP
    else {                                     // REPLAY
      debug_replay_merge_dentries(kqk, &rdentries);
      return replay_delta_versions(pc, &((*pc)["ncrdt"]), &rdentries, is_sub);
    }
  } else {                                                 // REWIND AND REPLAY
    debug_replay_merge_dentries(kqk, &rdentries);
    UInt64 min_gcv = zgc_get_min_gcv(&jgcsumms);
    adjust_replay_deltas_to_gc_summaries(kqk, &rdentries, min_gcv);
    LD("ZOOR.replay_subscriber_merge_deltas: MIN-GCV: " << min_gcv);
    adjust_gc_summaries_to_replay_deltas(&jgcsumms, &rdentries);
    jv     jncrdt  = zmerge_rewind_crdt_gc_version(kqk, jxcrdt, &jgcsumms);
    zh_set_new_crdt(pc, md, &jncrdt, true);
    (*md)["ogcv"]  = (*md)["gcv"]; // NOTE: SET MD.OGCV
    LD("ZOOR.replay_subscriber_merge_deltas: (N)GCV: " << (*md)["gcv"]);
    sqlres sr      = set_key_gcv_needs_reorder(kqk, xgcv, &rdentries);
    RETURN_SQL_ERROR(sr)
    sr             = replay_delta_versions(pc, &((*pc)["ncrdt"]),
                                           &rdentries, is_sub);
    RETURN_SQL_ERROR(sr)
    return finalize_rewound_subscriber_merge(pc);
  }
}

sqlres zoor_prepare_local_deltas_post_subscriber_merge(jv *pc) {
  bool    is_sub    = true;
  jv     *jks       = &((*pc)["ks"]);
  jv     *jxcrdt    = &((*pc)["xcrdt"]);
  jv     *jcavrsns  = &((*pc)["extra_data"]["cavrsns"]);
  string  kqk       = (*jks)["kqk"].asString();
  UInt64  xgcv      = (*jxcrdt)["_meta"]["GC_version"].asUInt64();
  LD("zoor_prepare_local_deltas_post_subscriber_merge: cavrsns: " << *jcavrsns);
  sqlres  sr        = get_merge_replay_deltas(jks, is_sub, jcavrsns);
  RETURN_SQL_ERROR(sr)
  jv      rdentries = sr.jres;
  return set_key_gcv_needs_reorder(kqk, xgcv, &rdentries);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE OOO SUBSCRIBER DELTAS ----------------------------------------------

static sqlres do_replay_ooo_deltas(jv *pc, bool is_sub, jv *rdentries) {
  jv     *jks       = &((*pc)["ks"]);
  jv     *md        = &((*pc)["extra_data"]["md"]);
  string  kqk       = (*jks)["kqk"].asString();
  jv     *jocrdt    = &((*md)["ocrdt"]);
  LE("ZOOR.do_replay_ooo_deltas: K: " << kqk << " rdentries[AV]: [" <<
       debug_dentries_agent_versions(rdentries) << "]");
  return replay_delta_versions(pc, jocrdt, rdentries, is_sub);
}

static sqlres get_replay_ooo_deltas(string &kqk, bool is_sub) {
  LT("get_replay_ooo_deltas");
  sqlres  sr         = zdelt_get_key_subscriber_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv      mavrsns    = sr.jres;
  sr                 = fetch_ooo_key_delta_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv      joooavrsns = sr.jres;
  zh_jv_sort(&joooavrsns, cmp_delta_agent_version);
  jv      authors    = JARRAY;
  add_contiguous_agent_version(&authors, &mavrsns, &joooavrsns);
  LD("get_replay_ooo_deltas: authors: " << authors);
  return zoor_dependencies_sort_authors(kqk, is_sub, &authors);
}

static sqlres replay_ooo_deltas(jv *pc, bool is_sub) {
  LT("replay_ooo_deltas");
  jv     *jks       = &((*pc)["ks"]);
  string  kqk       = (*jks)["kqk"].asString();
  sqlres  sr        = get_replay_ooo_deltas(kqk, is_sub);
  RETURN_SQL_ERROR(sr)
  jv     *rdentries = &(sr.jres);
  return do_replay_ooo_deltas(pc, is_sub, rdentries);
}

static sqlres check_finish_unexpected_delta(string &kqk, UInt64 auuid,
                                            bool is_sub) {
  string sauuid = to_string(auuid);
  bool   afire  = zh_jv_is_member(&UnexpectedDeltaPerAgent, kqk) &&
                  zh_jv_is_member(&(UnexpectedDeltaPerAgent[kqk]), sauuid);
  if (!afire) RETURN_EMPTY_SQLRES
  else {
    sqlres  sr        = get_replay_ooo_deltas(kqk, is_sub);
    RETURN_SQL_ERROR(sr)
    jv     *rdentries = &(sr.jres);
    LD("check_finish_unexpected_delta: (UNEXSD)");
    for (uint i = 0; i < rdentries->size(); i++) {
      jv     *jdentry = &((*rdentries)[i]);
      jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
      UInt64  dauuid  = (*jauthor)["agent_uuid"].asUInt64();
      if (auuid == dauuid) RETURN_EMPTY_SQLRES
    }
    UnexpectedDeltaPerAgent[kqk].removeMember(sauuid);
    UInt64 n = UnexpectedDeltaPerAgent[kqk].size();
    LD("NUM UNEXSD PER KQK: " << n);
    if (n == 0) {
      LE("FINISH-UNEXSD: K: " << kqk << " U: " << auuid);
      UnexpectedDeltaPerAgent.removeMember(kqk);
    }
    RETURN_EMPTY_SQLRES
  }
}

static sqlres conditional_replay_ooo_deltas(jv *pc, UInt64 noooav,
                                            bool is_sub) {
  if (!noooav) RETURN_EMPTY_SQLRES
  else {
    jv     *md = &((*pc)["extra_data"]["md"]);
    if (zh_jv_is_member(pc, "ncrdt") && zjd(&((*pc)["ncrdt"]))) {
      (*md)["ocrdt"] = (*pc)["ncrdt"]; // replay_ooo_deltas() on THIS CRDT
    }
    sqlres  sr = replay_ooo_deltas(pc, is_sub);
    RETURN_SQL_ERROR(sr)
    RETURN_EMPTY_SQLRES
  }
}

sqlres zoor_check_replay_ooo_deltas(jv *pc, bool do_check, bool is_sub) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jdentry = &((*pc)["dentry"]);
  string  kqk     = (*jks)["kqk"].asString();
  sqlres  sr      = fetch_ooo_key(kqk);
  RETURN_SQL_ERROR(sr)
  UInt64  noooav  = sr.ures;
  LD("zoor_check_replay_ooo_deltas: noooav: " << noooav);
  sr              = conditional_replay_ooo_deltas(pc, noooav, is_sub);
  RETURN_SQL_ERROR(sr)
  if (!do_check) RETURN_EMPTY_SQLRES
  else {
    jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
    UInt64  auuid   = (*jauthor)["agent_uuid"].asUInt64();
    return check_finish_unexpected_delta(kqk, auuid, is_sub);
  }
}

