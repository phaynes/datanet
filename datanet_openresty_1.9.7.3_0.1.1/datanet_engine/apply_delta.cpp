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
#include "shared.h"
#include "merge.h"
#include "xact.h"
#include "convert.h"
#include "deltas.h"
#include "cache.h"
#include "subscriber_delta.h"
#include "subscriber_merge.h"
#include "apply_delta.h"
#include "dack.h"
#include "activesync.h"
#include "creap.h"
#include "agent_daemon.h"
#include "gc.h"
#include "latency.h"
#include "datastore.h"
#include "trace.h"
#include "storage.h"

using namespace std;

extern UInt64 AgentLastReconnect;

extern jv zh_nobody_auth;

extern int ChaosMode;
extern vector<string> ChaosDescription;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FORWARD DECLARATIONS ------------------------------------------------------

static sqlres do_apply_delta(jv *pc, bool is_agent, bool is_reo);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static jv create_crdt_from_initial_delta(string &kqk, jv *jdentry) {
  jv  jjson; // Empty JSON
  jv *jdelta  = &((*jdentry)["delta"]);
  jv *jdmeta  = &((*jdelta)["_meta"]);
  jv  jcrdtd  = zconv_convert_json_object_to_crdt(&jjson, jdmeta, false);
  jv  jcrdt   = zh_format_crdt_for_storage(kqk, jdmeta, &jcrdtd);
  jv  jmerge  = zmerge_apply_deltas(&jcrdt, jdelta, false);
  jv *jncrdtd = &(jmerge["crdtd"]);
  jv  jncrdt  = zh_format_crdt_for_storage(kqk, jdmeta, jncrdtd);
  return jncrdt;
}

void zad_assign_post_merge_deltas(jv *pc, jv *jmerge) {
  if (jmerge) (*pc)["post_merge_deltas"] = (*jmerge)["post_merge_deltas"];
  else        (*pc)["post_merge_deltas"] = JARRAY;
  if (zh_jv_is_member(pc, "metadata")) {
    if (zh_jv_is_member(&((*pc)["metadata"]), "added_channels")) {
      jv entry;
      entry["added_channels"] = (*pc)["metadata"]["added_channels"];
      zh_jv_append(&((*pc)["post_merge_deltas"]), &entry);
    }
    if (zh_jv_is_member(&((*pc)["metadata"]), "removed_channels")) {
      jv entry;
      entry["removed_channels"] = (*pc)["metadata"]["removed_channels"];
      zh_jv_append(&((*pc)["post_merge_deltas"]), &entry);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE CRDT/GCV ------------------------------------------------------------

static sqlres set_agent_max_gc_version(string &kqk, UInt64 gcv) {
  LD("SET MAX-GCV: K: " << kqk << " MAX-GCV: " << gcv);
  return persist_agent_key_max_gc_version(kqk, gcv);
}

static sqlres set_agent_and_crdt_gc_version(string &kqk, jv *jncrdt,
                                            UInt64 gcv, UInt64 xgcv) {
  LD("set_agent_and_crdt_gc_version: K: " << kqk << " SET-GCV: " << gcv);
  sqlres sr = persist_gc_version(kqk, gcv);
  RETURN_SQL_ERROR(sr)
  if (xgcv) {          // STORE XGCV (MAX-GCV) for FETCH()
    (*jncrdt)["_meta"]["GC_version"] = xgcv;
    return set_agent_max_gc_version(kqk, xgcv);
  } else {
    sqlres sr   = fetch_agent_key_max_gc_version(kqk);
    RETURN_SQL_ERROR(sr);
    UInt64 mgcv = sr.ures ? sr.ures : 0;
    if (gcv <= mgcv) { // STORE MAX-GCV for FETCH()
      (*jncrdt)["_meta"]["GC_version"] = mgcv;
      RETURN_EMPTY_SQLRES
    } else {           // STORE (NEW)MAX-GCV for FETCH()
      (*jncrdt)["_meta"]["GC_version"] = gcv;
      return set_agent_max_gc_version(kqk, gcv);
    }
  }
}

sqlres zad_store_crdt_and_gc_version(string &kqk, jv *jncrdt,
                                     UInt64 gcv, UInt64 xgcv) {
  LD("ZAD.StoreCrdtAndGCVersion: K: " << kqk << " SET-GCV: " << gcv);
  sqlres sr = set_agent_and_crdt_gc_version(kqk, jncrdt, gcv, xgcv);
  RETURN_SQL_ERROR(sr)
  return zds_store_crdt(kqk, jncrdt);
}

static sqlres conditional_remove_key(string &kqk, bool oexists) {
  if (!oexists) RETURN_EMPTY_SQLRES
  else          return zds_remove_key(kqk);
}

sqlres zad_remove_crdt_store_gc_version(string &kqk, jv *jcrdt, UInt64 gcv, 
                                        bool oexists) {
  LD("ZAD.RemoveCrdtStoreGCVersion: K: " << kqk);
  jv     *jcmeta  = &((*jcrdt)["_meta"]);
  sqlres  sr      = persist_last_remove_delta(kqk, jcmeta);
  RETURN_SQL_ERROR(sr)
  sr              = conditional_remove_key(kqk, oexists);
  RETURN_SQL_ERROR(sr)
  jv      jncrdt  = JOBJECT; // NOTE: DUMMY CRDT
  UInt64  xgcv    = 0;       // NOTE: DUMMY XGCV
  return set_agent_and_crdt_gc_version(kqk, &jncrdt, gcv, xgcv);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// APPLY SUBSCRIBER-DELTA ----------------------------------------------------

// NOTE: returns CRDT
static sqlres store_remove_delta_metadata(jv *pc, string &kqk) {
  jv     *jdentry = &((*pc)["dentry"]);
  UInt64  gcv     = (*pc)["extra_data"]["gcv"].asUInt64();
  jv     *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  LD("store_remove_delta_metadata: K: " << kqk << " SET-GCV: " << gcv);
  sqlres  sr      =  persist_last_remove_delta(kqk, jdmeta);
  RETURN_SQL_ERROR(sr)
  jv      jncrdt  = JOBJECT; // NOTE: DUMMY CRDT
  UInt64  xgcv    = 0;       // NOTE: DUMMY XGCV
  sr              = set_agent_and_crdt_gc_version(kqk, &jncrdt, gcv, xgcv);
  RETURN_SQL_ERROR(sr)
  RETURN_EMPTY_SQLRES // NOTE: CRDT is NULL (REMOVE)
}

// NOTE: returns CRDT
static sqlres do_apply_remove_delta(jv *pc, string &kqk) {
  LT("do_apply_remove_delta");
  jv      jempty; // NOTE EMPTY
  jv     *md = &((*pc)["extra_data"]["md"]);
  zh_set_new_crdt(pc, md, &jempty, false);
  sqlres  sr = zds_remove_key(kqk);
  RETURN_SQL_ERROR(sr)
  return store_remove_delta_metadata(pc, kqk);
}

// NOTE: returns CRDT
static sqlres do_apply_expire_delta(jv *pc, string &kqk) {
  LT("do_apply_expire_delta");
  jv     *jdentry               = &((*pc)["dentry"]);
  jv     *md                    = &((*pc)["extra_data"]["md"]);
  jv     *jdmeta                = &((*jdentry)["delta"]["_meta"]);
  UInt64  expiration            = (*jdmeta)["expiration"].asUInt64();
  jv      jncrdt                = (*md)["ocrdt"];
  jncrdt["_meta"]["author"]     = (*jdmeta)["author"];
  jncrdt["_meta"]["expiration"] = expiration;
  RETURN_SQL_RESULT_JSON(jncrdt)
}

// NOTE: returns CRDT
static sqlres do_store_initial_crdt(string &kqk, jv *jdentry, jv *pc) {
  LD("apply_delta: STORE CRDT INITIAL DELTA: K: " << kqk);
  sqlres sr      = remove_last_remove_delta(kqk);
  RETURN_SQL_ERROR(sr)
  (*pc)["store"] = true;
  jv     jncrdt  = create_crdt_from_initial_delta(kqk, jdentry);
  RETURN_SQL_RESULT_JSON(jncrdt)
}

// NOTE: returns CRDT
static sqlres store_initial_crdt(string &kqk, jv *jdentry, jv *pc) {
  sqlres sr     = fetch_last_remove_delta(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *rmeta = &(sr.jres);
  if (zjn(rmeta)) { // NO LDR
    return do_store_initial_crdt(kqk, jdentry, pc);
  } else {         // LDR EXISTS -> LWW on DOC-CREATION[@]
    jv     *jdmeta   = &((*jdentry)["delta"]["_meta"]);
    UInt64  rcreated = (*rmeta)["document_creation"]["@"].asUInt64();
    UInt64  dcreated = (*jdmeta)["document_creation"]["@"].asUInt64();
    LD("store_initial: rcreated: " << rcreated << " dcreated: " << dcreated);
    if        (rcreated > dcreated) {      // LWW: LDR WINS
      RETURN_SQL_RESULT_ERROR(ZS_Errors["DeltaNotSync"])
    } else if (rcreated < dcreated) {      // LWW: DELTA WINS
      return do_store_initial_crdt(kqk, jdentry, pc);
    } else { /* (ocreated === dcreated) */ // LWW: TIEBRAKER
      UInt64  r_uuid   = (*rmeta)["document_creation"]["_"].asUInt64();
      UInt64  d_uuid   = (*jdmeta)["document_creation"]["_"].asUInt64();
      LD("store_initial: r_uuid: " << r_uuid << " d_uuid: " << d_uuid);
      if (r_uuid > d_uuid) {
        RETURN_SQL_RESULT_ERROR(ZS_Errors["DeltaNotSync"])
      } else {
        return do_store_initial_crdt(kqk, jdentry, pc);
      }
    }
  }
}

// NOTE: returns CRDT
static sqlres store_nondelta_crdt(string &kqk, jv *jdentry, jv *pc) {
  jv   *jdmeta = &((*jdentry)["delta"]["_meta"]);
  bool  remove  = zh_get_bool_member(jdmeta, "remove");
  // NOTE: EITHER STORE OR REMOVE
  if (remove) return do_apply_remove_delta(pc, kqk);
  else        return store_initial_crdt(kqk, jdentry, pc);
}

// NOTE: returns CRDT
static sqlres do_apply_GC_delta(jv *pc, string &kqk, jv *jdentry) {
  ztrace_start("ZAD.do_apply_GC_delta");
  jv     *jdmeta = &((*jdentry)["delta"]["_meta"]);
  string  avrsn  = (*jdmeta)["author"]["agent_version"].asString();
  LD("do_apply_GC_delta: K: " << kqk << " AV: " << avrsn);
  (*pc)["DO_GC"] = true;
  sqlres  sr     =  zgc_garbage_collect(pc, jdentry);
  ztrace_finish("ZAD.do_apply_GC_delta");
  return sr;
}

// NOTE: returns CRDT
static sqlres do_apply_REORDER_delta(jv *pc, string &kqk, jv *jdentry) {
  ztrace_start("ZAD.do_apply_REORDER_delta");
  jv     *jdmeta      = &((*jdentry)["delta"]["_meta"]);
  string  avrsn       = (*jdmeta)["author"]["agent_version"].asString();
  LD("do_apply_REORDER_delta: K: " << kqk << " AV: " << avrsn);
  (*pc)["DO_REORDER"] = true;
  (*jdentry)["delta"]["_meta"].removeMember("REORDERED"); // STOP RECURSION
  sqlres  sr          = do_apply_delta(pc, false, true);
  RETURN_SQL_ERROR(sr)
  jv      acrdt       = sr.jres;
  sr                  = zgc_reorder(kqk, &acrdt, jdentry);
  ztrace_finish("ZAD.do_apply_REORDER_delta");
  return sr;
}

static jv no_op_apply_delta(jv *pc, jv *jdentry) {
  jv *md                    = &((*pc)["extra_data"]["md"]);
  jv  jncrdt                = (*md)["ocrdt"];
  // ADD DELTA's AUTHOR (CRDTs have no AUTHORs)
  jncrdt["_meta"]["author"] = (*jdentry)["delta"]["_meta"]["author"];
  return jncrdt;
}

// NOTE: returns CRDT
static sqlres do_apply_REAP_delta(jv *pc, string &kqk, jv *jdentry) {
  LD("do_apply_REAP_delta: K: " << kqk << " AGENT -> NO-OP");
  jv jncrdt = no_op_apply_delta(pc, jdentry);
  RETURN_SQL_RESULT_JSON(jncrdt)
}

// NOTE: returns CRDT
static sqlres do_apply_IGNORE_delta(jv *pc, string &kqk, jv *jdentry) {
  LD("do_apply_IGNORE_delta: K: " << kqk << " -> NO-OP");
  jv jncrdt = no_op_apply_delta(pc, jdentry);
  RETURN_SQL_RESULT_JSON(jncrdt)
}

// NOTE: returns CRDT
static sqlres do_apply_delta(jv *pc, bool is_agent, bool is_reo) {
  jv     *jks       = &((*pc)["ks"]);
  jv     *jdentry   = &((*pc)["dentry"]);
  jv     *md        = &((*pc)["extra_data"]["md"]);
  jv     *jocrdt    = &((*md)["ocrdt"]);
  string  kqk       = (*jks)["kqk"].asString();
  jv     *jdmeta    = &((*jdentry)["delta"]["_meta"]);
  bool    initial   = zh_jv_is_member(jdmeta, "initial_delta");
  bool    remove    = zh_jv_is_member(jdmeta, "remove");
  bool    expire    = zh_jv_is_member(jdmeta, "expire");
  bool    do_gc     = zh_jv_is_member(jdmeta, "DO_GC");
  bool    reod      = zh_jv_is_member(jdmeta, "REORDERED");
  bool    do_ignore = zh_jv_is_member(jdmeta, "DO_IGNORE");
  bool    do_reap   = zh_jv_is_member(jdmeta, "DO_REAP");
  LD("do_apply_delta: do_gc: " << do_gc << " reod: " << reod <<
     " do_ignore: " << do_ignore << " do_reap: " << do_reap);
  bool    do_other  = do_gc || reod || do_reap || do_ignore;
  if (zjn(jocrdt)) { // TOP-LEVEL: Create new CRDT (from Delta)
    if (do_other) { // REMOVE-KEY outraced a DO_* DELTA -> NO-OP
      RETURN_EMPTY_SQLRES
    } else if (!initial && !remove) {
      // NOT-SYNC (no OCRDT & DELTA not initial/remove)
      LE("do_apply_delta: DELTA_NOT_SYNC: NO CRDT &" << " DELTA NOT INITIAL");
      RETURN_SQL_RESULT_ERROR(ZS_Errors["DeltaNotSync"])
    } else {
      return store_nondelta_crdt(kqk, jdentry, pc);
    }
  } else {       // TOP-LEVEL: Update existing CRDT
    jv   *jometa     = &((*jocrdt)["_meta"]);
    bool  a_mismatch = zmerge_compare_zdoc_authors(jometa, jdmeta);
    if (a_mismatch) { // DOC-CREATION VERSION MISmatch
      LD("DeltaAuthorVersion & CrdtAuthVersion MISMATCH");
      if (!initial && !remove) {
        LE("do_apply_delta: DELTA_NOT_SYNC: Delta NOT INITIAL NOR REMOVE");
        RETURN_SQL_RESULT_ERROR(ZS_Errors["DeltaNotSync"])
      } else { // LWW: OCRDT[@] vs DELTA[@]
        UInt64 ocreated = (*jometa)["document_creation"]["@"].asUInt64();
        UInt64 dcreated = (*jdmeta)["document_creation"]["@"].asUInt64();
        if        (ocreated > dcreated) {      // LWW: OCRDT WINS
          LD("do_apply_delta: DELTA_NOT_SYNC: OCRDT NEWER");
          RETURN_SQL_RESULT_ERROR(ZS_Errors["DeltaNotSync"])
        } else if (ocreated < dcreated) {      // LWW: DELTA WINS
          return store_nondelta_crdt(kqk, jdentry, pc);
        } else { /* (ocreated === dcreated) */ // LWW: TIEBRAKER
          UInt64 d_uuid = (*jdmeta)["document_creation"]["_"].asUInt64();
          UInt64 o_uuid = (*jometa)["document_creation"]["_"].asUInt64();
          LD("o_uuid: " << o_uuid << " d_uuid: " << d_uuid);
          if (o_uuid > d_uuid) {
            LD("do_apply_delta: DELTA_NOT_SYNC: Same[@]: OCRDT[_] HIGHER");
            RETURN_SQL_RESULT_ERROR(ZS_Errors["DeltaNotSync"])
          } else {
            return store_nondelta_crdt(kqk, jdentry, pc);
          }
        }
      }
    } else {          // DOC-CREATION VERSION MATCH
      if (do_ignore) {
        return do_apply_IGNORE_delta(pc, kqk, jdentry);
      } else if (do_gc) {
        return do_apply_GC_delta(pc, kqk, jdentry);
      } else if (reod) {
        return do_apply_REORDER_delta(pc, kqk, jdentry);
      } else if (do_reap) {
        return do_apply_REAP_delta(pc, kqk, jdentry);
      } else {
        if (remove) {        // REMOVE KEY
          (*pc)["remove"] = true;
          return do_apply_remove_delta(pc, kqk);
        } else if (expire) { // EXPIRE LKEY
          (*pc)["expire"] = true;
          return do_apply_expire_delta(pc, kqk);
        } else {
          jv *jdelta = &((*jdentry)["delta"]);
          ztrace_start("ZAD.zmerge_apply_deltas");
          jv  jmerge = zmerge_apply_deltas(jocrdt, jdelta, is_reo);
          ztrace_finish("ZAD.zmerge_apply_deltas");
          // NOTE: Only AgentDelta REJECTS on errors, CD & SD process non-errors
          if (is_agent && jmerge["errors"].size() != 0) {
            string errs = zh_join(&(jmerge["errors"]), ", ");
            RETURN_SQL_RESULT_ERROR(errs);
          } else {                                         // NORMAL APPLY DELTA
            zad_assign_post_merge_deltas(pc, &jmerge);
            jv     *jncrdtd = &(jmerge["crdtd"]);
            jv      jncrdt  = zh_format_crdt_for_storage(kqk, jdmeta, jncrdtd);
            RETURN_SQL_RESULT_JSON(jncrdt)
            // NOTE: Agent does NOT do ZGC.CheckGarbageCollect()
          }
        }
      }
    }
  }
}

static sqlres apply_delta(jv *pc, bool is_agent, bool is_reo) {
  jv     *md     = &((*pc)["extra_data"]["md"]);
  sqlres  sr     = do_apply_delta(pc, is_agent, is_reo);
  RETURN_SQL_ERROR(sr)
  jv     *jncrdt = &(sr.jres);
  if (zjd(jncrdt)) {
    zh_set_new_crdt(pc, md, jncrdt, false);
    UInt64 nbytes = zconv_calculate_crdt_bytes(&((*pc)["ncrdt"]));
    (*pc)["ncrdt"]["_meta"]["num_bytes"] = nbytes;
  }
  LD("apply_delta: SET PC.NCRDT: " << zjd(jncrdt));
  RETURN_EMPTY_SQLRES
}

sqlres zad_apply_agent_delta(jv *pc) {
  sqlres  sr     = apply_delta(pc, true, false);
  RETURN_SQL_ERROR(sr)
  UInt64  nbytes = 0;
  if (zh_jv_is_member(pc, "ncrdt")) {
    nbytes = (*pc)["ncrdt"]["_meta"]["num_bytes"].asUInt64();
  } else {
    nbytes = 0;
  }
  (*pc)["dentry"]["delta"]["_meta"]["num_bytes"] = nbytes;
  RETURN_EMPTY_SQLRES
}

sqlres zad_apply_subscriber_delta(jv *pc) {
  ztrace_start("ZAD.apply_subscriber_delta");
  sqlres sr = apply_delta(pc, false, false);
  ztrace_finish("ZAD.apply_subscriber_delta");
  return sr;
}

