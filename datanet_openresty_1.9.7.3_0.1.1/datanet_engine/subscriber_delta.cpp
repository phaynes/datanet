#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>

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
#include "external_hooks.h"
#include "shared.h"
#include "merge.h"
#include "xact.h"
#include "convert.h"
#include "deltas.h"
#include "cache.h"
#include "subscriber_delta.h"
#include "subscriber_merge.h"
#include "ooo_replay.h"
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

extern Int64  MyUUID;
extern UInt64 AgentLastReconnect;

extern jv zh_nobody_auth;

extern int ChaosMode;
extern vector<string> ChaosDescription;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FORWARD DECLARATIONS ------------------------------------------------------

static sqlres post_apply_subscriber_delta(jv *pc, bool replay);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static sqlres save_new_crdt(string desc, string &kqk, jv *jncrdt) {
  if (zjn(jncrdt)) {
    LD(desc << ": NOT STORING CRDT: K: " << kqk);
    RETURN_EMPTY_SQLRES
  } else {
    LD(desc << " STORE CRDT: K: " << kqk);
    UInt64 gcv = (*jncrdt)["_meta"]["GC_version"].asUInt64();
    return zad_store_crdt_and_gc_version(kqk, jncrdt, gcv, 0);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER DELTA METADATA -------------------------------------------------

static string get_subscriber_delta_ID(jv *pc) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jdentry = &((*pc)["dentry"]);
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  UInt64  auuid   = (*jdmeta)["author"]["agent_uuid"].asUInt64();
  string  avrsn   = (*jdmeta)["author"]["agent_version"].asString();
  return kqk + "-" + to_string(auuid) + "-" + avrsn;
}

sqlres zsd_fetch_subscriber_delta(string &kqk, jv *jauthor) {
  string avrsn = (*jauthor)["agent_version"].asString();
  LD("ZSD.FetchSubscriberDelta: K: " << kqk << " AV: " << avrsn);
  sqlres sd; FETCH_SUBSCRIBER_DELTA(kqk, avrsn);
  RETURN_SQL_ERROR(sd)
  return sd;
}

sqlres zsd_get_key_crdt_gcv_metadata(jv *md, string &kqk) {
  sqlres sr      = fetch_gc_version(kqk);
  RETURN_SQL_ERROR(sr)
  (*md)["gcv"]   = sr.ures;
  (*md)["ogcv"]  = sr.ures; // NOTE: Used in ZOOR.post_check_agent_gc_wait()
  sqlres sc;
  FETCH_CRDT(kqk);
  RETURN_SQL_ERROR(sc)
  (*md)["ocrdt"] = sc.jres;
  RETURN_EMPTY_SQLRES
}

static sqlres get_subscriber_delta_key_info(jv *md, string &kqk) {
  sqlres sr         = fetch_key_info(kqk, "SEPARATE");
  RETURN_SQL_ERROR(sr)
  (*md)["separate"] = sr.ures ? true : false;
  sr                = fetch_key_info(kqk, "PRESENT");
  RETURN_SQL_ERROR(sr)
  (*md)["present"]  = sr.ures ? true : false;
  RETURN_EMPTY_SQLRES
}

sqlres zsd_get_agent_sync_status(string &kqk, jv *md) {
  sqlres sr                = fetch_to_sync_key(kqk);
  RETURN_SQL_ERROR(sr)
  (*md)["to_sync_key"]     = sr.sres.size() ? true : false;
  sr                       = fetch_out_of_sync_key(kqk);
  RETURN_SQL_ERROR(sr)
  (*md)["out_of_sync_key"] = sr.ures ? true : false;
  return sr;
}

sqlres zsd_fetch_local_key_metadata(string &kqk, jv *md) {
  sqlres sr           = zsd_get_key_crdt_gcv_metadata(md, kqk);
  RETURN_SQL_ERROR(sr)
  sr                  = fetch_agent_watch_keys(kqk);
  RETURN_SQL_ERROR(sr)
  (*md)["watch"]      = sr.ures ? true : false;
  sr                  = get_subscriber_delta_key_info(md, kqk);
  RETURN_SQL_ERROR(sr)
  sr                  = fetch_cached_keys(kqk);
  RETURN_SQL_ERROR(sr)
  bool   cached       = sr.ures ? true : false;
  (*md)["need_evict"] = cached && 
                        (zjn(&(*md)["ocrdt"]) && (*md)["present"].asBool());
  return zsd_get_agent_sync_status(kqk, md);
}

static jv fetch_subscriber_delta_metadata(string &kqk, jv *jdentry) {
  jv      md;
  sqlres  sr     = zsd_fetch_local_key_metadata(kqk, &md);
  RETURN_SQL_ERROR_AS_JNULL(sr)
  jv     *jdmeta = &((*jdentry)["delta"]["_meta"]);
  UInt64  auuid  = (*jdmeta)["author"]["agent_uuid"].asUInt64();
  sr             = fetch_agent_key_ooo_gcv(kqk, auuid);
  RETURN_SQL_ERROR_AS_JNULL(sr)
  md["knreo"]    = sr.ures ? true : false;
  return md;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BROADCAST SUBSCRIBER DELTA TO WORKER CACHES -------------------------------

static void broadcast_subscriber_delta_to_worker_caches(string &kqk, jv *jcrdt,
                                                        jv *jdentry) {
  LT("broadcast_subscriber_delta_to_worker_caches");
  pid_t xpid     = getpid(); // do NOT send to this PID -> KQK-WORKER
  jv    prequest = zh_create_json_rpc_body("DocumentCacheDelta",
                                            &zh_nobody_auth);
  prequest["params"]["data"]["kqk"]      = kqk;
  prequest["params"]["data"]["is_merge"] = false;
  prequest["params"]["data"]["crdt"]     = *jcrdt;
  prequest["params"]["data"]["dentry"]   = *jdentry;
  if (!zexh_broadcast_delta_to_worker_caches(prequest, kqk, xpid)) {
    LD("ERROR: broadcast_subscriber_delta_to_worker_caches");
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// POST APPLY SUBSCRIBER DELTA CLEANUP ---------------------------------------

static sqlres post_apply_cleanup(jv *jks, jv *jdentry, jv *jncrdt, jv *md) {
  string  kqk        = (*jks)["kqk"].asString();
  jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
  sqlres  sr         = zoor_do_remove_ooo_delta(kqk, jauthor);
  RETURN_SQL_ERROR(sr)
  sr                 = zlat_add_to_subscriber_latencies(kqk, jdentry);
  RETURN_SQL_ERROR(sr)
  bool    need_evict = (*md)["need_evict"].asBool();
  if (need_evict) {
    LE("post_apply_cleanup: EVICT: K: " << kqk);
    bool send_central = true;
    bool need_merge   = false;
    zcache_internal_evict(jks, send_central, need_merge);
  } else { // NOTE: C++-AGENT FUNCTIONALITY (NOT IN JS-AGENT)
    broadcast_subscriber_delta_to_worker_caches(kqk, jncrdt, jdentry);
  }
  RETURN_EMPTY_SQLRES
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FAIL APPLY SUBSCRIBER DELTA -----------------------------------------------

static sqlres fail_apply_subscriber_delta_max_size(jv *jks, jv *jdentry) {
  string  kqk     = (*jks)["kqk"].asString();
  string  sectok  = (*jks)["security_token"].asString();
  jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
  string  avrsn   = (*jauthor)["agent_version"].asString();
  LE("fail_apply_subscriber_delta_max_size: K: " << kqk << " AV: " << avrsn);
  sqlres  sr      = zas_set_agent_sync_key(kqk, sectok);
  RETURN_SQL_ERROR(sr)
  sr              = remove_persisted_subscriber_delta(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  sr              = remove_subscriber_delta_version(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  RETURN_SQL_RESULT_ERROR(ZS_Errors["DeltaStorageMaxHit"])
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// APPLY SUBSCRIBER DELTA ----------------------------------------------------

static sqlres finalize_apply_subscriber_delta(jv *pc) {
  jv     *jks       = &((*pc)["ks"]);
  jv     *jdentry   = &((*pc)["dentry"]);
  string  kqk       = (*jks)["kqk"].asString();
  jv     *jcreateds = &((*jdentry)["delta"]["_meta"]["created"]);
  sqlres  sr        = zas_post_apply_delta_conditional_commit(jks, jdentry);
  RETURN_SQL_ERROR(sr)
  sr                = zdack_set_agent_createds(jcreateds);
  RETURN_SQL_ERROR(sr)
  zdelt_on_data_change(pc, false, false);
  RETURN_EMPTY_SQLRES
}

static sqlres conditional_apply_subscriber_delta(jv *pc) {
  bool    watch   = (*pc)["extra_data"]["watch"].asBool();
  jv     *jks     = &((*pc)["ks"]);
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jdentry = &((*pc)["dentry"]);
  jv     *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  bool    is_reo  = zh_jv_is_member(jdmeta, "reference_uuid");
  if (watch) {
    LD("ConditionalSubscriberDelta K: " << kqk << " -> WATCH");
    jv jmerge;
    jmerge["post_merge_deltas"] = (*jdentry)["post_merge_deltas"];
    zad_assign_post_merge_deltas(pc, &jmerge);
    RETURN_EMPTY_SQLRES
  } else if (is_reo) {
    LD("ConditionalSubscriberDelta K: " << kqk << " -> DO_REORDER");
    return zoor_apply_reorder_delta(pc, true);
  } else {
    LD("ConditionalSubscriberDelta K: " << kqk << " -> NORMAL");
    return zad_apply_subscriber_delta(pc);
  }
}

sqlres zsd_set_subscriber_agent_version(string &kqk, UInt64 auuid,
                                        string &avrsn) {
  LD("zsd_set_subscriber_agent_version: K: " << kqk << " U: " << auuid <<
     " AV: " << avrsn);
  sqlres sr = persist_device_subscriber_version(kqk, auuid, avrsn);
  RETURN_SQL_ERROR(sr)
  return persist_per_key_subscriber_version(kqk, auuid, avrsn);
}

static sqlres set_subscriber_agent_version(jv *pc) {
  jv     *jks     = &((*pc)["ks"]);
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jdentry = &((*pc)["dentry"]);
  jv     *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  UInt64  auuid   = (*jdmeta)["author"]["agent_uuid"].asUInt64();
  string  avrsn   = (*jdmeta)["author"]["agent_version"].asString();
  return zsd_set_subscriber_agent_version(kqk, auuid, avrsn);
}

static sqlres internal_apply_subscriber_delta(jv *pc) {
  LT("internal_apply_subscriber_delta");
  jv     *jks      = &((*pc)["ks"]);
  jv     *jdentry  = &((*pc)["dentry"]);
  jv     *md       = &((*pc)["extra_data"]["md"]);
  // NOTE: jrchans NOT a pointer, pc[dentry] can change
  jv      jrchans  = (*jdentry)["delta"]["_meta"]["replication_channels"];
  string  kqk      = (*jks)["kqk"].asString();
  UInt64  o_nbytes = zcreap_get_ocrdt_num_bytes(md);
  sqlres  sr       = set_subscriber_agent_version(pc);
  RETURN_SQL_ERROR(sr)
  ztrace_start("ZSD.ASD.DO.IASD.conditional_apply_subscriber_delta");
  sr               = conditional_apply_subscriber_delta(pc);
  ztrace_finish("ZSD.ASD.DO.IASD.conditional_apply_subscriber_delta");
  RETURN_SQL_ERROR(sr)
  sr               = persist_key_repchans(kqk, &jrchans);
  RETURN_SQL_ERROR(sr)
  sr               = zcreap_store_num_bytes(pc, o_nbytes, false);
  RETURN_SQL_ERROR(sr)
  sr               = finalize_apply_subscriber_delta(pc);
  return sr;
}

static sqlres do_persist_subscriber_delta(string &kqk, jv *jdentry) {
  jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
  string  avrsn   = (*jauthor)["agent_version"].asString();
  LD("do_persist_subscriber_delta: K: " << kqk << " AV: " << avrsn);
  sqlres  sr      = persist_persisted_subscriber_delta(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  sr              = persist_subscriber_delta_version(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  ztrace_start("ZSD.ASD.DO.CPSD.zds_store_subscriber_delta");
  sr              = zds_store_subscriber_delta(kqk, jdentry);
  ztrace_finish("ZSD.ASD.DO.CPSD.zds_store_subscriber_delta");
  RETURN_SQL_ERROR(sr)
  string  ddkey   = zs_get_dirty_subscriber_delta_key(kqk, jauthor);
  return            persist_dirty_delta(ddkey);
}

sqlres zsd_conditional_persist_subscriber_delta(string &kqk, jv *jdentry) {
  jv     *jauthor  = &((*jdentry)["delta"]["_meta"]["author"]);
  string  avrsn    = (*jauthor)["agent_version"].asString();
  sqlres  sr       = fetch_persisted_subscriber_delta(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  bool    p        = sr.ures ? true : false;
  if (!p) return do_persist_subscriber_delta(kqk, jdentry);
  else {
    LD("ZDS.persist_subscriber_delta: K: " << kqk <<
       " AV: " << avrsn << " DELTA ALREADY PERSISTED -> NO-OP");
    RETURN_EMPTY_SQLRES
  }
}

static sqlres do_apply_subscriber_delta(jv *pc) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jdentry = &((*pc)["dentry"]);
  string  kqk     = (*jks)["kqk"].asString();
  ztrace_start("ZSD.ASD.DO.conditional_persist_subscriber_delta");
  sqlres  sr      = zsd_conditional_persist_subscriber_delta(kqk, jdentry);
  ztrace_finish("ZSD.ASD.DO.conditional_persist_subscriber_delta");
  if (sr.err.size() && !sr.err.compare(ZS_Errors["DeltaStorageMaxHit"])) {
    return fail_apply_subscriber_delta_max_size(jks, jdentry);
  }
  RETURN_SQL_ERROR(sr)
  ztrace_start("ZSD.ASD.DO.internal_apply_subscriber_delta");
  sr              = internal_apply_subscriber_delta(pc);
  ztrace_finish("ZSD.ASD.DO.internal_apply_subscriber_delta");
  return sr;
}

sqlres retry_subscriber_delta(jv *pc) {
  sqlres sr;
  CHAOS_MODE_ENTRY_POINT(3)
  sr = do_apply_subscriber_delta(pc);
  if (sr.err.size()) {
    if (!sr.err.compare(ZS_Errors["DeltaNotSync"]) ||
        !sr.err.compare(ZS_Errors["DeltaStorageMaxHit"])) {
      RETURN_EMPTY_SQLRES
    } else {
      LD("ERROR: do_apply_subscriber_delta: " << sr.err);
      RETURN_SQL_ERROR(sr)
    }
  }
  return sr;
}

sqlres zsd_create_sd_pc(jv *jks, jv *jdentry, jv *md) {
  bool    watch  = (*md)["watch"].asBool();
  string  kqk    = (*jks)["kqk"].asString();
  string  op     = "SubscriberDelta";
  // PERFORMANCE OPTIMIZATIONS: [single copy PC & EDATA]
  sqlres  srret; 
  srret.jres     = zh_init_pre_commit_info(op, jks, jdentry, NULL, NULL);
  jv     *pc     = &(srret.jres);
  (*pc)["extra_data"]["watch"] = watch;
  (*pc)["extra_data"]["md"]    = *md;
  return srret;
}

static sqlres apply_subscriber_delta(jv *jks, jv *jdentry, jv *md,
                                     bool replay, jv *jpcrdt) {
  sqlres  sr     = zsd_create_sd_pc(jks, jdentry, md);
  RETURN_SQL_ERROR(sr)
  jv      pc     = sr.jres;
  string  udid   = get_subscriber_delta_ID(&pc);
  jv     *jocrdt = &((*md)["ocrdt"]);
  jv     *jdmeta = &((*jdentry)["delta"]["_meta"]);
  zh_calculate_rchan_match(&pc, jocrdt, jdmeta);
  sr        = persist_fixlog(udid, &pc);   //--------- START FIXLOG ----------|
  RETURN_SQL_ERROR(sr)                     //                                 |
  ztrace_start("ZSD.ASD.retry_subscriber_delta");
  sr        = retry_subscriber_delta(&pc); //          RETRY                  |
  ztrace_finish("ZSD.ASD.retry_subscriber_delta");
  ACTIVATE_FIXLOG_RETURN_SQL_ERROR(sr)     //                                 |
  sr        = remove_fixlog(udid);         //--------- END FIXLOG ------------|
  RETURN_SQL_ERROR(sr)
  ztrace_start("ZSD.ASD.post_apply_subscriber_delta");
  sr        = post_apply_subscriber_delta(&pc, replay);
  ztrace_finish("ZSD.ASD.post_apply_subscriber_delta");
  RETURN_SQL_ERROR(sr)
  zh_override_pc_ncrdt_with_md_gcv(&pc); // GCV used in save_new_crdt()
  *jpcrdt = pc["ncrdt"]; // PASS BACK NCRDT
  RETURN_EMPTY_SQLRES
}

static sqlres post_apply_subscriber_delta(jv *pc, bool replay) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jdentry = &((*pc)["dentry"]);
  jv     *jncrdt  = &((*pc)["ncrdt"]);
  jv     *md      = &((*pc)["extra_data"]["md"]);
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  string  avrsn   = (*jdmeta)["author"]["agent_version"].asString();
  LD("post_apply_subscriber_delta: K: " << kqk << " AV: " << avrsn <<
     " REPLAY: " << replay);
  sqlres  sr      = post_apply_cleanup(jks, jdentry, jncrdt, md);
  RETURN_SQL_ERROR(sr)
  if (replay) RETURN_SQL_RESULT_JSON(*jncrdt) // NO RECURSION
  else        return zoor_check_replay_ooo_deltas(pc, true, true);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CHECK SUBSCRIBER GCV ------------------------------------------------------

static sqlres zsd_check_delta_gcv(jv *jks, jv *jdentry, jv *md, bool is_sub) {
  string  kqk       = (*jks)["kqk"].asString();
  jv     *jdmeta    = &((*jdentry)["delta"]["_meta"]);
  jv     *jauthor   = &((*jdmeta)["author"]);
  bool    is_auto   = zh_get_bool_member(jdmeta, "AUTO");
  bool    do_reo    = zh_get_bool_member(jdmeta, "DO_REORDER");
  bool    do_reap   = zh_get_bool_member(jdmeta, "DO_REAP");
  bool    do_ignore = zh_get_bool_member(jdmeta, "DO_IGNORE");
  bool    no_check  = (is_auto || do_reo || do_reap || do_ignore);
  UInt64  gcv       = (*md)["gcv"].asUInt64();
  if (no_check) {
    LT("zsd_check_delta_gcv: NO_CHECK: CGCV: " << gcv);
    RETURN_SQL_RESULT_UINT(1)
  } else {
    string kqk  = (*jks)["kqk"].asString();
    UInt64 dgcv = zh_get_dentry_gcversion(jdentry);
    LD("DELTA: CHECK_GCV: K: " << kqk << " GCV: " << gcv << " DGCV: " << dgcv);
    if (dgcv >= gcv) { // dgcv > gcv, delta contains post GC deltas -> SAFE
      sqlres sr = zoor_unset_ooo_gcv_delta(kqk, jauthor);
      RETURN_SQL_ERROR(sr)
      RETURN_SQL_RESULT_UINT(1)
    } else {
      string  avrsn  = (*jdmeta)["author"]["agent_version"].asString();
      LE("DELTA: OOO-GCV: K: " << kqk << " AV: " << avrsn <<
         " GCV: " << gcv << " DGCV: " << dgcv);
      sqlres  sr     = zoor_handle_ooo_gcv_delta(jks, jdentry, md, true);
      RETURN_SQL_ERROR(sr)
      jv     *jncrdt = &(sr.jres);
      sr             = save_new_crdt("ZSD.CheckDeltaGCV", kqk, jncrdt);
      RETURN_SQL_ERROR(sr)
      RETURN_SQL_RESULT_UINT(0)
    }
  }
}

// NOTE: returns CRDT
static sqlres handle_ok_av_subscriber_delta(jv *jks, jv *jdentry, jv *md,
                                            bool replay, jv *jpcrdt) {
  LT("handle_ok_av_subscriber_delta");
  sqlres sr = zsd_check_delta_gcv(jks, jdentry, md, true);
  RETURN_SQL_ERROR(sr)
  bool   ok = sr.ures ? true : false;
  if (!ok) RETURN_EMPTY_SQLRES
  else     return apply_subscriber_delta(jks, jdentry, md, replay, jpcrdt);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CHECK SUBSCRIBER AV -------------------------------------------------------

void zsd_handle_unexpected_ooo_delta(jv *jks, jv *einfo) {
  string kqk = (*jks)["kqk"].asString();
  LE("UNEXPECTED OOO-SD (UNEXSD) -> SYNC-KEY: K: " << kqk);
  zas_set_agent_sync_key_signal_agent_to_sync_keys(jks);
}

static sqlres get_check_sdav_metadata(string &kqk, jv *jdentry) {
  jv      avmd    = JOBJECT;
  jv     *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  UInt64  auuid   = (*jdmeta)["author"]["agent_uuid"].asUInt64();
  sqlres  sr      = fetch_device_subscriber_version(kqk, auuid);
  RETURN_SQL_ERROR(sr)
  avmd["sentry"]  = JOBJECT;
  avmd["sentry"]["savrsn"]  = sr.sres;
  sr              = fetch_device_subscriber_version_timestamp(kqk, auuid);
  RETURN_SQL_ERROR(sr)
  UInt64  ts      = sr.ures ? sr.ures : ULLONG_MAX;
  avmd["sentry"]["ts"] = ts;
  sr              = zdelt_get_agent_delta_dependencies(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *mdeps   = &(sr.jres);
  avmd["mdeps"]   = *mdeps;
  RETURN_SQL_RESULT_JSON(avmd)
}

static sqlres do_check_subscriber_delta_agent_version(string &kqk, jv *jdentry,
                                                      jv *md) {
  LT("do_check_subscriber_delta_agent_version");
  jv     *jdmeta   = &((*jdentry)["delta"]["_meta"]);
  UInt64  auuid    = (*jdmeta)["author"]["agent_uuid"].asUInt64();
  string  avrsn    = (*jdmeta)["author"]["agent_version"].asString();
  sqlres  sr       = get_check_sdav_metadata(kqk, jdentry);
  RETURN_SQL_ERROR(sr)
  jv     *avmd     = &(sr.jres);
  string  savrsn   = (*avmd)["sentry"]["savrsn"].asString();
  UInt64  savnum   = zh_get_avnum(savrsn);
  jv      einfo    = JOBJECT;
  einfo["is_sub"]  = true;
  einfo["kosync"]  = zh_get_bool_member(md, "to_sync_key") ||
                     zh_get_bool_member(md, "out_of_sync_key");
  einfo["knreo"]   = (*md)["knreo"];
  einfo["central"] = zh_get_bool_member(jdmeta, "from_central");
  einfo["oooav"]   = false;
  einfo["ooodep"]  = false;
  einfo["is_reo"]  = false;
  UInt64  rconn    = AgentLastReconnect;
  bool    watch    = (*md)["watch"].asBool();
  UInt64  ts       = (*avmd)["sentry"]["ts"].asUInt64();
  bool    wskip    = (watch && (ts < rconn));
  if (wskip) {
    jv c;
    c["savrsn"] = savrsn;
    c["einfo"]  = einfo;
    c["rpt"]    = false;
    RETURN_SQL_RESULT_JSON(c)
  } else {
    UInt64  eavnum  = savnum + 1;
    UInt64  avnum   = zh_get_avnum(avrsn);
    bool    rpt     = (avnum <  eavnum);
    bool    oooav   = (avnum != eavnum);
    jv     *mdeps   = &((*avmd)["mdeps"]);
    jv     *jdeps   = &((*jdmeta)["dependencies"]);
    bool    ooodep  = zh_check_ooo_dependencies(kqk, avrsn, jdeps, mdeps);
    bool    is_reo  = zh_jv_is_member(jdmeta, "reference_uuid");
    einfo["oooav"]  = oooav;
    einfo["ooodep"] = ooodep;
    einfo["is_reo"] = is_reo;
    LD("(S)AV-CHECK: K: " << kqk << " U: " << auuid <<
       " SAVN: " << savnum << " EAVN: " << eavnum << " AVN: " << avnum <<
       " watch: " << watch << " RPT: " << rpt << " REO: " << is_reo << 
       " OOOAV: " << oooav << " OOODEP: " << ooodep <<
       " kosync: " << einfo["kosync"] << " knreo: " << einfo["knreo"] << 
       " central: " << einfo["central"]);
    jv c;
    c["savrsn"] = savrsn;
    c["einfo"]  = einfo;
    c["rpt"]    = rpt;
    RETURN_SQL_RESULT_JSON(c)
  }
}

static sqlres save_post_agent_gc_wait_crdt(jv *pc) {
  zh_override_pc_ncrdt_with_md_gcv(pc); // GCV used in save_new_crdt()
  jv     *jks    = &((*pc)["ks"]);
  jv     *jncrdt = &((*pc)["ncrdt"]);
  string  kqk    = (*jks)["kqk"].asString();
  return save_new_crdt("save_post_agent_gc_wait_crdt", kqk, jncrdt);
}

static sqlres check_subscriber_delta_av_fail(jv *jks, jv *jdentry, 
                                             jv *md, jv *c) {
  jv   *einfo  = &((*c)["einfo"]);
  bool  is_reo = zh_get_bool_member(einfo, "is_reo");
  if (!is_reo) RETURN_EMPTY_SQLRES
  else {
    string  kqk    = (*jks)["kqk"].asString();
    jv     *jdmeta = &((*jdentry)["delta"]["_meta"]);
    string  avrsn  = (*jdmeta)["author"]["agent_version"].asString();
    LD("check_subscriber_delta_av_fail: K: " << kqk  << " AV: " << avrsn);
    sqlres sr      = zsd_create_sd_pc(jks, jdentry, md);
    RETURN_SQL_ERROR(sr)
    jv      pc     = sr.jres;
    sr             = zoor_decrement_reorder_delta(&pc);
    RETURN_SQL_ERROR(sr)
    return save_post_agent_gc_wait_crdt(&pc);
  }
}

static sqlres handle_subscriber_delta_av_fail(jv *jks, jv *jdentry,
                                              jv *md, jv *c) {
  jv     *jdmeta = &((*jdentry)["delta"]["_meta"]);
  string  avrsn  = (*jdmeta)["author"]["agent_version"].asString();
  LE("OOO-AV SUBSCRIBER-DELTA: SAV: " << (*c)["savrsn"] << " AV: " << avrsn);
  jv     *einfo = &((*c)["einfo"]);
  sqlres  sr    = zoor_persist_ooo_delta(jks, jdentry, einfo);
  RETURN_SQL_ERROR(sr)
  sr            = check_subscriber_delta_av_fail(jks, jdentry, md, c);
  RETURN_SQL_ERROR(sr)
  RETURN_SQL_RESULT_UINT(0)
}

static sqlres check_subscriber_delta_agent_version(jv *jks, jv *jdentry,
                                                   jv *md) {
  string  kqk    = (*jks)["kqk"].asString();
  jv     *jdmeta = &((*jdentry)["delta"]["_meta"]);
  string  avrsn  = (*jdmeta)["author"]["agent_version"].asString();
  sqlres  sr     = do_check_subscriber_delta_agent_version(kqk, jdentry, md);
  RETURN_SQL_ERROR(sr)
  jv     *c      = &(sr.jres);
  if ((*c)["rpt"].asBool()) {
    LE("REPEAT SUBSCRIBER-DELTA: K: " << kqk << " AV: " << avrsn);
    RETURN_SQL_RESULT_UINT(0)
  } else if ((*c)["einfo"]["kosync"].asBool()) {
    LE("AV-CHECK FAIL: SUBSCRIBER-DELTA: KEY NOT IN SYNC: K: " << kqk);
    jv *einfo = &((*c)["einfo"]);
    sr        = zoor_persist_ooo_delta(jks, jdentry, einfo);
    RETURN_SQL_ERROR(sr)
    RETURN_SQL_RESULT_UINT(0)
  } else if ((*c)["einfo"]["oooav"].asBool()) {
    return handle_subscriber_delta_av_fail(jks, jdentry, md, c);
  } else if ((*c)["einfo"]["ooodep"].asBool()) {
    LE("OOO-DEP SUBSCRIBER-DELTA: K: " << kqk);
    jv *einfo = &((*c)["einfo"]);
    sr        = zoor_persist_ooo_delta(jks, jdentry, einfo);
    RETURN_SQL_ERROR(sr)
    RETURN_SQL_RESULT_UINT(0)
  } else {
    RETURN_SQL_RESULT_UINT(1)
  }
}

sqlres zsd_internal_handle_subscriber_delta(jv *jks, jv *jdentry, jv *md) {
  string  kqk    = (*jks)["kqk"].asString();
  jv     *jdmeta = &((*jdentry)["delta"]["_meta"]);
  string  avrsn  = (*jdmeta)["author"]["agent_version"].asString();
  bool    fc     = zh_get_bool_member(jdmeta, "from_central");
  LD("ZSD.InternalHandleSubscriberDelta: K: " << kqk << " AV: " << avrsn <<
     " FC: " << fc);
  sqlres  sr      = do_check_subscriber_delta_agent_version(kqk, jdentry, md);
  RETURN_SQL_ERROR(sr)
  jv     *c      = &(sr.jres);
  if ((*c)["rpt"].asBool()) {
    LE("REPEAT-AV: INTERNAL: AV: " << avrsn << " -> NO-OP");
    RETURN_EMPTY_SQLRES
  } else if ((*c)["einfo"]["oooav"].asBool()) {
    LE("OOO-AV: INTERNAL: SAV: " << (*c)["savrsn"] <<
       " AV: " << avrsn << " -> NO-OP");
    RETURN_EMPTY_SQLRES
  } else if ((*c)["einfo"]["ooodep"].asBool()) {
    LE("OOO-DEP: INTERNAL -> NO-OP");
    RETURN_EMPTY_SQLRES
  } else { // NOTE: kosync check not needed
    jv jpcrdt; // PERFORMANCE OPTIMIZATION -> SINGLE COPY
    if (fc) { // AUTO-DELTAS are GCV-OK on REPLAY -> SKIP CheckDeltaGC
      sqlres sr = apply_subscriber_delta(jks, jdentry, md, true, &jpcrdt);
      RETURN_SQL_ERROR(sr)
      sqlres srret;
      srret.ures = 1; // OK
      srret.jres = jpcrdt;
      return srret;
    } else {
      sqlres sr = handle_ok_av_subscriber_delta(jks, jdentry,
                                                md, true, &jpcrdt);
      RETURN_SQL_ERROR(sr)
      sqlres srret;
      srret.ures = 1; // OK
      srret.jres = jpcrdt;
      return srret;
    }
  }
}

// NOTE: returns CRDT
static sqlres do_handle_apply_subscriber_delta(jv *jks, jv *jdentry, jv *md,
                                               bool replay, jv *jpcrdt) {
  sqlres sr = check_subscriber_delta_agent_version(jks, jdentry, md);
  RETURN_SQL_ERROR(sr)
  bool   ok = sr.ures ? true : false;
  if (!ok) RETURN_EMPTY_SQLRES
  else {
    return handle_ok_av_subscriber_delta(jks, jdentry, md, replay, jpcrdt);
  }
}

static sqlres do_flow_handle_subscriber_delta(jv *jks, jv *jdentry,
                                              bool replay) {
  string  kqk = (*jks)["kqk"].asString();
  jv      md  = fetch_subscriber_delta_metadata(kqk, jdentry);
  if (zjn(&md)) RETURN_EMPTY_SQLRES
  jv      jpcrdt; // PERFORMANCE OPTIMIZATION -> SINGLE COPY
  sqlres  sr  = do_handle_apply_subscriber_delta(jks, jdentry, &md, replay,
                                                 &jpcrdt);
  RETURN_SQL_ERROR(sr)
  return save_new_crdt("subscriber_delta", kqk, &jpcrdt);
}

// NOTE: REPLAY means from DENTRIES[]
static sqlres do_process_subscriber_delta(jv *jks, jv *jdentry, bool replay) {
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  string  avrsn   = (*jdmeta)["author"]["agent_version"].asString();
  LD("START: process_subscriber_delta: K: " << kqk << " AV: " << avrsn);
  (*jdmeta)["subscriber_received"] = zh_get_ms_time();
  sqlres  sr      = do_flow_handle_subscriber_delta(jks, jdentry, replay);
  LD("END: process_subscriber_delta: K: " << kqk << " AV: " << avrsn);
  return sr;
}

jv process_subscriber_delta(lua_State *L, string id, jv *params) {
  LT("process_subscriber_delta");
  jv     *jks     = &((*params)["data"]["ks"]);
  jv     *jdentry = &((*params)["data"]["dentry"]);
  sqlres  sr      = do_process_subscriber_delta(jks, jdentry, false);
  PROCESS_GENERIC_ERROR(id, sr)
  return zh_create_jv_response_ok(id);
}

jv process_subscriber_dentries(lua_State *L, string id, jv *params) {
  LT("process_subscriber_dentries");
  jv     *jks       = &((*params)["data"]["ks"]);
  jv     *jdentries = &((*params)["data"]["dentries"]);
  string  kqk       = (*jks)["kqk"].asString();
  for (uint i = 0; i < jdentries->size(); i++) {
    jv     *jdentry = &((*jdentries)[i]);
    LD("process_subscriber_dentries: K: " << kqk << " I: " << i);
    sqlres  sr      = do_process_subscriber_delta(jks, jdentry, true);
    if (sr.err.size()) {
      LD("ERROR: handle_subscriber_dentries: " << sr.err);
    }
  }
  return zh_create_jv_response_ok(id);
}

