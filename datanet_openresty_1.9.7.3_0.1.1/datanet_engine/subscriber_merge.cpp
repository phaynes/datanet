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
#include "external_hooks.h"
#include "shared.h"
#include "convert.h"
#include "deltas.h"
#include "merge.h"
#include "cache.h"
#include "apply_delta.h"
#include "subscriber_delta.h"
#include "ooo_replay.h"
#include "dack.h"
#include "creap.h"
#include "agent_daemon.h"
#include "activesync.h"
#include "gc.h"
#include "dcompress.h"
#include "fixlog.h"
#include "datastore.h"
#include "notify.h"
#include "storage.h"

using namespace std;

extern Int64 MyUUID;

extern jv    zh_nobody_auth;

extern int ChaosMode;
extern vector<string> ChaosDescription;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

bool DeepDebugSubscriberMerge = true; //TODO FIXME HACK

static void deep_debug_subscriber_merge(jv *jncrdt) {
  if (DeepDebugSubscriberMerge) {
    LT("DeepDebugSubscriberMerge");
    jv *jcmeta  = &((*jncrdt)["_meta"]);
    jv *jcrdtdv = &((*jncrdt)["_data"]["V"]);
    zmerge_deep_debug_merge_crdt(jcmeta, jcrdtdv, false);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

sqlres zsm_set_key_in_sync(string &kqk) {
  LE("zsm_set_key_in_sync: K: " << kqk);
  sqlres sr = remove_to_sync_key(kqk);
  RETURN_SQL_ERROR(sr)
  return remove_out_of_sync_key(kqk);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EVICT-MERGE-RACE ----------------------------------------------------------

static sqlres handle_evict_merge_race(jv *jks, bool is_cache) {
  if (is_cache) RETURN_SQL_RESULT_UINT(0)
  else {
    jv     md;
    string kqk    = (*jks)["kqk"].asString();
    sqlres sr     = zsd_get_agent_sync_status(kqk, &md);
    RETURN_SQL_ERROR(sr)
    bool   kosync = zh_get_bool_member(&md, "to_sync_key") ||
                    zh_get_bool_member(&md, "out_of_sync_key");
    if (kosync) RETURN_SQL_RESULT_UINT(0)
    else {
      sr = fetch_evicted(kqk);
      RETURN_SQL_ERROR(sr)
      bool evicted = sr.ures ? true : false;
      if (!evicted) RETURN_SQL_RESULT_UINT(0)
      else {
        LE("NO-OP: EVICT WINS OVER NEED-MERGE: K: " << kqk);// EVICT-MERGE-RACE
        bool send_central = false;
        bool need_merge   = false;
        zcache_internal_evict(jks, send_central, need_merge);
        RETURN_SQL_RESULT_UINT(1)
      }
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BROADCAST SUBSCRIBER MERGE TO WORKER CACHES -------------------------------

static void broadcast_subscriber_merge_to_worker_caches(string &kqk, jv *pc) {
  LT("broadcast_subscriber_merge_to_worker_caches");
  jv     *jncrdt   = &((*pc)["ncrdt"]);
  jv     *jcavrsns = &((*pc)["extra_data"]["cavrsns"]);
  pid_t   xpid     = getpid(); // do NOT send to this PID -> KQK-WORKER
  jv      prequest = zh_create_json_rpc_body("DocumentCacheDelta",
                                            &zh_nobody_auth);
  prequest["params"]["data"]["kqk"]                    = kqk;
  prequest["params"]["data"]["is_merge"]               = true;
  prequest["params"]["data"]["crdt"]                   = *jncrdt;
  prequest["params"]["data"]["central_agent_versions"] = *jcavrsns;
  if (!zexh_broadcast_delta_to_worker_caches(prequest, kqk, xpid)) {
    LE("ERROR: broadcast_subscriber_merge_to_worker_caches");
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER MERGE ----------------------------------------------------------

static bool DebugMerge = true;

static void notify_merge_case(string &kqk, string c) {
  if (DebugMerge) {
    LE("MERGE: K: " << kqk << " CASE: " << c);
    jv debug = JOBJECT;
    debug["subscriber_merge_case"] = c;
    znotify_debug_notify(kqk, &debug);
  }
}

static sqlres subscriber_merge_not_sync(jv *pc) {
  jv     *jks = &((*pc)["ks"]);
  string  kqk = (*jks)["kqk"].asString();
  LD("subscriber_merge_not_sync: K: " << kqk);
  return zas_set_agent_sync_key_signal_agent_to_sync_keys(jks);
}

static void finalize_apply_subscriber_merge(jv *pc) {
  zad_assign_post_merge_deltas(pc, NULL);
  zdelt_on_data_change(pc, true, false);
}

static sqlres post_apply_subscriber_merge(jv *pc) {
  jv     *jks = &((*pc)["ks"]);
  string  kqk = (*jks)["kqk"].asString();
  sqlres  sr  = zsm_set_key_in_sync(kqk);
  RETURN_SQL_ERROR(sr)
  sr          = zadaem_get_agent_key_drain(kqk, true);
  RETURN_SQL_ERROR(sr)
  finalize_apply_subscriber_merge(pc);
  broadcast_subscriber_merge_to_worker_caches(kqk, pc);
  RETURN_EMPTY_SQLRES
}

static sqlres store_subscriber_merge_crdt(string &kqk, jv *pc, jv *jncrdt) {
  jv     *jxcrdt = &((*pc)["xcrdt"]);
  jv     *md     = &((*pc)["extra_data"]["md"]);
  jv     *jxmeta = &((*jxcrdt)["_meta"]);
  UInt64  xgcv   = zh_get_gcv_version(jxmeta);
  LD("store_subscriber_merge_crdt: K: " << kqk);
  zh_set_new_crdt(pc, md, jncrdt, true);
  deep_debug_subscriber_merge(jncrdt);
  UInt64  md_gcv = (*md)["gcv"].asUInt64();
  return zad_store_crdt_and_gc_version(kqk, &((*pc)["ncrdt"]), md_gcv, xgcv); 
}

static sqlres store_lww_ocrdt(jv *pc) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *md      = &((*pc)["extra_data"]["md"]);
  jv     *jocrdt  = &((*md)["ocrdt"]);
  string  kqk     = (*jks)["kqk"].asString();
  bool    oexists = zjd(jocrdt);
  LD("store_lww_ocrdt: K: " << kqk << " O: " << oexists);
  if (!oexists) { // NOTE: do NOT GCV-SET -> done by Reorder(Remove)Delta
    RETURN_EMPTY_SQLRES
  } else {
    jv     *jxcrdt = &((*pc)["xcrdt"]);
    jv     *jxmeta = &((*jxcrdt)["_meta"]);
    UInt64  xgcv   = zh_get_gcv_version(jxmeta);
    zh_set_new_crdt(pc, md, &((*md)["ocrdt"]), true); // MERGE-WINNER(OCRDT)
    UInt64  md_gcv = (*md)["gcv"].asUInt64();
    return zad_store_crdt_and_gc_version(kqk, &((*pc)["ncrdt"]), md_gcv, xgcv); 
  }
}

static sqlres store_remove_subscriber_merge_crdt(jv *pc) {
  (*pc)["remove"] = true;
  jv     *jks     = &((*pc)["ks"]);
  jv     *jxcrdt  = &((*pc)["xcrdt"]);
  jv     *md      = &((*pc)["extra_data"]["md"]);
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jxmeta  = &((*jxcrdt)["_meta"]);
  UInt64  xgcv    = zh_get_gcv_version(jxmeta);
  jv     *jocrdt  = &((*md)["ocrdt"]);
  bool    oexists = zjd(jocrdt);
  return zad_remove_crdt_store_gc_version(kqk, jxcrdt, xgcv, oexists);
}

static sqlres store_lww_xcrdt(jv *pc) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jxcrdt  = &((*pc)["xcrdt"]);
  bool    remove  = (*pc)["extra_data"]["remove"].asBool();
  string  kqk     = (*jks)["kqk"].asString();
  LD("store_lww_xcrdt: K: " << kqk);
  if (remove) return store_remove_subscriber_merge_crdt(pc);
  else        return store_subscriber_merge_crdt(kqk, pc, jxcrdt);
}

static sqlres lww_xcrdt(jv *pc, string mcase) {
  jv     *jks    = &((*pc)["ks"]);
  string  kqk    = (*jks)["kqk"].asString();
  bool    remove  = (*pc)["extra_data"]["remove"].asBool();
  LD("lww_xcrdt: K: " << kqk);
  notify_merge_case(kqk, mcase);
  if (!remove) (*pc)["ncrdt"] = (*pc)["xcrdt"]; // MERGE-WINNER(XCRDT)
  sqlres  sr     = store_lww_xcrdt(pc);
  RETURN_SQL_ERROR(sr)
  if (remove) RETURN_EMPTY_SQLRES
  else        RETURN_SQL_RESULT_JSON((*pc)["ncrdt"])
}

static sqlres lww_ocrdt(jv *pc, string mcase) {
  jv     *jks = &((*pc)["ks"]);
  string  kqk = (*jks)["kqk"].asString();
  LD("lww_ocrdt: K: " << kqk);
  notify_merge_case(kqk, mcase);
  sqlres  sr  = store_lww_ocrdt(pc);
  RETURN_SQL_ERROR(sr)
  return zoor_prepare_local_deltas_post_subscriber_merge(pc);
}

static sqlres do_normal_subscriber_merge(jv *pc) {
  jv     *jks    = &((*pc)["ks"]);
  bool    remove = (*pc)["extra_data"]["remove"].asBool();
  string  kqk    = (*jks)["kqk"].asString();
  if (remove) return lww_xcrdt(pc, "K");
  else {
    LD("SubMerge: NORMAL MERGE"); 
    notify_merge_case(kqk, "M");
    sqlres  sr = zoor_replay_subscriber_merge_deltas(pc, true);
    RETURN_SQL_ERROR(sr)
    return store_subscriber_merge_crdt(kqk, pc, &((*pc)["ncrdt"]));
  }
}

static sqlres do_subscriber_merge(jv *pc) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jxcrdt  = &((*pc)["xcrdt"]);
  jv     *md      = &((*pc)["extra_data"]["md"]);
  jv     *jocrdt  = &((*md)["ocrdt"]);
  string  kqk     = (*jks)["kqk"].asString();
  jv     *jxmeta  = &((*jxcrdt)["_meta"]);
  if (zjn(jocrdt)) { // TOP-LEVEL: NO LOCAL CRDT
    sqlres sr = fetch_last_remove_delta(kqk);
    RETURN_SQL_ERROR(sr)
    jv *rmeta = &(sr.jres);
    if (zjn(rmeta)) { // NO LDR -> NEVER EXISTED LOCALLY
      return lww_xcrdt(pc, "A");
    } else {          // LDR EXISTS -> REMOVED WHILE OFFLINE
      bool r_mismatch = zmerge_compare_zdoc_authors(jxmeta, rmeta);
      if (r_mismatch) { // MISMATCH: LDR & XCRDT DOC-CREATION VERSION
        UInt64 rcreated = (*rmeta)["document_creation"]["@"].asUInt64();
        UInt64 xcreated = (*jxmeta)["document_creation"]["@"].asUInt64();
        // NOTE: case B is impossible, DrainDeltas runs before ToSyncKeys
        if        (rcreated > xcreated) {      // LWW: OCRDT wins
          LD("SubMerge: DocMismatch & LDR[@] NEWER -> NO-OP");
          return lww_ocrdt(pc, "B");
        } else if (rcreated < xcreated) {      // LWW: XCRDT wins
          return lww_xcrdt(pc, "C");
        } else { /* (rcreated === xcreated) */ // LWW: TIEBRAKER
          UInt64 r_uuid = (*rmeta)["author"]["agent_uuid"].asUInt64();
          UInt64 x_uuid = (*jxmeta)["author"]["agent_uuid"].asUInt64();
          if (r_uuid > x_uuid) {
            LD("SubMerge: DocMismatch & LDR[_] HIGHER -> NO-OP");
            return lww_ocrdt(pc, "D");
          } else {
            return lww_xcrdt(pc, "E");
          }
        }
      } else { // MATCH: LDR & XCRDT DOC-CREATION VERSION -> [_,#] match
        LD("SubMerge: Same[@] -> NO-OP");
        return lww_ocrdt(pc, "F");
      }
    }
  } else {           // TOP-LEVEL: Local CRDT exists
    jv *jometa = &((*jocrdt)["_meta"]);;
    bool a_mismatch = zmerge_compare_zdoc_authors(jxmeta, jometa);
    if (a_mismatch) { // MISMATCH: OCRDT & XCRDT DOC-CREATION VERSION
      UInt64 ocreated = (*jometa)["document_creation"]["@"].asUInt64();
      UInt64 xcreated = (*jxmeta)["document_creation"]["@"].asUInt64();
      if        (ocreated > xcreated) {      // LWW: OCRDT wins
        LD("SubMerge: OCRDT[@] NEWER -> NO-OP");
        return lww_ocrdt(pc, "G");
      } else if (ocreated < xcreated) {      // LWW: XCRDT wins
        return lww_xcrdt(pc, "H");
      } else { /* (ocreated === xcreated) */ // LWW: TIEBRAKER
        UInt64 o_uuid = (*jometa)["document_creation"]["_"].asUInt64();
        UInt64 x_uuid = (*jxmeta)["document_creation"]["_"].asUInt64();
        if (o_uuid > x_uuid) {
          LD("SubMerge: Same[@] & OCRDT[_] HIGHER -> NO-OP");
          return lww_ocrdt(pc, "I");
        } else {
          return lww_xcrdt(pc, "J");
        }
      }
    } else {          // MATCH: OCRDT & XCRDT DOC-CREATION VERSION
      return do_normal_subscriber_merge(pc);
    }
  }
}

static sqlres store_ether(string &kqk, vector<jv *> *pdentries) {
  for (uint i = 0; i < pdentries->size(); i++) {
    jv     *jdentry = (*pdentries)[i];
    jv     *jmeta   = &((*jdentry)["delta"]["_meta"]);
    UInt64  auuid   = (*jmeta)["author"]["agent_uuid"].asUInt64();
    if ((int)auuid != MyUUID) { // Do NOT store SELFIE
      sqlres sr = zsd_conditional_persist_subscriber_delta(kqk, jdentry);
      RETURN_SQL_ERROR(sr)
    }    
  }
  RETURN_EMPTY_SQLRES
}

static sqlres store_subscriber_merge_ether(jv *pc) {
  LT("store_subscriber_merge_ether");
  jv     *jks    = &((*pc)["ks"]);
  jv     *jether = &((*pc)["extra_data"]["ether"]);
  string  kqk    = (*jks)["kqk"].asString();
  vector<jv *>   pdentries;
  vector<string> mbrs = zh_jv_get_member_names(jether);
  for (uint i = 0; i < mbrs.size(); i++) {
    jv *jval = &((*jether)[mbrs[i]]);
    pdentries.push_back(jval);
  }
  return store_ether(kqk, &pdentries);
}

static sqlres reset_subscriber_AVs(string &kqk) {
  sqlres          sr   = fetch_per_key_subscriber_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv             *jsvs = &(sr.jres);
  vector<string>  mbrs = zh_jv_get_member_names(jsvs);
  for (uint i = 0; i < mbrs.size(); i++) {
    string sauuid = mbrs[i];
    UInt64 auuid  = strtoul(sauuid.c_str(), NULL, 10);
    sqlres sr     = remove_device_subscriber_version(kqk, auuid);
    RETURN_SQL_ERROR(sr)
    sr            = remove_per_key_subscriber_version(kqk, auuid);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres set_subscriber_agent_versions(jv *pc) {
  LT("set_subscriber_agent_versions");
  jv     *jks      = &((*pc)["ks"]);
  jv     *jcavrsns = &((*pc)["extra_data"]["cavrsns"]);
  string  kqk      = (*jks)["kqk"].asString();
  sqlres sr        = reset_subscriber_AVs(kqk);
  RETURN_SQL_ERROR(sr)
  if (zjn(jcavrsns)) RETURN_EMPTY_SQLRES
  vector<string> mbrs = zh_jv_get_member_names(jcavrsns);
  for (uint i = 0; i < mbrs.size(); i++) {
    jv     *jcavrsn = &((*jcavrsns)[mbrs[i]]);
    string  cavrsn  = jcavrsn->asString();
    UInt64  auuid   = strtoul(mbrs[i].c_str(), NULL, 10);
    LD("do_set_subscriber_AV: K: " << kqk << " (C)AV: " << cavrsn);
    sqlres  sr      = persist_device_subscriber_version(kqk, auuid, cavrsn);
    RETURN_SQL_ERROR(sr)
    sr              = persist_per_key_subscriber_version(kqk, auuid, cavrsn);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

sqlres set_key_metadata(jv *pc) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jxcrdt  = &((*pc)["xcrdt"]);
  string  kqk     = (*jks)["kqk"].asString();
  if (zjn(jxcrdt)) RETURN_EMPTY_SQLRES // REMOVE
  else {
    jv     *jrchans   = &((*jxcrdt)["_meta"]["replication_channels"]);
    sqlres  sr        = persist_key_repchans(kqk, jrchans);
    RETURN_SQL_ERROR(sr)
    sr                = persist_key_info_field(kqk, "PRESENT", 1);
    RETURN_SQL_ERROR(sr)
    jv     *jcreateds = &((*jxcrdt)["_meta"]["created"]);
    return zdack_set_agent_createds(jcreateds);
  }
}

static sqlres do_apply_subscriber_merge(jv *pc) {
  LT("do_apply_subscriber_merge");
  sqlres  sr;
  jv     *jks      = &((*pc)["ks"]);
  jv     *md       = &((*pc)["extra_data"]["md"]);
  jv     *jgcsumms = &((*pc)["extra_data"]["gcsumms"]);
  string  kqk      = (*jks)["kqk"].asString();
  UInt64  o_nbytes = zcreap_get_ocrdt_num_bytes(md);
  sr = store_subscriber_merge_ether(pc);
  RETURN_SQL_ERROR(sr)
  CHAOS_MODE_ENTRY_POINT(4)
  sr = zgc_save_gcv_summaries(kqk, jgcsumms);
  RETURN_SQL_ERROR(sr)
  sr = set_subscriber_agent_versions(pc);
  RETURN_SQL_ERROR(sr)
  sr = set_key_metadata(pc);
  RETURN_SQL_ERROR(sr)
  sr = zgc_cancel_agent_gc_wait(kqk);
  RETURN_SQL_ERROR(sr)
  (*pc)["ncrdt"] = (*pc)["xcrdt"]; // NOTE: START ZCR BOOK-KEEPING
  sr = zcreap_store_num_bytes(pc, o_nbytes, false);
  pc->removeMember("ncrdt");       // NOTE: END ZCR BOOK-KEEPING
  RETURN_SQL_ERROR(sr)
  sr = do_subscriber_merge(pc);
  if (sr.err.size()) {
    if (sr.err.compare(ZS_Errors["DeltaNotSync"])) return sr;
    else {
      return subscriber_merge_not_sync(pc);
    }
  }
  post_apply_subscriber_merge(pc);
  return sr;
}

sqlres retry_subscriber_merge(jv *pc) {
  return do_apply_subscriber_merge(pc);
}

static sqlres start_subscriber_merge(jv   *jks,      jv   *jxcrdt,
                                     jv   *jcavrsns, jv   *jgcsumms, jv *jether,
                                     bool  remove,   bool  is_cache, jv *md) {
  string  kqk       = (*jks)["kqk"].asString();
  UInt64  gcv       = (*md)["gcv"].asUInt64();
  LD("start_subscriber_merge: K: " << kqk << " (C)GCV: " << gcv);
  //LD("start_subscriber_merge: XCRDT: " << *jxcrdt);
  jv edata;
  edata["cavrsns"]  = *jcavrsns;
  edata["gcsumms"]  = *jgcsumms;
  edata["ether"]    = *jether;
  edata["remove"]   = remove;
  edata["is_cache"] = is_cache;
  edata["md"]       = *md;
  jv     *jocrdt    = &((*md)["ocrdt"]);
  jv     *jxmeta    = &((*jxcrdt)["_meta"]);
  string  op        = "SubscriberMerge";
  jv      pc        = zh_init_pre_commit_info(op, jks, NULL, jxcrdt, &edata);
  if (!remove) zh_calculate_rchan_match(&pc, jocrdt, jxmeta);
  sqlres sr = persist_fixlog(kqk, &pc); // -------- START FIXLOG ----------|
  RETURN_SQL_ERROR(sr)                  //                                 |
  sr = retry_subscriber_merge(&pc);     //          RETRY                  |
  ACTIVATE_FIXLOG_RETURN_SQL_ERROR(sr)  //                                 |
  return remove_fixlog(kqk);            // -------- END FIXLOG ------------|
}

static jv get_subscriber_merge_metadata(string &kqk) {
  jv     md;
  sqlres sr      = zsd_fetch_local_key_metadata(kqk, &md);
  RETURN_SQL_ERROR_AS_JNULL(sr)
  md["is_merge"] = true;
  return md;
}

sqlres do_process_subscriber_merge(jv *jks, jv *jxcrdt, jv *jcavrsns,
                                   jv *jgcsumms, jv *jether,
                                   bool remove, bool is_cache) {
  LT("do_process_subscriber_merge");
  string kqk = (*jks)["kqk"].asString();
  jv     md  = get_subscriber_merge_metadata(kqk);
  if (zjn(&md)) RETURN_EMPTY_SQLRES
  return start_subscriber_merge(jks, jxcrdt, jcavrsns, jgcsumms, jether,
                                remove, is_cache, &md);
}

static sqlres apply_subscriber_merge(jv *jks, jv *jxcrdt, 
                                     jv *jcavrsns, jv *jgcsumms, jv *jether,
                                     bool remove, bool is_cache) {
  LT("apply_subscriber_merge: XCDRT: " << *jxcrdt);
  sqlres sr      = handle_evict_merge_race(jks, is_cache);
  RETURN_SQL_ERROR(sr)
  bool   evicted = sr.ures ? true : false;
  if (evicted) RETURN_EMPTY_SQLRES
  else {
    return do_process_subscriber_merge(jks, jxcrdt, jcavrsns, jgcsumms,
                                       jether, remove, is_cache);
  }
}

jv process_subscriber_merge(lua_State *L, string id, jv *params) {
  LT("process_subscriber_merge");
  jv     *jpd  = &((*params)["data"]);
  jv     *jks  = &((*jpd)["ks"]);
  string  prid = zh_get_string_member(jpd, "request_id");
  string  kqk  = (*jks)["kqk"].asString();
  sqlres  sr   = fetch_need_merge_request_id(kqk);
  PROCESS_GENERIC_ERROR(id, sr)
  string *rid  = &(sr.sres);
  LD("SubscriberMerge: PRID: " << prid << " RID: " << *rid);
  // NOTE: GeoNeedMerge SEND SubscriberMerge w/o request_id
  if (prid.size() && rid->compare(prid)) {
    sqlres sr;
    sr.err = ZS_Errors["OverlappingSubscriberMergeRequests"];
    PROCESS_GENERIC_ERROR(id, sr)
    return zh_create_jv_response_ok(id); // compiler warning
  } else {
    jv     *jxcrdt   = &((*jpd)["zcrdt"]);
    zcmp_decompress_crdt(jxcrdt);
    jv     *jcavrsns = &((*jpd)["central_agent_versions"]);
    jv     *jgcsumms = &((*jpd)["gc_summary"]);
    jv     *jether   = &((*jpd)["ether"]);
    bool    remove   = zh_get_bool_member(jpd, "remove");
    bool    is_cache = zh_get_bool_member(jpd, "is_cache");
    sqlres  sr       = apply_subscriber_merge(jks, jxcrdt, jcavrsns, jgcsumms,
                                              jether, remove, is_cache);
    PROCESS_GENERIC_ERROR(id, sr)
    return zh_create_jv_response_ok(id);
  }
}

