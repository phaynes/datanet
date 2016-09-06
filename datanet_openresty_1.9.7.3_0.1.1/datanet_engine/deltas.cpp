#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

extern "C" {
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
#include "xact.h"
#include "convert.h"
#include "oplog.h"
#include "merge.h"
#include "deltas.h"
#include "cache.h"
#include "apply_delta.h"
#include "subscriber_delta.h"
#include "creap.h"
#include "gc.h"
#include "auth.h"
#include "aio.h"
#include "datastore.h"
#include "notify.h"
#include "trace.h"
#include "storage.h"

using namespace std;

extern Int64 MyUUID;

extern jv zh_nobody_auth;

extern int ChaosMode;
extern vector<string> ChaosDescription;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BROADCAST AGENT DELTA TO WORKER CACHES ------------------------------------

static void broadcast_agent_delta_to_worker_caches(string &kqk, jv *jcrdt,
                                                   jv *jdentry, UInt64 apid) {
  LT("broadcast_agent_delta_to_worker_caches");
  jv prequest = zh_create_json_rpc_body("DocumentCacheDelta", &zh_nobody_auth);
  prequest["params"]["data"]["kqk"]      = kqk;
  prequest["params"]["data"]["is_merge"] = false;
  prequest["params"]["data"]["crdt"]     = *jcrdt;
  prequest["params"]["data"]["dentry"]   = *jdentry;
  if (!zexh_broadcast_delta_to_worker_caches(prequest, kqk, apid)) {
    LD("ERROR: broadcast_agent_delta_to_worker_caches");
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RESPONSE HELPERS ----------------------------------------------------------

static jv respond_client_commit(string &id, jv *jncrdt, jv *mdeps) {
  jv response = zh_create_jv_response_ok(id);
  if (zjd(jncrdt)) {
    response["result"]["crdt"]    = *jncrdt;
  } else {
    response["result"]["removed"] = true;
  }
  response["result"]["dependencies"] = *mdeps;
  return response;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ON DATA CHANGE ------------------------------------------------------------

void zdelt_on_data_change(jv *pc, bool full_doc, bool selfie) {
  sqlres sr = fetch_notify_set();
  if (sr.err.size()) return;
  bool nset = sr.ures ? true : false;
  LD("zdelt_on_data_change: NotifySet: " << nset);
  if (nset) znotify_notify_on_data_change(pc, full_doc, selfie);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE JSON HELPERS --------------------------------------------------------

static sqlres create_crdt_from_json(string &kqk, jv *jdata) {
  LT("create_crdt_from_json");
  jv rchans; // NOTE: EMPTY
  if (zh_jv_is_member(jdata, "_channels")) {
    rchans = (*jdata)["_channels"];
    if (!rchans.isArray()) RETURN_SQL_RESULT_ERROR(ZS_Errors["RChansNotArray"])
  }
  UInt64 expiration = 0;
  if (zh_jv_is_member(jdata, "_expire")) {
    UInt64 expire = (*jdata)["_expire"].asUInt64();
    UInt64 nows   = (zh_get_ms_time() / 1000);
    expiration    = nows + expire;
    jdata->removeMember("_expire");
  }
  string sfname;
  if (zh_jv_is_member(jdata, "_server_filter")) {
    sfname = (*jdata)["_server_filter"].asString();
  }
  jv  jmeta  = zh_init_meta(kqk, &rchans, expiration, sfname);
  jv  jjson  = *jdata;                  // clone -> delete keyword fields
  jjson.removeMember("_id");            // extract "_id" from CRDT's body
  jjson.removeMember("_channels");      // extract "_channels"
  jjson.removeMember("_server_filter"); // extract "_server_filter"
  jv  jcrdtd = zconv_convert_json_object_to_crdt(&jjson, &jmeta, false);
  jv  jcrdt  = zh_format_crdt_for_storage(kqk, &jmeta, &jcrdtd);
  RETURN_SQL_RESULT_JSON(jcrdt)
}

static jv init_first_delta(jv *jcrdt) {
  jv *jcmeta = &((*jcrdt)["_meta"]);
  jv  jdelta = zh_init_delta(jcmeta);
  jv *jdata  = &((*jcrdt)["_data"]["V"]);
  vector<string> mbrs = zh_jv_get_member_names(jdata);
  for (uint i = 0; i < mbrs.size(); i++) { // [+] ALL first level members
    jv *jval = &((*jdata)[mbrs[i]]);
    (*jval)["+"] = true;
  }
  return jdelta;
}
  
sqlres zdelt_get_key_subscriber_versions(string &kqk) {
  LT("zdelt_get_key_subscriber_versions");
  sqlres  sr    = fetch_per_key_subscriber_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *mdeps = &(sr.jres);
  if (zjn(mdeps)) {
    *mdeps = JOBJECT;
  } else {
    mdeps->removeMember("_id");
  }
  return sr;
}

sqlres zdelt_get_agent_delta_dependencies(string &kqk) {
  LT("zdelt_get_agent_delta_dependencies");
  sqlres  sr    = zdelt_get_key_subscriber_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *mdeps = &(sr.jres);
  if (zjd(mdeps)) { // do NOT include MYUUID's dependency
    mdeps->removeMember(to_string(MyUUID));
  }
  return sr;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DRAIN DELTA QUEUE (FROM AGENT TO CENTRAL) ---------------------------------

void zdelt_delete_meta_timings(jv *jdentry) {
  LT("zdelt_delete_meta_timings");
  jv *jdmeta = &((*jdentry)["delta"]["_meta"]);
  jdmeta->removeMember("agent_received");
  jdmeta->removeMember("geo_sent");
  jdmeta->removeMember("geo_received");
  jdmeta->removeMember("subscriber_sent");
  jdmeta->removeMember("subscriber_received");
}

void zdelt_cleanup_agent_delta_before_send(jv *jdentry) {
  LT("zdelt_cleanup_agent_delta_before_send");
  jv *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  jdmeta->removeMember("is_geo"); // Fail-safe
  jdmeta->removeMember("last_member_count");
  jdmeta->removeMember("op_count");
  jdmeta->removeMember("overwrite");
  if (zh_jv_is_member(jdmeta, "removed_channels")) {
    jv jrmc = (*jdmeta)["removed_channels"];
    if (!jrmc.size()) jdmeta->removeMember("removed_channels");
  }
  if (!zh_get_bool_member(jdmeta, "initial_delta")) {
    jdmeta->removeMember("initial_delta");
  }
}

static jv create_agent_delta_request(string &kqk, jv *jdentry, jv *jauth) {
  jv prequest = zh_create_json_rpc_body("AgentDelta", jauth);
  prequest["params"]["data"]           = zh_get_my_device_agent_info();
  prequest["params"]["data"]["ks"]     = zh_create_ks(kqk, NULL);
  prequest["params"]["data"]["dentry"] = *jdentry;
  return prequest;
}

static sqlres drain_agent_delta(string &kqk, jv *jdentry, jv *jauth) {
  LD("drain_agent_delta: "<< zh_summarize_delta(kqk, jdentry));
  jv     *jdmeta   = &((*jdentry)["delta"]["_meta"]);
  string  avrsn    = (*jdmeta)["author"]["agent_version"].asString();
  UInt64  now      = zh_get_ms_time();
  sqlres  sr       = persist_agent_sent_delta(kqk, avrsn, now);
  RETURN_SQL_ERROR(sr)
  jv      pdentry  = *jdentry; // NOTE: gets modified
  zdelt_cleanup_agent_delta_before_send(&pdentry);
  zaio_cleanup_delta_before_send_central(&pdentry);
  jv      prequest = create_agent_delta_request(kqk, &pdentry, jauth);
  if (!zexh_send_central_https_request(0, prequest, false)) {
    RETURN_SQL_RESULT_ERROR("drain_agent_delta");
  }
  RETURN_EMPTY_SQLRES
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT COMMIT -------------------------------------------------------------

sqlres rollback_client_commit(jv *pc) {
  jv     *jks    = &((*pc)["ks"]);
  jv     *md     = &((*pc)["extra_data"]["md"]);
  jv     *jocrdt = &((*md)["ocrdt"]);
  UInt64  ndk    = (*pc)["metadata"]["dirty_key"].asUInt64();
  string  oavrsn = (*pc)["extra_data"]["old_agent_version"].asString();
  string  navrsn = (*pc)["extra_data"]["new_agent_version"].asString();
  string  kqk    = (*jks)["kqk"].asString();
  LE("rollback_client_commit: K: " << kqk);
  sqlres  sr     = persist_agent_version(kqk, oavrsn);
  RETURN_SQL_ERROR(sr)
  CHAOS_MODE_ENTRY_POINT(2)
  zmerge_debug_crdt(jocrdt);
  LD("PERSIST_CRDT: K: " << kqk);
  sqlres sc;
  PERSIST_CRDT(kqk, jocrdt);
  RETURN_SQL_ERROR(sc)
  sr = persist_agent_dirty_keys(kqk, ndk);
  RETURN_SQL_ERROR(sr)
  sr = remove_persisted_subscriber_delta(kqk, navrsn);
  RETURN_SQL_ERROR(sr)
  sr = remove_subscriber_delta_version(kqk, navrsn);
  RETURN_SQL_ERROR(sr)
  sr = zds_remove_agent_delta(kqk, navrsn);
  RETURN_SQL_ERROR(sr)
  string ddkey = zs_get_dirty_agent_delta_key(kqk, navrsn);
  sr           = remove_dirty_subscriber_delta(ddkey);
  RETURN_SQL_ERROR(sr)
  jv  upc = *pc;
  jv *umd = &(upc["extra_data"]["md"]);
  (*umd)["ocrdt"] = (*pc)["ncrdt"];
  upc["ncrdt"]    = (*md)["ocrdt"];
  UInt64 o_nbytes = zcreap_get_ocrdt_num_bytes(umd);
  return zcreap_store_num_bytes(&upc, o_nbytes, true);
}

static sqlres online_drain_agent_delta(jv *pc, jv *jdentry) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jauth   = &((*pc)["extra_data"]["auth"]);
  string  kqk     = (*jks)["kqk"].asString();
  sqlres  sr      = zh_get_agent_offline();
  RETURN_SQL_ERROR(sr)
  bool    offline = sr.ures ? true : false;
  LD("online_drain_agent_delta:  K: " << kqk << " OFF: " << offline);
  if (offline) RETURN_EMPTY_SQLRES
  else         return drain_agent_delta(kqk, jdentry, jauth);
}

static sqlres post_commit_agent_delta(jv *pc) {
  LT("post_commit_agent_delta");
  sqlres sr = online_drain_agent_delta(pc, &(*pc)["dentry"]);
  RETURN_SQL_ERROR(sr)
  zdelt_on_data_change(pc, false, true);
  RETURN_EMPTY_SQLRES
}

string zdelt_get_agent_delta_ID(jv *pc) {
  jv     *jks      = &((*pc)["ks"]);
  string  kqk      = (*jks)["kqk"].asString();
  string  navrsn   = (*pc)["extra_data"]["new_agent_version"].asString();
  return kqk + "-" + navrsn;
}

static sqlres store_agent_delta(string &kqk, jv *jdentry, string navrsn,
                                jv *jauth) {
  LD("store_agent_delta: K: " << kqk << " AV: " << navrsn);
  sqlres  sr   = persist_persisted_subscriber_delta(kqk, navrsn);
  RETURN_SQL_ERROR(sr)
  sr           = persist_subscriber_delta_version(kqk, navrsn);
  RETURN_SQL_ERROR(sr)
  sr           = zds_store_agent_delta(kqk, jdentry, jauth);
  RETURN_SQL_ERROR(sr)
  string ddkey = zs_get_dirty_agent_delta_key(kqk, navrsn);
  return persist_dirty_delta(ddkey);
}

// NOTE: MUST PRECEDE ZCR.StoreNumBytes()
static sqlres commit_delta_set_auto_cache(jv *pc, string &kqk) {
  jv     *jauth   = &((*pc)["extra_data"]["auth"]);
  jv     *jdentry = &((*pc)["dentry"]);
  jv     *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  bool    acache  = zh_get_bool_member(jdmeta, "auto_cache");
  if (!acache) RETURN_EMPTY_SQLRES
  else         return zcache_agent_store_cache_key_metadata(kqk, false, jauth);
}

static sqlres finish_commit_delta(jv *pc, string &kqk, UInt64 o_nbytes) {
  sqlres sr = commit_delta_set_auto_cache(pc, kqk);
  RETURN_SQL_ERROR(sr)
  sr        = zcreap_store_num_bytes(pc, o_nbytes, true);
  RETURN_SQL_ERROR(sr)
  if (zh_jv_is_member(pc, "ncrdt") && zjd(&((*pc)["ncrdt"]))) {
    sqlres sr = zds_store_crdt(kqk, &((*pc)["ncrdt"]));
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres do_commit_delta(jv *pc) {
  LT("do_commit_delta");
  jv     *jks      = &((*pc)["ks"]);
  jv     *jauth    = &((*pc)["extra_data"]["auth"]);
  jv     *jdentry  = &((*pc)["dentry"]);
  jv     *md       = &((*pc)["extra_data"]["md"]);
  UInt64  ndk      = (*pc)["metadata"]["dirty_key"].asUInt64();
  string  oavrsn   = (*pc)["extra_data"]["old_agent_version"].asString();
  string  navrsn   = (*pc)["extra_data"]["new_agent_version"].asString();
  string  kqk      = (*jks)["kqk"].asString();
  UInt64  o_nbytes = zcreap_get_ocrdt_num_bytes(md);
  sqlres  sr       = persist_agent_version(kqk, navrsn);
  RETURN_SQL_ERROR(sr)
  CHAOS_MODE_ENTRY_POINT(1)
  sr               = zsd_set_subscriber_agent_version(kqk, MyUUID, navrsn);
  RETURN_SQL_ERROR(sr)
  ztrace_start("DELT.ZHB.SEND.DO.CC.DO.CAD.ZDCD.DCD.zad_apply_agent_delta");
  sr               = zad_apply_agent_delta(pc);
  ztrace_finish("DELT.ZHB.SEND.DO.CC.DO.CAD.ZDCD.DCD.zad_apply_agent_delta");
  RETURN_SQL_ERROR(sr)
  CHAOS_MODE_ENTRY_POINT(2)
  ndk             += 1;
  sr               = persist_agent_dirty_keys(kqk, ndk);
  RETURN_SQL_ERROR(sr)
  sr               = store_agent_delta(kqk, jdentry, navrsn, jauth);
  RETURN_SQL_ERROR(sr)
  ztrace_start("DELT.ZHB.SEND.DO.CC.DO.CAD.ZDCD.DCD.finish_commit_delta");
  sr               = finish_commit_delta(pc, kqk, o_nbytes);
  ztrace_finish("DELT.ZHB.SEND.DO.CC.DO.CAD.ZDCD.DCD.finish_commit_delta");
  return sr;
}

static sqlres try_agent_delta(jv *pc) {
  sqlres sr = do_commit_delta(pc);
  if (sr.err.size()) {
    if (!sr.err.compare(ZS_Errors["DeltaNotSync"])) {
      RETURN_EMPTY_SQLRES
    } else {
      LD("ERROR: do_commit_delta: " << sr.err);
      RETURN_SQL_ERROR(sr)
    }
  }
  return sr;
}

static sqlres run_commit_delta(string &kqk, jv *pc) {
  string udid = zdelt_get_agent_delta_ID(pc);
  sqlres sr = persist_fixlog(udid, pc); //---------- START FIXLOG ----------|
  RETURN_SQL_ERROR(sr)                  //                                  |
  sr = try_agent_delta(pc);             //          TRY                     |
  ACTIVATE_FIXLOG_RETURN_SQL_ERROR(sr)  //                                  |
  return remove_fixlog(udid);           //---------- END FIXLOG ------------|
}

static sqlres zdelt_do_commit_delta(jv *jks, jv *jdentry, jv *jocrdt,
                                    bool sep, UInt64 locex, jv *jauth,
                                    UInt64 gcv,
                                    jv *jndentry, UInt64 ndk) {
  string kqk     = (*jks)["kqk"].asString();
  sqlres sr      = fetch_agent_version(kqk);
  RETURN_SQL_ERROR(sr)
  string oavrsn  = sr.sres;
  UInt64 oavnum  = zh_get_avnum(oavrsn);
  UInt64 navnum  = oavnum + 1;
  string navrsn  = zh_create_avrsn(MyUUID, navnum);
  LD("do_commit_delta: K: " << kqk << " AV: " << navrsn << " GCV: " << gcv);
  jv     *jdmeta = &((*jndentry)["delta"]["_meta"]);
  (*jdmeta)["author"]["agent_version"] = navrsn;
  jv edata;
  edata["auth"]              = *jauth;
  edata["old_agent_version"] = oavrsn;
  edata["new_agent_version"] = navrsn;
  string op = "AgentDelta";
  jv     pc = zh_init_pre_commit_info(op, jks, jndentry, NULL, &edata);
  // PERFORMANCE OPTIMIZATIONS: [single copy EDATA.MD]
  pc["extra_data"]["md"]          = JOBJECT;
  pc["extra_data"]["md"]["gcv"]   = gcv;
  pc["extra_data"]["md"]["ocrdt"] = *jocrdt;
  if (!zh_calculate_rchan_match(&pc, jocrdt, jdmeta)) {
    RETURN_SQL_RESULT_ERROR(ZS_Errors["RChanChange"]);
  }
  pc["metadata"]["dirty_key"] = ndk;
  ztrace_start("DELT.ZHB.SEND.DO.CC.DO.CAD.ZDCD.run_commit_delta");
  sr        = run_commit_delta(kqk, &pc);
  ztrace_finish("DELT.ZHB.SEND.DO.CC.DO.CAD.ZDCD.run_commit_delta");
  RETURN_SQL_ERROR(sr)
  ztrace_start("DELT.ZHB.SEND.DO.CC.DO.CAD.ZDCD.post_commit_agent_delta");
  sr        = post_commit_agent_delta(&pc);
  ztrace_finish("DELT.ZHB.SEND.DO.CC.DO.CAD.ZDCD.post_commit_agent_delta");
  RETURN_SQL_ERROR(sr)
  sqlres srret; // PERFORMANCE OPTIMIZATION -> SINGLE COPY [NCRDT,NDENTRY]
  srret.jres["crdt"]   = pc["ncrdt"]; // NOTE: RETURN DATA.CRDT
  srret.jres["dentry"] = *jndentry;   // NOTE: RETURN DATA.DENTRY
  return srret;
}

static sqlres check_coalesce_deltas(jv *jks, jv *jdentry,
                                    jv *jocrdt, bool sep, UInt64 locex,
                                    jv *jauth, UInt64 gcv, jv *jndentry,
                                    UInt64 ndk) {
  return zdelt_do_commit_delta(jks, jdentry, jocrdt, sep, locex, jauth,
                               gcv, jndentry, ndk);
}

static sqlres check_auto_cache(string &kqk, jv *jdentry, jv *jauth) {
  jv     *jdmeta  = &((*jdentry)["delta"]["_meta"]);
  jv     *jrchans = &((*jdmeta)["replication_channels"]);
  sqlres  sr      = zauth_is_agent_auto_cache_key(jauth, kqk, jrchans);
  RETURN_SQL_ERROR(sr)
  bool   acache   = sr.ures ? true : false;
  (*jdmeta)["auto_cache"] = acache;
  RETURN_EMPTY_SQLRES
}

// NOTE: AgentDeltas authored at MAX-GCV
static sqlres commit_agent_delta(jv *jks, jv *jdentry, jv *jocrdt,
                                 bool sep, UInt64 locex, jv *jauth) {
  LT("commit_agent_delta");
  string  kqk      = (*jks)["kqk"].asString();
  sqlres  sr       = check_auto_cache(kqk, jdentry, jauth);
  RETURN_SQL_ERROR(sr);
  sr               = fetch_agent_key_max_gc_version(kqk);
  RETURN_SQL_ERROR(sr);
  UInt64  gcv      = sr.ures ? sr.ures : 0;
  sqlres  sj       = zmerge_freshen_agent_delta(kqk, jdentry, jocrdt, gcv);
  RETURN_SQL_ERROR(sj);
  jv     *jndentry = &(sj.jres);
  if (zjn(jndentry)) {
    RETURN_SQL_RESULT_ERROR(ZS_Errors["StaleAgentDelta"]);
  }
  sr               = fetch_agent_dirty_keys(kqk);
  RETURN_SQL_ERROR(sr);
  UInt64 ndk       = sr.ures;
  ztrace_start("DELT.ZHB.SEND.DO.CC.DO.CAD.check_coalesce_deltas");
  sr               =  check_coalesce_deltas(jks, jdentry, jocrdt, sep, locex,
                                            jauth, gcv, jndentry, ndk);
  ztrace_finish("DELT.ZHB.SEND.DO.CC.DO.CAD.check_coalesce_deltas");
  return sr;
}

// NOTE: PURE ALGORITHM STEP
static sqlres finalize_commit_dentry(jv *jks, jv *jocrdt, jv *jncrdt,
                                     jv *jdelta, bool remove, bool expire,
                                     jv *joplog, bool sep, UInt64 locex,
                                     jv *jauth) {
  LT("finalize_commit_dentry");
  string kqk     = (*jks)["kqk"].asString();
  UInt64 ts      = zh_get_ms_time();
  UInt64 ncnt    = zxact_get_operation_count(jncrdt, joplog,
                                             remove, expire, jdelta);
  sqlres sr      = zxact_reserve_op_id_range(ncnt);
  RETURN_SQL_ERROR(sr);
  UInt64 dvrsn   = sr.ures;
  dvrsn          = zxact_update_crdt_meta_and_added(jncrdt, ncnt, dvrsn, ts);
  zxact_finalize_delta_for_commit(jncrdt, jdelta, dvrsn, ts);
  sqlres srret; // PERFORMANCE OPTIMZATION -> SINGLE COPY JDELTA
  ZH_CREATE_DENTRY(srret, jdelta, kqk)
  return srret;
}

sqlres check_out_of_sync_key(string &kqk) {
  sqlres sr   = fetch_out_of_sync_key(kqk);
  RETURN_SQL_ERROR(sr);
  bool oosync = sr.ures ? true : false;
  if (oosync) {
    LE("COMMIT ON OUT-OF-SYNC K: " << kqk << " -> FAIL");
    RETURN_SQL_RESULT_ERROR(ZS_Errors["CommitOnOutOfSyncKey"])
  }
  RETURN_EMPTY_SQLRES
}

static sqlres do_create_client_store_delta(jv *jks, jv *jjson,
                                           bool sep, UInt64 locex,
                                           jv *jocrdt, jv *jauth) {
  string  kqk                = (*jks)["kqk"].asString();
  sqlres  sr                 = fetch_gc_version(kqk);
  RETURN_SQL_ERROR(sr)
  UInt64  gcv                = sr.ures;
  bool    overwrite          = zjd(jocrdt);
  sr                         = create_crdt_from_json(kqk, jjson);
  RETURN_SQL_ERROR(sr)
  jv      jncrdt             = sr.jres;
  jv      jdelta             = init_first_delta(&jncrdt);
  jv     *jcmeta             = &(jncrdt["_meta"]);
  sr                         = zdelt_get_agent_delta_dependencies(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jdeps              = &(sr.jres);
  zh_init_author(jcmeta, jdeps);
  (*jcmeta)["initial_delta"] = true;
  (*jcmeta)["overwrite"]     = overwrite;
  (*jcmeta)["GC_version"]    = gcv;
  sr                         = persist_key_info_field(kqk, "PRESENT", 1);
  RETURN_SQL_ERROR(sr)
  bool    remove             = false;
  bool    expire             = false;
  sr                         = finalize_commit_dentry(jks, jocrdt, &jncrdt,
                                                      &jdelta, remove, expire,
                                                      NULL, sep, locex, jauth);
  RETURN_SQL_ERROR(sr)
  jv      jdentry            = sr.jres;
  return commit_agent_delta(jks, &jdentry, jocrdt, sep, locex, jauth);
}

static sqlres do_create_client_remove_delta(jv *jks, bool sep, UInt64 locex,
                                            jv *jocrdt, jv *jauth) {
  string kqk = (*jks)["kqk"].asString();
  sqlres sr  = check_out_of_sync_key(kqk);
  RETURN_SQL_ERROR(sr)
  if (zjn(jocrdt)) {
    RETURN_SQL_RESULT_ERROR(ZS_Errors["NoKeyToRemove"]);
  }
  jv   *jcmeta               = &((*jocrdt)["_meta"]);
  sr                         = zdelt_get_agent_delta_dependencies(kqk);
  RETURN_SQL_ERROR(sr)
  jv   *jdeps                = &(sr.jres);
  zh_init_author(jcmeta, jdeps);
  (*jcmeta)["remove"]        = true;
  (*jcmeta)["initial_delta"] = false;
  (*jcmeta)["_"]             = MyUUID;
  jv    jdelta               = zh_init_delta(jcmeta);
  bool  remove               = true;
  bool  expire               = false;
  sr                         = finalize_commit_dentry(jks, jocrdt, jocrdt,
                                                      &jdelta, remove, expire,
                                                      NULL, sep, locex, jauth);
  RETURN_SQL_ERROR(sr)
  jv      jdentry            = sr.jres;
  return commit_agent_delta(jks, &jdentry, jocrdt, sep, locex, jauth);
}

static sqlres do_create_client_expire_delta(jv *jks, UInt64 expiration,
                                            bool sep, UInt64 locex,
                                            jv *jocrdt, jv *jauth) {
  string kqk = (*jks)["kqk"].asString();
  sqlres sr  = check_out_of_sync_key(kqk);
  RETURN_SQL_ERROR(sr)
  if (zjn(jocrdt)) {
    RETURN_SQL_RESULT_ERROR(ZS_Errors["NoKeyToExpire"]);
  }
  jv   *jcmeta               = &((*jocrdt)["_meta"]);
  sr                         = zdelt_get_agent_delta_dependencies(kqk);
  RETURN_SQL_ERROR(sr)
  jv   *jdeps                = &(sr.jres);
  zh_init_author(jcmeta, jdeps);
  (*jcmeta)["expire"]        = true;
  (*jcmeta)["expiration"]    = expiration;
  (*jcmeta)["initial_delta"] = false;
  (*jcmeta)["_"]             = MyUUID;
  jv    jdelta               = zh_init_delta(jcmeta);
  bool  remove               = false;
  bool  expire               = true;
  sr                         = finalize_commit_dentry(jks, jocrdt, jocrdt,
                                                      &jdelta, remove, expire,
                                                      NULL, sep, locex, jauth);
  RETURN_SQL_ERROR(sr)
  jv      jdentry            = sr.jres;
  return commit_agent_delta(jks, &jdentry, jocrdt, sep, locex, jauth);
}

// NOTE: PURE ALGORITHM STEP
static sqlres zdelt_create_commit_dentry(jv *jks, jv *jccrdt, jv *joplog,
                                         bool sep, UInt64 locex, jv *jocrdt,
                                         jv *jauth) {
  LT("zdelt_create_commit_dentry");
  string  kqk    = (*jks)["kqk"].asString();
  jv      jdelta = zoplog_create_delta(jccrdt, joplog);
  if (zh_jv_is_member(&jdelta, "err")) {
    string err = jdelta["err"].asString();
    LD("ERROR: zoplog_create_delta: " << err);
    RETURN_SQL_RESULT_ERROR(err)
  }
  jv     *jcmeta             = &((*jccrdt)["_meta"]);
  sqlres  sr                 = zdelt_get_agent_delta_dependencies(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jdeps              = &(sr.jres);
  zh_init_author(jcmeta, jdeps);
  (*jcmeta)["initial_delta"] = false;
  bool    remove             = false;
  bool    expire             = false;
  return finalize_commit_dentry(jks, jocrdt, jccrdt, &jdelta, remove, expire,
                                joplog, sep, locex, jauth);
}

static sqlres do_create_client_commit_delta(jv *jks, jv *jccrdt, jv *joplog,
                                            bool sep, UInt64 locex, jv *jocrdt,
                                            jv *jauth) {
  string  kqk      = (*jks)["kqk"].asString();
  sqlres  sr       = check_out_of_sync_key(kqk);
  RETURN_SQL_ERROR(sr)
  ztrace_start("DELT.ZHB.SEND.DO.CC.DO.zdelt_create_commit_dentry");
  sr              = zdelt_create_commit_dentry(jks, jccrdt, joplog, sep, locex,
                                               jocrdt, jauth);
  ztrace_finish("DELT.ZHB.SEND.DO.CC.DO.zdelt_create_commit_dentry");
  RETURN_SQL_ERROR(sr)
  jv     *jdentry = &(sr.jres);
  return commit_agent_delta(jks, jdentry, jocrdt, sep, locex, jauth);
}

static sqlres do_process_internal_commit(jv *jks, jv *jdentry, jv *jocrdt,
                                         bool sep, UInt64 locex, jv *jauth) {
  LT("do_process_internal_commit");
  string  kqk = (*jks)["kqk"].asString();
  sqlres  sr  = check_out_of_sync_key(kqk);
  RETURN_SQL_ERROR(sr)
  return commit_agent_delta(jks, jdentry, jocrdt, sep, locex, jauth);
}

static sqlres do_create_client_delta(jv *jks, bool store, bool commit,
                                     bool remove, bool expire, jv *jjson,
                                     jv *jccrdt, jv *joplog, UInt64 expiration,
                                     bool sep, UInt64 locex, jv *jauth) {
  string  kqk    = (*jks)["kqk"].asString();
  sqlres  sr     = zds_retrieve_crdt(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jocrdt = &(sr.jres);
  if (store) {         // STORE
    return do_create_client_store_delta(jks, jjson, sep, locex, jocrdt, jauth);
  } else if (commit) { // COMMIT
    return do_create_client_commit_delta(jks, jccrdt, joplog, sep, locex,
                                         jocrdt, jauth);
  } else if (remove) { // REMOVE
    return do_create_client_remove_delta(jks, sep, locex, jocrdt, jauth);
  } else {             // EXPIRE
    return do_create_client_expire_delta(jks, expiration, sep, locex,
                                         jocrdt, jauth);
  }
}

static sqlres create_client_delta(jv *jks, bool store, bool commit, bool remove,
                                  bool expire, jv *jjson, jv *jccrdt,
                                  jv *joplog, UInt64 expiration, bool sep,
                                  UInt64 locex, jv *jauth) {
  if (store) { // STORE
    return do_create_client_delta(jks, store, commit, remove, expire,
                                  jjson, jccrdt, joplog,
                                  expiration, sep, locex, jauth);
  } else {     // REMOVE/COMMIT/EXPIRE
    string kqk   = (*jks)["kqk"].asString();
    sqlres sr    = fetch_agent_watch_keys(kqk);
    RETURN_SQL_ERROR(sr)
    bool   watch = sr.ures ? true : false;
    if (watch) {
      RETURN_SQL_RESULT_ERROR(ZS_Errors["ModifyWatchKey"]);
    }
    //TODO(V2) SEPARATE
    return do_create_client_delta(jks, store, commit, remove, expire,
                                  jjson, jccrdt, joplog,
                                  expiration, sep, locex, jauth);
  }
}

static jv client_commit(string &id, jv *params, bool internal) {
  LT("client_commit");
  jv     *jauth      = &((*params)["authentication"]);
  jv     *jpd        = &((*params)["data"]);
  jv      jns        = (*params)["data"]["namespace"];
  jv      jcn        = (*params)["data"]["collection"];
  jv      jkey       = (*params)["data"]["key"];
  jv     *jjson      = zh_jv_is_member(jpd, "json") ? &((*jpd)["json"]) : NULL;
  jv     *jccrdt     = zh_jv_is_member(jpd, "crdt") ? &((*jpd)["crdt"]) : NULL;
  if (!internal) zexh_repair_lua_crdt(jccrdt);
  jv     *joplog     = zh_jv_is_member(jpd, "oplog") ? &((*jpd)["oplog"]) :
                                                       NULL;
  string  sjson      = zh_get_string_member(jpd, "sjson");
  bool    store      = zh_get_bool_member(params, "store");
  bool    commit     = zh_get_bool_member(params, "commit");
  bool    remove     = zh_get_bool_member(params, "remove");
  bool    expire     = zh_get_bool_member(params, "expire");
  UInt64  expiration = zh_get_uint64_member(params, "expiration");
  string  kqk        = zs_create_kqk(jns, jcn, jkey);
  jv      jks        = zh_create_ks(kqk, NULL);
  bool    separate   = zh_get_bool_member(jpd, "separate");
  UInt64  locex      = 0;
  UInt64  apid       = (UInt64)getpid(); // AUTHOR PID IS ME
  jv      jsjson;
  if (sjson.size()) {
    jsjson = zh_parse_json_text(sjson, false);
    jjson  = &jsjson;
  }
  ztrace_start("DELT.ZHB.SEND.DO.CC.create_client_delta");
  sqlres  sr = create_client_delta(&jks, store, commit, remove, expire,
                                   jjson, jccrdt, joplog,
                                   expiration, separate, locex, jauth);
  ztrace_finish("DELT.ZHB.SEND.DO.CC.create_client_delta");
  PROCESS_GENERIC_ERROR(id, sr)
  jv      jdata      = sr.jres;
  jv     *jncrdt     = &(jdata["crdt"]);   // NOTE: REPONSE CRDT
  jv     *jdentry    = &(jdata["dentry"]); // NOTE: REPONSE DENTRY
  broadcast_agent_delta_to_worker_caches(kqk, jncrdt, jdentry, apid);
  sr                 = zdelt_get_key_subscriber_versions(kqk);
  PROCESS_GENERIC_ERROR(id, sr)
  jv      mdeps      = sr.jres;
  return respond_client_commit(id, jncrdt, &mdeps);
}

// NOTE: Used by ZHB
jv internal_process_client_store(string &id, jv *params, bool internal) {
  (*params)["store"] = true;
  return client_commit(id, params, internal);
}
jv process_client_store(lua_State *L, string id, jv *params) {
  LT("process_client_store");
  return internal_process_client_store(id, params, false);
}

// NOTE: Used by ZHB
jv internal_process_client_commit(string &id, jv *params, bool internal) {
  (*params)["commit"] = true;
  return client_commit(id, params, internal);
}
jv process_client_commit(lua_State *L, string id, jv *params) {
  LT("process_client_commit");
  return internal_process_client_commit(id, params, false);
}

jv process_client_remove(lua_State *L, string id, jv *params) {
  LT("process_client_remove");
  (*params)["remove"] = true;
  return client_commit(id, params, false);
}

jv process_client_expire(lua_State *L, string id, jv *params) {
  LT("process_client_fetch");
  jv     *jpd    = &((*params)["data"]);
  UInt64  expire = zh_get_uint64_member(jpd, "expire");
  if (!expire) {
    sqlres sr;
    sr.err = ZS_Errors["ExpireNaN"];
    PROCESS_GENERIC_ERROR(id, sr)
    return JNULL; // compiler warning
  } else {
    (*params)["expire"]     = true;
    (*params)["expiration"] = (zh_get_ms_time() / 1000) + expire;
    return client_commit(id, params, false);
  }
}

jv process_internal_commit(lua_State *L, string id, jv *params) {
  LT("process_internal_commit: ID: " << id);
  jv     *jauth      = &((*params)["authentication"]);
  jv     *jpd        = &((*params)["data"]);
  jv      jns        = (*params)["data"]["namespace"];
  jv      jcn        = (*params)["data"]["collection"];
  jv      jkey       = (*params)["data"]["key"];
  jv     *jdentry    = &((*params)["data"]["dentry"]);
  string  kqk        = zs_create_kqk(jns, jcn, jkey);
  jv      jks        = zh_create_ks(kqk, NULL);
  bool    separate   = zh_get_bool_member(jpd, "separate");
  UInt64  locex      = 0;
  UInt64  apid       = zh_get_uint64_member(jpd, "pid");
  sqlres  sr         = zds_retrieve_crdt(kqk);
  if (sr.err.size()) return JNULL;
  jv     *jocrdt     = &(sr.jres);
  sr                 = do_process_internal_commit(&jks, jdentry, jocrdt,
                                                  separate, locex, jauth);
  PROCESS_GENERIC_ERROR(id, sr)
  jv      jdata      = sr.jres;
  jv     *jncrdt     = &(jdata["crdt"]);   // NOTE: REPONSE CRDT
  jv     *jndentry   = &(jdata["dentry"]); // NOTE: REPONSE DENTRY
  broadcast_agent_delta_to_worker_caches(kqk, jncrdt, jndentry, apid);
  sr                 = zdelt_get_key_subscriber_versions(kqk);
  PROCESS_GENERIC_ERROR(id, sr)
  jv      mdeps      = sr.jres;
  return respond_client_commit(id, jncrdt, &mdeps);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOCAL APPLY OPLOG/DENTRY --------------------------------------------------

// NOTE: PURE ALGORITHM STEP
static jv local_apply_dentry(string &kqk, jv *jocrdt, jv *jdentry, bool isad) {
  LT("local_apply_dentry");
  jv *jdelta = &((*jdentry)["delta"]);
  jv *jdmeta = &((*jdelta)["_meta"]);
  //LT("local_apply_dentry: DELTA: " << *jdelta);
  jv  jmerge = zmerge_apply_deltas(jocrdt, jdelta, false);
  if (isad && jmerge["errors"].size() != 0) {
    return JNULL;
  } else if (zh_jv_is_member(&jmerge, "remove")) { // REMOVE(key) operation
    return JNULL;
  } else {                                         // NORMAL APPLY DELTA
    jv *jncrdtd = &(jmerge["crdtd"]);
    jv  jncrdt  = zh_format_crdt_for_storage(kqk, jdmeta, jncrdtd);
    if (!isad) return jncrdt;
    else { // AGENT-DELTA needs [CRDT,DENTRY]
      jv applied;
      applied["crdt"]   = jncrdt;
      applied["dentry"] = *jdentry;
      return applied;
    }
  }
}

// NOTE: PURE ALGORITHM STEP
jv zdelt_local_apply_dentry(const char *skqk, const char *socrdt,
                            const char *sdentry) {
  string kqk     = skqk;
  string c       = socrdt;
  jv     jocrdt  = zh_parse_json_text(c, false);
  zexh_repair_lua_crdt(&jocrdt);
  string d       = sdentry;
  jv     jdentry = zh_parse_json_text(d, false);
  return local_apply_dentry(kqk, &jocrdt, &jdentry, false);
}

// NOTE: PURE ALGORITHM STEP
// NOTE: RETURNS [NEW-CRDT,DENTRY] -> BOTH CACHED BY LUA
jv zdelt_local_apply_oplog(const char *skqk, const char *scrdt,
                           const char *sopcontents,
                           const char *username, const char *password) {

  LT("zdelt_local_apply_oplog");
  jv      jauth       = JOBJECT;
  jauth["username"]   = username;
  jauth["password"]   = password;
  string  kqk         = skqk;
  jv      jjson; // NOTE EMPTY
  string  c           = scrdt;
  jv      jccrdt      = zh_parse_json_text(c, false);
  zexh_repair_lua_crdt(&jccrdt);
  // FROM zdoc.js: zdoc CONSTRUCTOR
  jccrdt["_meta"]["last_member_count"] = jccrdt["_meta"]["member_count"];
  string  o           = sopcontents;
  jv      jopcontents = zh_parse_json_text(o, false);
  jv     *joplog      = &(jopcontents["contents"]);
  zexh_repair_lua_oplog(joplog);
  //LT("zdelt_local_apply_oplog: CRDT: " << jccrdt << " OPLOG: " << *joplog);
  jv      jks         = zh_create_ks(kqk, NULL);
  bool    separate    = false;
  UInt64  locex       = 0;
  sqlres  sc          = zds_retrieve_crdt(kqk);
  if (sc.err.size()) return JNULL;
  jv      joocrdt     = sc.jres;
  jv      jocrdt      = sc.jres; // NOTE: gets modified
  sqlres  sr          = zdelt_create_commit_dentry(&jks, &jccrdt, joplog,
                                                   separate, locex,
                                                   &jocrdt, &jauth);
  if (sr.err.size()) return JNULL;
  jv     *jdentry = &(sr.jres);
  return local_apply_dentry(kqk, &joocrdt, jdentry, true); // NOTE: OOCRDT
}

