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
#include "xact.h"
#include "oplog.h"
#include "deltas.h"
#include "apply_delta.h"
#include "agent_daemon.h"
#include "activesync.h"
#include "heartbeat.h"
#include "datastore.h"
#include "storage.h"

using namespace std;

extern Int64 MyUUID;
extern string MyDataCenterUUID;

extern int ChaosMode;
extern vector<string> ChaosDescription;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static sqlres do_key_to_sync(jv *jks) {
  string kqk = (*jks)["kqk"].asString();
  LD("ZDack.do_key_to_sync: K: " << kqk);
  return zas_set_agent_sync_key_signal_agent_to_sync_keys(jks);
}

static sqlres set_agent_key_out_of_sync(jv *jks) {
  sqlres sr;
  string kqk = (*jks)["kqk"].asString();
  LD("ZDack.set_agent_key_out_of_sync: K: " << kqk);
  sr         = persist_out_of_sync_key(kqk);
  RETURN_SQL_ERROR(sr)
  sr         = do_key_to_sync(jks);
  RETURN_SQL_ERROR(sr)
  RETURN_SQL_RESULT_ERROR(ZS_Errors["DeltaNotSync"])
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EMPTY DELTA ---------------------------------------------------------------

static sqlres save_empty_agent_delta(string kqk, jv *jauth, UInt64 avnum,
                                     jv *jdentry) {
  sqlres sr;
  LE("save_empty_agent_delta: K: " << kqk);
  string avrsn = zh_create_avrsn(MyUUID, avnum);
  sr           = persist_subscriber_delta_version(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  sr           = zds_store_agent_delta(kqk, jdentry, jauth);
  RETURN_SQL_ERROR(sr)
  string ddkey = zs_get_dirty_subscriber_delta_key(kqk, MyUUID, avrsn);
  return persist_dirty_delta(ddkey);
}

static void cleanup_empty_delta(jv *jdentry) {
  (*jdentry)["delta"]["_meta"].removeMember("author");
  (*jdentry)["delta"]["_meta"].removeMember("is_geo");
}

static sqlres build_create_empty_delta(string &kqk,    UInt64  auuid,
                                       string &cavrsn, jv     *jocrdt,
                                       jv     *jdelta, UInt64  dvrsn) {
  UInt64  ts      = zh_get_ms_time();
  UInt64  ncnt    = 1;
  dvrsn           = zxact_update_crdt_meta_and_added(jocrdt, ncnt, dvrsn, ts);
  zxact_finalize_delta_for_commit(jocrdt, jdelta, dvrsn, ts);
  jv      jdentry = zh_create_dentry(jdelta, kqk);
  cleanup_empty_delta(&jdentry);
  jdentry["_"]                         = auuid;
  jv     *jdmeta                       = &(jdentry["delta"]["_meta"]);
  (*jdmeta)["initial_delta"]           = false;
  (*jdmeta)["author"]["agent_uuid"]    = auuid;
  (*jdmeta)["author"]["agent_version"] = cavrsn;
  zh_debug_json_value("jdentry", &jdentry);
  RETURN_SQL_RESULT_JSON(jdentry)
}

static sqlres create_empty_delta(string kqk, UInt64 auuid, UInt64 cavnum) {
  string  cavrsn  = zh_create_avrsn(auuid, cavnum);
  LE("empty_delta: K: " << kqk << " AV: " << cavrsn);
  sqlres  sc;
  FETCH_CRDT(kqk);
  RETURN_SQL_ERROR(sc)
  jv     *jocrdt  = &(sc.jres);
  jv      joplog;
  jv      jdelta  = zoplog_create_delta(jocrdt, &joplog);
  UInt64  ncnt    = 1;
  jdelta["_meta"]["op_count"] = ncnt;
  sqlres  sr      = zxact_reserve_op_id_range(ncnt);
  RETURN_SQL_ERROR(sr)
  UInt64  dvrsn   = sr.ures;
  return build_create_empty_delta(kqk, auuid, cavrsn, jocrdt, &jdelta, dvrsn);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ACK AGENT DELTA -----------------------------------------------------------

static string get_remove_agent_delta_ID(jv pc) {
  jv     jks    = pc["ks"];
  jv     javrsn = pc["extra_data"]["author"]["agent_version"];
  string kqk    = jks["kqk"].asString();
  string avrsn  = javrsn.asString();
  return kqk + "-" + avrsn;
}

static sqlres remove_dirty_key(jv *pc) {
  string kqk   = (*pc)["ks"]["kqk"].asString();
  string avrsn = (*pc)["extra_data"]["author"]["agent_version"].asString();
  UInt64 ndk   = (*pc)["metadata"]["dirty_key"].asUInt64() - 1;
  sqlres sr    = persist_agent_dirty_keys(kqk, ndk);
  RETURN_SQL_ERROR(sr)
  CHAOS_MODE_ENTRY_POINT(5)
  string ddkey = zs_get_dirty_agent_delta_key(kqk, avrsn);
  sr           = remove_dirty_subscriber_delta(ddkey);
  RETURN_SQL_ERROR(sr)
  RETURN_EMPTY_SQLRES
}

static sqlres __remove_agent_delta(jv *pc) {
  string kqk   = (*pc)["ks"]["kqk"].asString();
  string avrsn = (*pc)["extra_data"]["author"]["agent_version"].asString();
  sqlres sr    = remove_subscriber_delta_version(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  sr           = zds_remove_agent_delta(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  return remove_dirty_key(pc);
}

static sqlres do_remove_agent_delta(jv *pc) {
  string kqk   = (*pc)["ks"]["kqk"].asString();
  string avrsn = (*pc)["extra_data"]["author"]["agent_version"].asString();
  LD("ZDack.do_remove_agent_delta: K: " << kqk << " AV: " << avrsn);
  sqlres sr    = __remove_agent_delta(pc);
  RETURN_SQL_ERROR(sr)
  return remove_agent_sent_delta(kqk, avrsn);
}

sqlres retry_remove_agent_delta(jv *pc) {
  return do_remove_agent_delta(pc);
}

sqlres zdack_do_flow_remove_agent_delta(jv *jks, jv *jauthor) {
  LT("zdack_do_flow_remove_agent_delta");
  sqlres sr;
  string kqk      = (*jks)["kqk"].asString();
  jv edata;
  edata["author"] = *jauthor;
  string op       = "RemoveAgentDelta";
  jv     pc       = zh_init_pre_commit_info(op, jks, NULL, NULL, &edata);
  sr              = fetch_agent_dirty_keys(kqk);
  RETURN_SQL_ERROR(sr)
  UInt64 ndk      = sr.ures;
  pc["metadata"]["dirty_key"] = (ndk == 0) ? 1 : ndk;
  string udid = get_remove_agent_delta_ID(pc);
  sr          = persist_fixlog(udid, &pc);     // ---- START FIXLOG ----------|
  RETURN_SQL_ERROR(sr)                         //                             |
  sr          = retry_remove_agent_delta(&pc); //         RETRY               |
  ACTIVATE_FIXLOG_RETURN_SQL_ERROR(sr)         //                             |
  return remove_fixlog(udid);                  // ---- END FIXLOG ------------|
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DELTA ERROR ACKS ----------------------------------------------------

static void drain_key_delta(string kqk) {
  LD("drain_key_delta: K: " << kqk);
  zadaem_get_agent_key_drain(kqk, true);
}

static sqlres handle_server_rejected_delta(jv *jks, string cavrsn, jv *jauth) {
  string kqk     = (*jks)["kqk"].asString();
  UInt64 cavnum  = zh_get_avnum(cavrsn);
  LD("handle_server_rejected_delta: AV: " << cavnum);
  sqlres sr      = create_empty_delta(kqk, MyUUID, cavnum);
  RETURN_SQL_ERROR(sr)
  jv     jdentry = sr.jres;
  sr             = save_empty_agent_delta(kqk, jauth, cavnum, &jdentry);
  RETURN_SQL_ERROR(sr)
  sr             = do_key_to_sync(jks);
  RETURN_SQL_ERROR(sr)
  drain_key_delta(kqk);
  RETURN_EMPTY_SQLRES
}

sqlres zdack_handle_ooo_agent_delta_ack(jv *jks) {
  string kqk = (*jks)["kqk"].asString();
  LD("AckAgentDelta: OOO-DELTA: K: " << kqk);
  drain_key_delta(kqk);
  RETURN_EMPTY_SQLRES
}

jv process_agent_delta_error(lua_State *L, string id, jv *params) {
  LT("process_agent_delta_error");
  Int64  ecode = (*params)["error"]["code"].asInt64();
  string emsg  = (*params)["error"]["message"].asString();
  if (ecode != -32006) {
    LE("-ERROR: AgentDelta-ACK is MALFORMED: code: " << ecode <<
       " message: " << emsg);
  } else {
    if        (!emsg.compare(ZS_Errors["RepeatDelta"])) {
      LD("RepeatDelta");
      jv     *jdetails = &((*params)["error"]["details"]);
      jv     *jks      = &((*jdetails)["ks"]);
      jv     *jauthor  = &((*jdetails)["author"]);
      sqlres  sr       = zdack_do_flow_remove_agent_delta(jks, jauthor);
      PROCESS_GENERIC_ERROR(id, sr)
    } else if (!emsg.compare(ZS_Errors["DeltaNotSync"])) {
      LD("DeltaNotSync");
      jv     *jdetails = &((*params)["error"]["details"]);
      jv     *jks      = &((*jdetails)["ks"]);
      jv     *jauthor  = &((*jdetails)["author"]);
      sqlres  sr       = zdack_do_flow_remove_agent_delta(jks, jauthor);
      PROCESS_GENERIC_ERROR(id, sr)
      sr               = set_agent_key_out_of_sync(jks);
      PROCESS_GENERIC_ERROR(id, sr)
    } else if (!emsg.compare(ZS_Errors["OutOfOrderDelta"])) {
      LD("OutOfOrderDelta");
      jv     *jdetails = &((*params)["error"]["details"]);
      jv     *jks      = &((*jdetails)["ks"]);
      sqlres  sr       = zdack_handle_ooo_agent_delta_ack(jks);
      PROCESS_GENERIC_ERROR(id, sr)
    } else if (!emsg.compare(ZS_Errors["ServerFilterFailed"]) ||
               !emsg.compare(ZS_Errors["WritePermsFail"])) {
      jv     *jdetails = &((*params)["error"]["details"]);
      jv     *jks      = &((*jdetails)["ks"]);
      string  cavrsn   = (*jdetails)["central_agent_version"].asString();
      jv     *jauth    = &((*jdetails)["authorization"]);
      sqlres  sr       = handle_server_rejected_delta(jks, cavrsn, jauth);
      // TODO(V2) ZS.EngineCallback.DeltaFailure()
      PROCESS_GENERIC_ERROR(id, sr)
    } else {
      LE("Unhandled AckAgentDeltaError: ecode(-32006)");
      jv *error = &((*params)["error"]);
      zh_debug_json_value("error", error);
    }
  }
  return zh_create_jv_response_ok(id);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL-VERSION GET/SET ---------------------------------------------------

sqlres zdack_get_agent_last_created() {
  if (MyDataCenterUUID.size() == 0) RETURN_EMPTY_SQLRES
  else {
    return fetch_agent_created(MyDataCenterUUID);
  }
}

sqlres zdack_set_agent_createds(jv *jcreateds) {
  UInt64  nc         = jcreateds->size();
  if (!nc) RETURN_EMPTY_SQLRES
  sqlres  sr         = fetch_all_agent_createds();
  RETURN_SQL_ERROR(sr)
  jv     *jlcreateds = &(sr.jres);
  if (zjn(jlcreateds)) return persist_agent_createds(jcreateds);
  else {
    zh_type_object_assert(jlcreateds, "LOGIC(zdack_set_agent_createds)");
    for (jv::iterator it = jlcreateds->begin(); it != jlcreateds->end(); it++) {
      jv     jguuid    = it.key();
      string guuid     = jguuid.asString();
      jv     jlcreated = (*it);
      UInt64 lcreated  = jlcreated.asUInt64();
      jv     jcreated  = zh_jv_is_member(jcreateds, guuid) ?
                                                   (*jcreateds)[guuid] : JNULL;
      UInt64 created   = zjn(&jcreated) ? 0 : jcreated.asUInt64();
      if (created && created > lcreated) *it = jcreated;
    }
    zh_type_object_assert(jcreateds, "LOGIC(zdack_set_agent_createds(2))");
    for (jv::iterator it = jcreateds->begin(); it != jcreateds->end(); it++) {
      jv     jguuid    = it.key();
      string guuid     = jguuid.asString();
      jv     jcreated  = (*it);
      jv     jlcreated = zh_jv_is_member(jlcreateds, guuid) ?
                                                   (*jlcreateds)[guuid] : JNULL;
      if (zjn(&jlcreated)) (*jlcreateds)[guuid] = jcreated;
    }
    return persist_agent_createds(jlcreateds);
  }
}

