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
#include "oplog.h"
#include "deltas.h"
#include "merge.h"
#include "cache.h"
#include "apply_delta.h"
#include "subscriber_delta.h"
#include "ooo_replay.h"
#include "dack.h"
#include "handler.h"
#include "agent_daemon.h"
#include "datastore.h"
#include "storage.h"

using namespace std;

extern Int64 MyUUID;

extern int ChaosMode;
extern vector<string> ChaosDescription;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER COMMIT DELTA ---------------------------------------------------

string zas_get_subscriber_commit_ID(jv *pc) {
  jv     *jks     = &((*pc)["ks"]);
  jv     *jauthor = &((*pc)["extra_data"]["author"]);
  string  kqk     = (*jks)["kqk"].asString();
  string  avrsn   = (*jauthor)["agent_version"].asString();
  return kqk + "-" + avrsn;
}

static sqlres do_subscriber_commit_delta(jv *pc) {
  jv      jks     = (*pc)["ks"];
  jv     *jauthor = &((*pc)["extra_data"]["author"]);
  string  kqk     = jks["kqk"].asString();
  string  avrsn   = (*jauthor)["agent_version"].asString();
  LT("do_subscriber_commit_delta: K: " << kqk << " AV: " << avrsn);
  sqlres  sr      = remove_persisted_subscriber_delta(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  sr              = remove_subscriber_delta_version(kqk, avrsn);
  RETURN_SQL_ERROR(sr)
  sr              = zoor_remove_subscriber_delta(kqk, jauthor);
  RETURN_SQL_ERROR(sr)
  string  ddkey   = zs_get_dirty_subscriber_delta_key(kqk, jauthor);
  return remove_dirty_subscriber_delta(ddkey);
}

sqlres retry_subscriber_commit_delta(jv *pc) {
  sqlres sr = do_subscriber_commit_delta(pc);
  RETURN_SQL_ERROR(sr)
  CHAOS_MODE_ENTRY_POINT(6)
  return sr;
}

static sqlres subscriber_commit_delta(jv *jks, jv *jauthor) {
  LT("subscriber_commit_delta");
  string kqk       = (*jks)["kqk"].asString();
  jv edata;
  edata["author"]  = *jauthor;
  string op        = "SubscriberCommitDelta";
  jv     pc        = zh_init_pre_commit_info(op, jks, NULL, NULL, &edata);
  string udid = zas_get_subscriber_commit_ID(&pc);
  sqlres sr   = persist_fixlog(udid, &pc); // -------- START FIXLOG ----------|
  RETURN_SQL_ERROR(sr)                     //                                 |
  sr          = retry_subscriber_commit_delta(&pc);//  RETRY                  |
  ACTIVATE_FIXLOG_RETURN_SQL_ERROR(sr)     //                                 |
  return remove_fixlog(udid);              // -------- END FIXLOG ------------|
}

sqlres zas_do_commit_delta(jv *jks, jv *jauthor) {
  UInt64 auuid = (*jauthor)["agent_uuid"].asUInt64();
  if ((int)auuid == MyUUID) {
    sqlres sr = zdack_do_flow_remove_agent_delta(jks, jauthor);
    RETURN_SQL_ERROR(sr)
  } else {
    sqlres sr = subscriber_commit_delta(jks, jauthor);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

sqlres zas_post_apply_delta_conditional_commit(jv *jks, jv *jdentry) {
  LT("zas_post_apply_delta_conditional_commit");
  string  kqk       = (*jks)["kqk"].asString();
  jv     *jauthor   = &((*jdentry)["delta"]["_meta"]["author"]);
  sqlres  sr        = fetch_delta_committed(kqk, jauthor);
  RETURN_SQL_ERROR(sr)
  bool    committed = sr.ures ? true : false;
  if (!committed) RETURN_EMPTY_SQLRES
  else            return zas_do_commit_delta(jks, jauthor);
}

static sqlres subscriber_commit_later(string &kqk, jv *jauthor,
                                      const char *reason) {
  string avrsn = (*jauthor)["agent_version"].asString();
  LE("SD-COMMIT-DELTA " << reason << ": K: " << kqk << " AV: " << avrsn);
  return persist_delta_committed(kqk, jauthor);
}

sqlres zas_check_flow_subscriber_commit_delta(jv *jks, jv *jauthor) {
  string kqk   = (*jks)["kqk"].asString();
  string avrsn = (*jauthor)["agent_version"].asString();
  LD("zas_check_flow_subscriber_commit_delta: K: " << kqk << " AV: " << avrsn);
  sqlres sr    = fetch_to_sync_key(kqk);
  RETURN_SQL_ERROR(sr)
  bool tosync = sr.sres.size() ? true : false;
  if (tosync) return subscriber_commit_later(kqk, jauthor, "ON TO-SYNC-KEY");
  else {
    sqlres sr   = fetch_delta_need_reference(kqk, jauthor);
    RETURN_SQL_ERROR(sr)
    bool   nref = zjd(&(sr.jres));
    if (nref) {
      return subscriber_commit_later(kqk, jauthor, "DURING AGENT-GC-WAIT");
    } else {
      sqlres sr    = fetch_ooo_delta(kqk, jauthor);
      RETURN_SQL_ERROR(sr)
      bool   oooav = sr.ures ? true : false;
      if (oooav) return subscriber_commit_later(kqk, jauthor, "ON OOO-SD");
      else {
        sqlres  sr      = zsd_fetch_subscriber_delta(kqk, jauthor);
        RETURN_SQL_ERROR(sr)
        jv     *jdentry = &(sr.jres);
        if (zjn(jdentry)) {
          return subscriber_commit_later(kqk, jauthor, "DELTA NOT ARRIVED");
        } else {
          sqlres sr    = fetch_agent_delta_gcv_needs_reorder(kqk, avrsn);
          RETURN_SQL_ERROR(sr)
          bool   needs = sr.ures ? true : false;
          if (needs) {
            return subscriber_commit_later(kqk, jauthor, "KEY-GCV-NREO");
          } else {
            return zas_do_commit_delta(jks, jauthor);
          }
        }
      }
    }
  }
}

jv process_subscriber_commit_delta(lua_State *L, string id, jv *params) {
  jv     *jks     = &((*params)["data"]["ks"]);
  jv     *jauthor = &((*params)["data"]["author"]);
  string  kqk     = (*jks)["kqk"].asString();
  LD("process_subscriber_commit_delta: K: " << kqk);
  sqlres sr       = zas_check_flow_subscriber_commit_delta(jks, jauthor);
  PROCESS_GENERIC_ERROR(id, sr)
  return zh_create_jv_response_ok(id);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SYNC CHANNEL --------------------------------------------------------

sqlres zas_set_agent_sync_key(string &kqk, string &sectok) {
  LE("zas_set_agent_sync_key: K: " << kqk);
  return persist_to_sync_key(kqk, sectok);
}

sqlres zas_set_agent_sync_key_signal_agent_to_sync_keys(jv *jks) {
  string kqk    = (*jks)["kqk"].asString();
  string sectok = (*jks)["security_token"].asString();
  sqlres sr     = zas_set_agent_sync_key(kqk, sectok);
  RETURN_SQL_ERROR(sr)
  zadaem_local_signal_to_sync_keys_daemon(); // KQK-WORKER
  RETURN_EMPTY_SQLRES
}

static sqlres do_sync_missing_key(string &kqk, string &sectok) {
  sqlres sr   = fetch_lru_key_field(kqk, "WHEN");
  RETURN_SQL_ERROR(sr)
  UInt64 when = sr.ures;
  if (when) { // ALREADY PRESENT
    LD("already synced: K: " << kqk << " -> NO-OP");
    RETURN_EMPTY_SQLRES
  } else {    // MISSING -> SYNC it from Central
    return zas_set_agent_sync_key(kqk, sectok);
  }
}

// NOTE: KQK-WORKER
sqlres zas_sync_missing_keys(jv *skss, bool issub) {
  vector<string> mbrs = zh_jv_get_member_names(skss);
  if (mbrs.size() == 0) RETURN_EMPTY_SQLRES
  for (uint i = 0; i < mbrs.size(); i++) {
    string  kqk    = mbrs[i];
    if (!zh_is_key_local_to_worker(kqk)) continue;
    jv     *jks    = &((*skss)[kqk]);
    string  sectok = (*jks)["security_token"].asString();
    if (!issub) { // STATION PKSS[]
      sqlres sr = do_sync_missing_key(kqk, sectok);
      RETURN_SQL_ERROR(sr)
    } else {
      sqlres sr     = fetch_cached_keys(kqk);
      RETURN_SQL_ERROR(sr)
      bool   cached = sr.ures ? true : false;
      if (!cached) {
        sqlres sr = do_sync_missing_key(kqk, sectok);
        RETURN_SQL_ERROR(sr)
      } else {
        LD("sync_missing_key: K: " << kqk << " -> EVICT");
        bool   send_central = true;
        bool   need_merge   = true;
        sqlres sr           = zcache_internal_evict(jks, send_central,
                                                    need_merge);
        RETURN_SQL_ERROR(sr)
      }
    }
  }
  zadaem_local_signal_to_sync_keys_daemon(); // KQK-WORKER
  RETURN_EMPTY_SQLRES
}

sqlres zas_agent_sync_channel_kss(jv *pkss, bool issub) {
  LT("zas_agent_sync_channel_kss");
  jv skss = JOBJECT;
  for (uint i = 0; i < pkss->size(); i++) {
    jv     *jks = &((*pkss)[i]);
    string  kqk = (*jks)["kqk"].asString();
    skss[kqk]   = *jks;
  }
  jv rkss = JARRAY;
  if (!broadcast_workers_settings(&skss, &rkss, false, issub)) {
    RETURN_SQL_RESULT_ERROR("broadcast_workers_settings");
  }
  RETURN_EMPTY_SQLRES
}

