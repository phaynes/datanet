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
#include "deltas.h"
#include "subscriber_delta.h"
#include "ooo_replay.h"
#include "dack.h"
#include "activesync.h"
#include "agent_daemon.h"
#include "handler.h"
#include "aio.h"
#include "datastore.h"
#include "storage.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNS -------------------------------------------------------------------

extern lua_State *GL;

extern Int64 MyUUID;

extern UInt64 AgentDirtyDeltaMaximumAllowedStaleness;

extern UInt64 MaxDataCenterUUID;
extern bool   DebugDaemons;
extern jv     zh_nobody_auth;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

map<string, UInt64> KeyDrainMap;

void zadaem_reset_key_drain_map() {
  LT("zadaem_reset_key_drain_map");
  KeyDrainMap.clear();
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG OVERRIDES -----------------------------------------------------------

#define DISABLE_LT
#ifdef DISABLE_LT
  #undef LT
  #define LT(text) /* text */
#endif


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

uint KeyDrainTimeout     = 1000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FORARD DECLARATIONS -------------------------------------------------------

static jv create_authors_from_avrsns(jv *javrsns);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// TO_SYNC_KEYS DAEMON -------------------------------------------------------

static sqlres send_central_need_merge(string &kqk, string &sectok,
                                      UInt64 gcv, UInt64 nd) {
  LD("NeedMerge: K: " << kqk << " SEC: " << sectok);
  jv     prequest = zh_create_json_rpc_body("AgentNeedMerge", &zh_nobody_auth);
  string id       = prequest["id"].asString();
  LD("persist_need_merge_request_id: K: " << kqk << " ID: " << id);
  sqlres sr       = persist_need_merge_request_id(kqk, id);
  RETURN_SQL_ERROR(sr)
  prequest["params"]["data"]["ks"]         = zh_create_ks(kqk, sectok.c_str());
  prequest["params"]["data"]["gc_version"] = gcv;
  prequest["params"]["data"]["num_deltas"] = nd;
  if (zexh_send_central_https_request(0, prequest, false)) RETURN_EMPTY_SQLRES
  else {
    string err = "FAIL: send_central_need_merge";
    RETURN_SQL_RESULT_ERROR(err)
  }
}

static sqlres fetch_dentries(string &kqk, jv *jauthors) {
  jv jdentries = JARRAY;
  for (uint i = 0; i < jauthors->size(); i++) {
    jv     *jauthor = &((*jauthors)[i]);
    sqlres  sr       = zsd_fetch_subscriber_delta(kqk, jauthor);
    RETURN_SQL_ERROR(sr)
    jv     *jdentry = &(sr.jres);
    if (zjd(jdentry)) zh_jv_append(&jdentries, jdentry);
  }
  RETURN_SQL_RESULT_JSON(jdentries)
}

static sqlres fetch_simple_agent_need_merge_metadata(string &kqk) {
  LT("fetch_simple_agent_need_merge_metadata");
  sqlres sr         = fetch_gc_version(kqk);
  RETURN_SQL_ERROR(sr)
  UInt64 gcv        = sr.ures;
  jv     md         = JOBJECT;
  md["gcv"]         = gcv;
  md ["num_deltas"] = 0;
  RETURN_SQL_RESULT_JSON(md)
}

static sqlres fetch_agent_need_merge_metadata(string &kqk) {
  sqlres  sr      = fetch_simple_agent_need_merge_metadata(kqk);
  RETURN_SQL_ERROR(sr)
  jv      md      = sr.jres;
  sr              = fetch_subscriber_delta_aversions(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *javrsns = &(sr.jres);
  if (javrsns->size() == 0) RETURN_SQL_RESULT_JSON(md)
  else {
    jv      jauthors = create_authors_from_avrsns(javrsns);
    sqlres  sr       = fetch_dentries(kqk, &jauthors);
    RETURN_SQL_ERROR(sr)
    jv     *jdentries = &(sr.jres);
    UInt64  min_gcv   = md["gcv"].asUInt64();
    for (uint i = 0; i < jdentries->size(); i++) {
      jv     *jdentry = &((*jdentries)[i]);
      UInt64  dgcv    = zh_get_dentry_gcversion(jdentry);
      if (dgcv < min_gcv) min_gcv = dgcv;
    }
    LD("fetch_agent_need_merge_metadata: MIN-GCV: " << min_gcv);
    md["gcv"]         = min_gcv;
    md ["num_deltas"] = jdentries->size();
    RETURN_SQL_RESULT_JSON(md)
  }
}

static sqlres zadaem_need_merge(string &kqk, string &sectok) {
  sqlres  sr     = fetch_agent_need_merge_metadata(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *md     = &(sr.jres);
  UInt64  sm_gcv = ((*md)["gcv"].asUInt64() + 1); // NEED NEXT GCV
  UInt64  nd     = (*md)["nd"].asUInt64();
  return send_central_need_merge(kqk, sectok, sm_gcv, nd);
}

static void do_to_sync_keys_daemon() {
  if (DebugDaemons) LD("START: do_to_sync_keys_daemon");
  sqlres sr      = zh_get_agent_offline();
  if (sr.err.size()) return;
  bool   offline = sr.ures ? true : false;
  if (!offline) {
    sqlres  sr    = fetch_all_to_sync_keys();
    jv     *jkqks = &(sr.jres);
    if (zjn(jkqks)) {
      if (DebugDaemons) LD("AGENT_TOSYNC_KEYS: ZERO KEYS");
    } else {
      jv  loc_kqks = JOBJECT;
      zh_type_object_assert(jkqks, "LOGIC(do_to_sync_keys_daemon)");
      for (jv::iterator it = jkqks->begin(); it != jkqks->end(); it++) {
        jv     jkqk = it.key();
        string kqk  = jkqk.asString();
        if (zh_is_key_local_to_worker(kqk)) {
          loc_kqks[kqk] = *it;
        }
      }
      UInt64 ntsk  = loc_kqks.size();
      if (DebugDaemons) LD("AGENT_TOSYNC_KEYS: #Ks: " << ntsk);
      zh_type_object_assert(&loc_kqks, "LOGIC(do_to_sync_keys_daemon(2))");
      for (jv::iterator it = loc_kqks.begin(); it != loc_kqks.end(); it++) {
        jv     jkqk    = it.key();
        string kqk     = jkqk.asString();
        jv     jsectok = (*it);
        string sectok  = jsectok.asString();
        sqlres sr      = zadaem_need_merge(kqk, sectok);
        if (sr.err.size()) {
          LE("ERROR: zadaem_need_merge: " << sr.err);
          return;
        }
      }
    }
  }
  if (DebugDaemons) LD("FINISH: do_to_sync_keys_daemon");
}

static bool ToSyncKeysDaemonLock = false;

void zadaem_attempt_do_to_sync_keys_daemon() {
  if (DebugDaemons) LT("zadaem_attempt_do_to_sync_keys_daemon");
  if (ToSyncKeysDaemonLock) {
    LD("DAEMON LOCKED: AGENT_TOSYNC_KEYS");
    return;
  } else {
    ToSyncKeysDaemonLock = true;
    do_to_sync_keys_daemon();
    ToSyncKeysDaemonLock = false;
  }
}

// NOTE: KQK-WORKER call
void zadaem_local_signal_to_sync_keys_daemon() {
  LT("zadaem_local_signal_to_sync_keys_daemon");
  zadaem_attempt_do_to_sync_keys_daemon();
}

jv process_internal_to_sync_keys_daemon(lua_State *L, string id, jv *params) {
  LT("process_internal_to_sync_keys_daemon");
  zadaem_attempt_do_to_sync_keys_daemon();
  return zh_create_jv_response_ok(id);
}

// NOTE: ALL-WORKERS
bool zadaem_signal_to_sync_keys_daemon() {
  LT("zadaem_signal_to_sync_keys_daemon");
  jv prequest = zh_create_json_rpc_body("InternalRunToSyncKeysDaemon",
                                        &zh_nobody_auth);
  return zexh_broadcast_internal_request(prequest, 0);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DIRTY DELTA REAPER DRAIN ------------------------------------------

static jv create_agent_dentries_request(string &kqk, jv *jauthdentries) {
  LT("create_agent_dentries_request");
  jv dentries = JARRAY;
  for (uint i = 0; i < jauthdentries->size(); i++) {
    jv     *jad              = &((*jauthdentries)[i]);
    jv      jdentry          = (*jad)["dentry"];
    jv      cdentry          = jdentry; // NOTE: gets modified
    zdelt_cleanup_agent_delta_before_send(&cdentry);
    // NOTE AuthorDeltas send AUTHORIZATION via sd.auth
    cdentry["authorization"] = (*jad)["auth"];
    zaio_cleanup_delta_before_send_central(&cdentry);
    zh_jv_append(&dentries, &cdentry);
  }
  jv prequest = zh_create_json_rpc_body("AgentDentries", &zh_nobody_auth);
  prequest["params"]["data"]["ks"]       = zh_create_ks(kqk, NULL);
  prequest["params"]["data"]["dentries"] = dentries;
  return prequest;
}

// NOTE: no SET_TIMER() on TOO SOON failure
//       | -> next run AgentDirtyDeltaReaper is enough
static bool send_agent_dentries(string &kqk, jv *jauthdentries) {
  LT("send_agent_dentries K: " << kqk << " #D: " << jauthdentries->size());
  if (jauthdentries->size() == 0) return true;
  UInt64 now = zh_get_ms_time();
  map<string, UInt64>::iterator it = KeyDrainMap.find(kqk);
  if (it != KeyDrainMap.end()) {
    UInt64 moo_drain = it->second;
    UInt64 diff      = now - moo_drain;
    if (diff < KeyDrainTimeout) {
      LD("MISSING-OOO-AGENT-DELTA-DRAIN: TOO SOON: diff:" << diff);
      return true;
    }
  }
  KeyDrainMap[kqk] = now;
  jv prequest      = create_agent_dentries_request(kqk, jauthdentries);
  return zexh_send_central_https_request(0, prequest, true);
}

static sqlres add_auth_to_dentries(string &kqk, jv *jdentries) {
  LT("add_auth_to_dentries");
  jv jauthdentries = JARRAY;
  for (uint i = 0; i < jdentries->size(); i++) {
    jv     *jdentry  = &((*jdentries)[i]);
    jv     *jdmeta   = &((*jdentry)["delta"]["_meta"]);
    string  avrsn    = (*jdmeta)["author"]["agent_version"].asString();
    sqlres  sr       = fetch_agent_delta_auth(kqk, avrsn);
    RETURN_SQL_ERROR(sr)
    jv      aentry;
    aentry["auth"]   = sr.jres;
    aentry["dentry"] = *jdentry;
    zh_jv_append(&jauthdentries, &aentry);
  }
  RETURN_SQL_RESULT_JSON(jauthdentries)
}

static sqlres drain_key_versions(string &kqk, jv *jauthors, bool internal) {
  LT("drain_key_versions");
  if (!jauthors->size()) RETURN_EMPTY_SQLRES
  else {
    zh_remove_repeat_author(jauthors);
    sqlres sr             = zoor_dependencies_sort_authors(kqk, true, jauthors);
    RETURN_SQL_ERROR(sr)
    jv     jdentries      = sr.jres;
    sr                    = add_auth_to_dentries(kqk, &jdentries);
    RETURN_SQL_ERROR(sr)
    jv     *jauthdentries = &(sr.jres);
    if (!send_agent_dentries(kqk, jauthdentries)) {
      RETURN_SQL_RESULT_ERROR("send_agent_dentries");
    }
    RETURN_EMPTY_SQLRES
  }
}

static jv create_authors_from_avrsns(jv *javrsns) {
  LT("create_authors_from_avrsns");
  jv jauthors = JARRAY;
  for (uint i = 0; i < javrsns->size(); i++) {
    string avrsn             = (*javrsns)[i].asString();
    UInt64 auuid             = zh_get_avuuid(avrsn);
    jv     jauthor;
    jauthor["agent_uuid"]    = auuid;
    jauthor["agent_version"] = avrsn;
    zh_jv_append(&jauthors, &jauthor);
  }
  return jauthors;
}

static sqlres get_agent_key_versions(string &kqk) {
  LT("get_agent_key_versions");
  sqlres sr        = fetch_subscriber_delta_aversions(kqk);
  RETURN_SQL_ERROR(sr)
  jv     javrsns    = sr.jres;
  sr                = fetch_ooo_key_delta_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv     joooavrsns = sr.jres;
  for (uint i = 0; i < joooavrsns.size(); i++) {
    zh_jv_append(&javrsns, &(joooavrsns[i]));
  }
  jv     jauthors   = create_authors_from_avrsns(&javrsns);
  RETURN_SQL_RESULT_JSON(jauthors)
}

// NOTE: DO NOT DRAIN KEYS OUT-OF-SYNC (TO-SYNC is OK)
static sqlres check_key_sync(string &kqk) {
  jv     md;
  sqlres sr     = zsd_get_agent_sync_status(kqk, &md);
  RETURN_SQL_ERROR(sr)
  bool   oosync = zh_get_bool_member(&md, "out_of_sync_key");
  sr.ures       = (!oosync);
  return sr;
}

sqlres zadaem_get_agent_key_drain(string &kqk, bool internal) {
  LD("zadaem_get_agent_key_drain: K: " << kqk << " INTERNAL: " << internal);
  sqlres sr = check_key_sync(kqk);
  RETURN_SQL_ERROR(sr)
  bool   ok = sr.ures;
  if (!ok) { // TO-SYNC or OUT-OF-SYNC
    LD("FetchAgentKeyDrain: K: " << kqk << " KEY NOT-IN-SYNC -> NO-OP");
    RETURN_EMPTY_SQLRES
  } else {
    sqlres  sr       = get_agent_key_versions(kqk);
    RETURN_SQL_ERROR(sr)
    jv     *jauthors = &(sr.jres);
    LD("FetchAgentKeyDrain: K: " << kqk << " DELTAS: " << *jauthors);
    return drain_key_versions(kqk, jauthors, internal);
  }
}

static sqlres get_agent_keys_drain(vector<string> kss) {
  LD("get_agent_keys_drain: #Ks: " << kss.size());
  for (uint i = 0; i < kss.size(); i++) {
    string kqk = kss[i];
    sqlres sr  = zadaem_get_agent_key_drain(kqk, false);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres run_agent_dirty_deltas_reaper() {
  sqlres sr = fetch_all_dirty_deltas();
  RETURN_SQL_ERROR(sr)
  if (zjn(&sr.jres)) {
    if (DebugDaemons) { LD("AGENT_DIRTY_DELTAS: ZERO DELTAS"); }
    RETURN_EMPTY_SQLRES
  }
  jv     *dmap = &(sr.jres);
  UInt64  now  = zh_get_ms_time();
  UInt64  old  = now - (AgentDirtyDeltaMaximumAllowedStaleness * 1000);
  vector<string> dvals;
  zh_type_object_assert(dmap, "LOGIC(run_agent_dirty_deltas_reaper)");
  for (jv::iterator it = dmap->begin(); it != dmap->end(); it++) {
    jv     jdval = it.key();
    string dval  = jdval.asString();
    jv     jts   = (*it);
    UInt64 ts    = jts.asUInt64();
    bool   ok    = (ts < old);
    if (ok) dvals.push_back(dval);
  }
  if (!dvals.size()) {
    if (DebugDaemons) {
      LD("AGENT_DIRTY_DELTAS: ZERO DIRTY_DELTAS");
    }
    return sr;
  }
  map<string, bool> ukss;
  for (uint i = 0; i < dvals.size(); i++) {
    string         dval = dvals[i];
    vector<string> res  = split(dval, '-');
    string         kqk  = res[0];
    if (zh_is_key_local_to_worker(kqk)) {
      ukss.insert(make_pair(kqk, true));
    }
  }
  vector<string> kss;
  for (map<string, bool>::iterator it = ukss.begin(); it != ukss.end(); it++) {
    kss.push_back(it->first);
  }
  return get_agent_keys_drain(kss);
}

void zadaem_attempt_agent_dirty_deltas_reaper() {
  if (DebugDaemons) LD("START: agent_dirty_deltas_reaper");
  sqlres sr      = zh_get_agent_offline();
  if (sr.err.size()) return;
  bool   offline = sr.ures ? true : false;
  if (!offline) {
    run_agent_dirty_deltas_reaper();
  }
  if (DebugDaemons) LD("FINISH: agent_dirty_deltas_reaper");
}

