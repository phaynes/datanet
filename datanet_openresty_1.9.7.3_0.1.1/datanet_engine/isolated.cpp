#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>
#include <mutex>

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
#include "storage.h"
#include "dack.h"
#include "isolated.h"
#include "agent_daemon.h"
#include "activesync.h"
#include "handler.h"
#include "datastore.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNS -------------------------------------------------------------------

extern lua_State *GL;

extern Int64  MyUUID;
extern string MyDataCenterUUID;
extern string ConfigDataCenterName;

extern bool   AgentSynced;
extern jv     AgentGeoNodes;
extern jv     CentralMaster; 
extern UInt64 AgentLastReconnect;
extern bool   AgentConnected;
extern bool   Isolation;

extern string DeviceKey;
extern jv     zh_nobody_auth;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FORWARD DECLARATIONS ------------------------------------------------------

static bool   recheck_central(jv *params);
static sqlres on_connect_sync_ack_agent_online(jv *params);

static void   do_geo_failover(jv *gnode);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZWSS SETTINGS -------------------------------------------------------------

UInt64 MaxNumReconnectFailsBeforeFailover = 3;
UInt64 RetryConfigDCSleep                 = 30000;

// NOTE: (AgentGeoFailoverSleep > CentralDirtyDeltaStaleness)
UInt64 AgentGeoFailoverSleep              = 40000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZWSS GLOBALS --------------------------------------------------------------

bool   RetryConfigDC               = true;
UInt64 CurrentNumberReconnectFails =  0;
Int64  LastGeoNode                 = -1;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

bool zisl_do_broadcast_workers_settings() {
  LT("zisl_do_broadcast_workers_settings");
  jv   skss  = JOBJECT;
  jv   rkss  = JARRAY;
  bool isao  = false;
  bool issub = false;
  return broadcast_workers_settings(&skss, &rkss, isao, issub);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RECHECK AGENT TO CENTRAL --------------------------------------------------

static jv get_new_kss_elements(jv *akss, jv *bkss) {
  jv dkss = JARRAY;
  map<string, bool> adict;
  map<string, bool> bdict;
  for (uint i = 0; i < akss->size(); i++) {
    jv     *jks = &((*akss)[i]);
    string  kqk = (*jks)["kqk"].asString();
    adict.insert(make_pair(kqk, true));
  }
  for (uint i = 0; i < bkss->size(); i++) {
    jv     *jks = &((*bkss)[i]);
    string  kqk = (*jks)["kqk"].asString();
    bdict.insert(make_pair(kqk, true));
  }
  for (map<string, bool>::iterator it  = bdict.begin();
                                   it != bdict.end(); it++) {
    string kqk = it->first;
    map<string, bool>::iterator ait = adict.find(kqk);
    if (ait != adict.end()) {
      jv jks = zh_create_ks(kqk, NULL);
      zh_jv_append(&dkss, &jks);
    }
  }
  return dkss;
}

sqlres process_ack_agent_recheck(lua_State *L, jv *jreq, jv *result) {
  LD("process_ack_agent_recheck");
  UInt64 recheck = (*result)["recheck"].asUInt64();
  if (recheck) {
    jv *oparams           = &((*jreq)["params"]);
    (*oparams)["recheck"] = recheck;
    if (!recheck_central(oparams)) {
      RETURN_SQL_RESULT_ERROR("recheck_central");
    }
  } else {
    jv *opkss = &((*jreq)["params"]["data"]["pkss"]);
    jv *orkss = &((*jreq)["params"]["data"]["rkss"]);
    jv *npkss = &((*result)["pkss"]);
    jv *nrkss = &((*result)["rkss"]);
    jv  dpkss = get_new_kss_elements(opkss, npkss);
    // TODO check if pkss have been sync'ed lately (e.g. via SM)
    jv  drkss = get_new_kss_elements(orkss, nrkss);
    jv  nparams;
    nparams["geo_nodes"] = (*result)["geo_nodes"];
    nparams["pkss"]      = dpkss;
    nparams["rkss"]      = drkss;
    sqlres sr            = on_connect_sync_ack_agent_online(&nparams);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static bool recheck_central(jv *params) {
  UInt64 recheck = ((*params)["recheck"].asUInt64() / 1000);
  if (!recheck) return true;
  LD("recheck_central: RECHECK: " << recheck);
  Int64  created  = (*params)["original_created"].asInt64();
  jv     prequest = zh_create_json_rpc_body("AgentRecheck", &zh_nobody_auth);
  prequest["params"]["data"]["created"] = created;
  return zexh_send_central_https_request(recheck, prequest, false);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HANDLE AGENT-ONLINE RESPONSE FROM CENTRAL ---------------------------

sqlres zisl_set_agent_synced() {
  AgentSynced = true;
  LD("ZH.Agent.synced");
  RETURN_EMPTY_SQLRES
}

static sqlres sync_ks(jv *skss) {
  vector<string> mbrs = zh_jv_get_member_names(skss);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  kqk    = mbrs[i];
    jv     *jks    = &((*skss)[kqk]);
    string  sectok = (*jks)["security_token"].asString();
    sqlres  sr     = zas_set_agent_sync_key(kqk, sectok);
    RETURN_SQL_ERROR(sr);
  }
  RETURN_EMPTY_SQLRES
}

static sqlres do_remove_sync_ack_agent_online(jv *rkss) {
  LT("do_remove_sync_ack_agent_online");
  for (uint i = 0; i < rkss->size(); i++) {
    jv     *ks  = &((*rkss)[i]);
    string  kqk = (*ks)["kqk"].asString();
    sqlres  sr  = zds_remove_key(kqk);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres sync_ack_agent_online(jv *skss, jv *rkss) {
  LT("sync_ack_agent_online");
  if (rkss->size()) {
    sqlres sr = do_remove_sync_ack_agent_online(rkss);
    RETURN_SQL_ERROR(sr)
  }
  sqlres sr = sync_ks(skss);
  RETURN_SQL_ERROR(sr);
  return zisl_set_agent_synced();
}

// NOTE: LOCAL_ONLY: Local AgentDeltas but NOT in AgentOnline.pkss/ckss[]
static sqlres get_local_only_change(jv *skss, jv *jkqk_drain) {
  LT("get_local_only_change");
  sqlres  sr = fetch_all_agent_dirty_keys();
  RETURN_SQL_ERROR(sr)
  jv     *dk = &(sr.jres);
  vector<string> mbrs = zh_jv_get_member_names(dk);
  for (uint i = 0; i < mbrs.size(); i++) {
    string dkqk = mbrs[i];
    if (!zh_jv_is_member(skss, dkqk)) { // MISS
      jkqk_drain->append(dkqk);
    }
  }
  RETURN_EMPTY_SQLRES
}

static sqlres drain_local_only_change(jv *jkqk_drain) {
  for (uint i = 0; i < jkqk_drain->size(); i++) {
    string kqk = (*jkqk_drain)[i].asString();
    sqlres sr  = zadaem_get_agent_key_drain(kqk, false);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static void remove_non_local_kqk(jv *skss) {
  vector<string> mbrs = zh_jv_get_member_names(skss);
  for (uint i = 0; i < mbrs.size(); i++) {
    string kqk = mbrs[i];
    if (!zh_is_key_local_to_worker(kqk)) {
      skss->removeMember(kqk);
    }
  }
}

sqlres zisl_worker_sync_keys(jv *skss, jv *rkss) {
  LT("zisl_worker_sync_keys: SKSS: " << *skss);
  jv      jkqk_drain = JARRAY;
  remove_non_local_kqk(skss);
  sqlres  sr  = sync_ack_agent_online(skss, rkss);
  RETURN_SQL_ERROR(sr)
  zadaem_local_signal_to_sync_keys_daemon();
  zadaem_reset_key_drain_map();
  sr          = get_local_only_change(skss, &jkqk_drain);
  RETURN_SQL_ERROR(sr)
  sr          = drain_local_only_change(&jkqk_drain);
  RETURN_SQL_ERROR(sr)
  RETURN_EMPTY_SQLRES
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RECONNECT AGENT TO CENTRAL-------------------------------------------------

static sqlres init_new_device(Int64 duuid) {
  MyUUID             = duuid;
  LD("initial_agent_online: D: " << MyUUID);
  sqlres sr          = persist_agent_uuid();
  RETURN_SQL_ERROR(sr)
  UInt64 first_op_id = MyUUID * 100000;
  sr                 = persist_next_op_id(first_op_id);
  RETURN_SQL_ERROR(sr)
  return persist_agent_num_cache_bytes(0);
}

static jv build_sync_arrays(jv *params) {
  LT("build_sync_arrays");
  jv skss = JOBJECT;
  if (zh_jv_is_member(params, "pkss")) {
    jv *pkss = &((*params)["pkss"]);
    for (uint i = 0; i < pkss->size(); i++) {
      jv     *jks = &((*pkss)[i]);
      string  kqk = (*jks)["kqk"].asString();
      skss[kqk]   = *jks;
    }
  }
  if (zh_jv_is_member(params, "ckss")) {
    jv *ckss = &((*params)["ckss"]);
    for (uint i = 0; i < ckss->size(); i++) {
      jv     *jks = &((*ckss)[i]);
      string  kqk = (*jks)["kqk"].asString();
      skss[kqk]   = *jks;
    }
  }
  return skss;
}

static sqlres on_connect_sync_ack_agent_online(jv *params) {
  jv    skss  = build_sync_arrays(params);
  jv   *jgn   = &((*params)["geo_nodes"]);
  jv   *rkss  = &((*params)["rkss"]);
  zisl_set_geo_nodes(jgn);
  bool  isao  = true;
  bool  issub = false;
  if (!broadcast_workers_settings(&skss, rkss, isao, issub)) {
    RETURN_SQL_RESULT_ERROR("broadcast_workers_settings");
  }
  RETURN_EMPTY_SQLRES
}

static sqlres do_redirect(jv *jcnode) {
  LD("SWITCH -> CLUSTER-NODE: " << *jcnode);
  CentralMaster                = *jcnode;
  CentralMaster["device_uuid"] = MyDataCenterUUID;
  if (!zexh_send_central_agent_online(0, true)) {
    RETURN_SQL_RESULT_ERROR("zexh_send_central_agent_online");
  }
  RETURN_SQL_RESULT_UINT(1) // NOTE: means REDIRECTED
}

static sqlres check_redirect(jv *device) {
  LT("check_redirect");
  if (!zh_jv_is_member(device, "cluster_node")) RETURN_EMPTY_SQLRES
  else {
    jv   *jcnode = &((*device)["cluster_node"]);
    bool  redir  = !zh_same_master(&CentralMaster, jcnode);
    if (redir) return do_redirect(jcnode);
    else       RETURN_EMPTY_SQLRES
  }
}

static sqlres handle_redirect(jv *params) {
  MyDataCenterUUID = (*params)["datacenter"].asString();
  LD("handle_redirect: DC: " << MyDataCenterUUID);
  sqlres sr = persist_datacenter_uuid(MyDataCenterUUID);
  RETURN_SQL_ERROR(sr)
  if (zh_get_bool_member(params, "offline")) {
    sqlres sr = persist_agent_connected(false);
    RETURN_SQL_ERROR(sr)
    RETURN_EMPTY_SQLRES
  } else {
    jv *device = &((*params)["device"]);
    if (zh_get_bool_member(device, "initialize_device")) {
      Int64  duuid = (*device)["uuid"].asInt64();
      sqlres sr    = init_new_device(duuid);
      RETURN_SQL_ERROR(sr)
      return check_redirect(device);
    } else {
      LD("handle_redirect: -> check_redirect");
      return check_redirect(device);
    }
  }
}

static sqlres handle_ack_agent_online(jv *params) {
  LT("handle_ack_agent_online");
  jv *device = &((*params)["device"]);
  if (zh_jv_is_member(device, "key")) {
    string dkey = (*device)["key"].asString();
    sqlres sr   = persist_device_key(dkey);
    RETURN_SQL_ERROR(sr)
    DeviceKey   = dkey;
    LE("initialize_agent_device_key: CPP-DEVICE-KEY: " << DeviceKey);
  }
  sqlres sr         = handle_redirect(params);
  RETURN_SQL_ERROR(sr)
  bool   redirected = sr.ures ? true : false;
  if (zh_get_bool_member(params, "offline")) {
    AgentConnected = false;
  } else {
    if (!redirected) {
      AgentConnected     = true;
      sqlres sr          = persist_agent_connected(true);
      RETURN_SQL_ERROR(sr)
      AgentLastReconnect = zh_get_ms_time();
      LD("AgentLastReconnect: " << AgentLastReconnect);
      sr                 = on_connect_sync_ack_agent_online(params);
      RETURN_SQL_ERROR(sr)
      if (!recheck_central(params)) {
        RETURN_SQL_RESULT_ERROR("recheck_central");
      }
    }
  }
  RETURN_EMPTY_SQLRES
}

sqlres process_ack_agent_online(lua_State *L, jv *jreq, jv *params) {
  return handle_ack_agent_online(params);
}

bool zisl_reconnect_to_central() {
  LT("zisl_reconnect_to_central");
  DeviceKey.empty(); // NEW DEVICE-KEY PER CENTRAL CONNECTION
  sqlres sr   = fetch_agent_backoff_end_time();
  if (sr.err.size()) return false;
  UInt64 et   = sr.ures;
  UInt64 now  = zh_get_ms_time();
  Int64  diff = (et - now);
  if (diff >= 0) {
    LE("zisl_reconnect_to_central: AgentBackoff still active -> NO-OP");
    return true;
  } else {
    return zexh_send_central_agent_online(0, true);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GENERIC CALLBACK HANDLERS -------------------------------------------------

static bool GeoFailoverTimerSet = false;

static bool should_do_geo_failover() {
  if (GeoFailoverTimerSet)                             return false;
  if (RetryConfigDC == false)                          return false;
  if (!ConfigDataCenterName.compare(MyDataCenterUUID)) return false;
  for (uint i = 0; i < AgentGeoNodes.size(); i++) {
    jv     *gnode  = &(AgentGeoNodes[i]);
    string  dcname = (*gnode)["device_uuid"].asString();
    if (!ConfigDataCenterName.compare(dcname)) {
      return true;
    }
  }
  return false;
}

static bool do_geo_failover() {
  LT("do_geo_failover");
  GeoFailoverTimerSet = false;
  for (uint i = 0; i < AgentGeoNodes.size(); i++) {
    jv     *gnode  = &(AgentGeoNodes[i]);
    string  dcname = (*gnode)["device_uuid"].asString();
    if (!ConfigDataCenterName.compare(dcname)) {
      LD("RECONNECT TO CONFIG DATACENTER: " << *gnode);
      do_geo_failover(gnode);
      return true;
    }
  }
  return false;
}

static bool handle_callback_retry_config_datacenter() {
  LT("handle_callback_retry_config_datacenter");
  LD("RetryConfigDC -> TRUE");
  RetryConfigDC = true;
  bool sgf = should_do_geo_failover();
  if (sgf) {
    GeoFailoverTimerSet = true;
    do_geo_failover();
  }
  return true;
}

static bool handle_callback_geo_failover() {
  LT("handle_callback_geo_failover");
  bool sgf = should_do_geo_failover();
  if (sgf) do_geo_failover();
  return true;
}

bool zisl_handle_generic_callback(const char *cbname) {
  LT("zisl_handle_generic_callback: CB: " << cbname);
  string cbn(cbname);
  if (!cbn.compare("GEO_FAILOVER")) {
    return handle_callback_geo_failover();
  } else if (!cbn.compare("RETRY_CONFIG_DATACENTER")) {
    return handle_callback_retry_config_datacenter();
  } else {
    zh_fatal_error("LOGIC(zisl_handle_generic_callback");
    return false; // compiler warning
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SET-GEO-NODES -------------------------------------------------------

static bool handle_geo_state_change_event() {
  pid_t pid = getpid();
  LD("handle_geo_state_change_event: PID: " << pid);
  zh_debug_json_value("AgentGeoNodes", &AgentGeoNodes);
  bool sgf = should_do_geo_failover();
  if (sgf) {
    GeoFailoverTimerSet = true;
    (void)zexh_set_timer_callback("GEO_FAILOVER", AgentGeoFailoverSleep);
    return true;
  }
  return false;
}

bool zisl_set_geo_nodes(jv *jgn) {
  if (zjd(jgn)) AgentGeoNodes = *jgn;
  return handle_geo_state_change_event();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT GET CLUSTER INFO ---------------------------------------------------

static jv respond_client_get_cluster_info(string &id, jv *jinfo) {
  jv response = zh_create_jv_response_ok(id);
  response["result"]["information"] = *jinfo;
  return response;
}

jv process_client_get_cluster_info(lua_State *L, string id, jv *params) {
  LT("process_client_get_cluster_info");
  jv jinfo              = JOBJECT;
  jinfo["cluster_node"] = CentralMaster;
  jinfo["datacenter"]   = MyDataCenterUUID;
  jinfo["geo_nodes"]    = AgentGeoNodes;
  sqlres sr             = fetch_isolation();
  PROCESS_GENERIC_ERROR(id, sr)
  bool isolation        = sr.ures ? true : false;
  jinfo["isolated"]     = isolation;
  sr                    = fetch_agent_connected();
  PROCESS_GENERIC_ERROR(id, sr)
  jinfo["connected"]    = sr.ures ? true : false;
  return respond_client_get_cluster_info(id, &jinfo);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SEND CENTRAL DEVICE-ISOLATION ---------------------------------------

sqlres zisl_set_agent_isolation(bool b) {
  return persist_isolation(b);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HANDLE ACK-BACKOFF FROM CENTRAL -------------------------------------

static sqlres handle_agent_backoff(UInt64 bsecs, UInt64 endt) {
  sqlres sr = persist_agent_backoff_end_time(endt);
  RETURN_SQL_ERROR(sr)
  if (!zexh_send_central_agent_online(bsecs, true)) {
    RETURN_SQL_RESULT_ERROR("zexh_send_central_agent_online");
  }
  RETURN_EMPTY_SQLRES
}

jv process_agent_backoff(lua_State *L, string id, jv *params) {
  UInt64 bsecs = (*params)["data"]["seconds"].asUInt64();
  UInt64 endt  = zh_get_ms_time() + (bsecs * 1000);
  LD("process_agent_backoff: SECS: " << bsecs << " END: " << endt);
  sqlres sr    = handle_agent_backoff(bsecs, endt);
  PROCESS_GENERIC_ERROR(id, sr)
  return zh_create_jv_response_ok(id);
}

// TODO ZISL.StartupOnBackoff()


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT GET AGENT-INFO FOR SEND-CENTRAL-AGENT-ONLINE-------------------------

sqlres zisl_get_agent_info() {
  sqlres  sr       = fetch_all_stationed_users();
  RETURN_SQL_ERROR(sr)
  jv      jsusers  = sr.jres; // NOTE: CLONE
  sr               = fetch_all_user_permissions();
  RETURN_SQL_ERROR(sr)
  jv      uperms   = sr.jres; // NOTE: CLONE
  sr               = fetch_all_user_subscriptions();
  RETURN_SQL_ERROR(sr)
  jv      usubs    = sr.jres; // NOTE: CLONE
  sqlres  sr_ainfo;
  sr_ainfo.jres["stationedusers"] = JARRAY;
  jv     *jasusers = &(sr_ainfo.jres["stationedusers"]);
  for (uint i = 0; i < jsusers.size(); i++) {
    jv *jsuser = &(jsusers[i]);
    zh_jv_append(jasusers, jsuser);
  }
  sr_ainfo.jres["permissions"]   = uperms;
  sr_ainfo.jres["subscriptions"] = usubs;
  return sr_ainfo;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZWSS LOGIC ----------------------------------------------------------------

static void run_agent_config_datacenter_up() {
  LD("RetryConfigDC -> FALSE SLEEP: " << RetryConfigDCSleep);
  RetryConfigDC = false;
  (void)zexh_set_timer_callback("RETRY_CONFIG_DATACENTER", RetryConfigDCSleep);
}

static void unset_retry_config_DC() { LT("unset_retry_config_DC");
  if (RetryConfigDC == false) return;
  run_agent_config_datacenter_up();
}

static void assign_load_balancer_to_server(jv *cnode) {
  string conname = zh_get_connection_name();
  (*cnode)[conname]["server"] = (*cnode)[conname]["load_balancer"];
}

static void do_geo_failover(jv *gnode) {
  LD("do_geo_failover: gnode: " << *gnode);
  string  conname  = zh_get_connection_name();
  jv     *glb      = &((*gnode)[conname]["load_balancer"]);
  string  glb_host = (*glb)["hostname"].asString();
  UInt64  glb_port = (*glb)["port"].asUInt64();
  jv      cnode;
  cnode["device_uuid"] = (*gnode)["device_uuid"].asString();
  cnode[conname]["load_balancer"]["hostname"] = glb_host;
  cnode[conname]["load_balancer"]["port"]     = glb_port;
  assign_load_balancer_to_server(&cnode);
  CentralMaster         = cnode;
  MyDataCenterUUID      = CentralMaster["device_uuid"].asString();
  LD("do_geo_failover: NEW MASTER: " << CentralMaster);
  (void)zisl_do_broadcast_workers_settings();
  (void)zisl_reconnect_to_central();
}

static bool zwss_reconnect_agent() {
  if (Isolation) {
    LD("reconnect_agent(): ISOLATED -> not reconnecting");
    return true;
  }
  CurrentNumberReconnectFails += 1;
  UInt64 nfails  = CurrentNumberReconnectFails;
  UInt64 maxfail = MaxNumReconnectFailsBeforeFailover;
  LD("reconnect_agent(): Reconnecting: TRY#: " << nfails);
  if (nfails >= maxfail) {
    unset_retry_config_DC();
    if ((nfails % maxfail) == 0) { // GEO FAILOVER
      LD("Geo-Failover: AgentGeoNodes[]: " << AgentGeoNodes);
      if (zjd(&AgentGeoNodes) && AgentGeoNodes.size() != 0) {
        LastGeoNode += 1;
        if (LastGeoNode == AgentGeoNodes.size()) LastGeoNode = 0;
        int  lgn   = LastGeoNode;
        jv  *gnode = &(AgentGeoNodes[lgn]);
        do_geo_failover(gnode);
      }
    }
  }
  return true;
}

bool zisl_broken_central_connection() { LT("zisl_broken_central_connection");
  return zwss_reconnect_agent();
}

