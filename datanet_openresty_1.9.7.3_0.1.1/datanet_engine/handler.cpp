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

extern "C" {
  #include "lua.h"
  #include "lualib.h"
  #include "lauxlib.h"
}

#include "json/json.h"
#include "easylogging++.h"

#include "helper.h"
#include "external_hooks.h"
#include "shared.h"
#include "deltas.h"
#include "isolated.h"
#include "creap.h"
#include "agent_daemon.h"
#include "activesync.h"
#include "auth.h"
#include "storage.h"

using namespace std;

extern lua_State *GL;

extern Int64  MyUUID;
extern string MyDataCenterUUID;
extern jv     CentralMaster;

extern jv     AgentGeoNodes;
extern UInt64 AgentLastReconnect;
extern bool   AgentConnected;

extern string DeviceKey;
extern jv     zh_nobody_auth;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WORKER BROADCAST SETTINGS -------------------------------------------------

jv process_internal_agent_settings(lua_State *L, string id, jv *params) {
  zisl_set_agent_synced();
  MyUUID           = (*params)["data"]["device"]["uuid"].asInt();
  DeviceKey        = (*params)["data"]["device"]["key"].asString();
  CentralMaster    = (*params)["data"]["central"];
  MyDataCenterUUID = (*params)["data"]["datacenter"].asString();
  AgentGeoNodes    = (*params)["data"]["geo_nodes"];
  AgentConnected   = (*params)["data"]["online"].asBool();
  jv   *skss       = &((*params)["data"]["skss"]);
  jv   *rkss       = &((*params)["data"]["rkss"]);
  bool  isao       = (*params)["data"]["from_agent_online"].asBool();
  bool  issub      = (*params)["data"]["is_subscribe"].asBool();
  if (AgentConnected) {
    AgentLastReconnect = (*params)["data"]["last_reconnect"].asUInt64();
  }
  LD("process_internal_agent_settings: MyUUID: " << MyUUID <<
     " DC: " << MyDataCenterUUID << " DK: " << DeviceKey);
  if (isao) zisl_worker_sync_keys(skss, rkss);
  else      zas_sync_missing_keys(skss, issub);
  return zh_create_jv_response_ok(id);
}

jv create_agent_settings_json_rpc_body(jv *skss, jv *rkss,
                                       bool isao, bool issub) {
  jv prequest = zh_create_json_rpc_body("InternalAgentSettings",
                                        &zh_nobody_auth);
  prequest["params"]["data"]["datacenter"]        = MyDataCenterUUID;
  prequest["params"]["data"]["central"]           = CentralMaster;
  prequest["params"]["data"]["geo_nodes"]         = AgentGeoNodes;
  prequest["params"]["data"]["online"]            = AgentConnected;
  prequest["params"]["data"]["last_reconnect"]    = AgentLastReconnect;
  prequest["params"]["data"]["skss"]              = *skss;
  prequest["params"]["data"]["rkss"]              = *rkss;
  prequest["params"]["data"]["from_agent_online"] =  isao;
  prequest["params"]["data"]["is_subscribe"]      =  issub;
  return prequest;
}

bool broadcast_workers_settings(jv *skss, jv *rkss, bool isao, bool issub) {
  if (MyUUID == -1) return false; // NO INFO TO SEND
  LT("broadcast_workers_settings");
  jv prequest = create_agent_settings_json_rpc_body(skss, rkss, isao, issub);
  return zexh_broadcast_internal_request(prequest, 0);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER RECONNECT ------------------------------------------------------

jv process_subscriber_reconnect(lua_State *L, string id, jv *params) {
  LT("process_subscriber_reconnect");
  if (!zexh_send_central_agent_online(0, true)) {
    LE("process_subscriber_reconnect: ERROR: zexh_send_central_agent_online");
  }
  return zh_create_jv_response_ok(id);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER GEO STATE CHANGE -----------------------------------------------

jv process_subscriber_geo_state_change(lua_State *L, string id, jv *params) {
  jv   *jgn       = &((*params)["data"]["geo_nodes"]);
  LT("process_subscriber_geo_state_change");
  bool  geo_redir = zisl_set_geo_nodes(jgn);
  if (!geo_redir) {
    (void)zisl_do_broadcast_workers_settings();
  }
  return zh_create_jv_response_ok(id);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZUM -----------------------------------------------------------------------

static sqlres grant_user(string username, string schanid, string priv) {
  if (!priv.compare("REVOKE")) {
    sqlres sr = remove_user_permission(username, schanid);
    RETURN_SQL_ERROR(sr)
  } else {
    string pabbr = !priv.compare("WRITE") ? "W" : "R";
    sqlres sr    = persist_user_permission(username, schanid, pabbr);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

jv process_propogate_grant_user(lua_State *L, string id, jv *result) {
  LT("process_propogate_grant_user");
  string username = (*result)["data"]["username"].asString();
  string schanid  = (*result)["data"]["channel"]["id"].asString();
  string priv     = (*result)["data"]["privilege"].asString();
  sqlres sr       =  grant_user(username, schanid, priv);
  PROCESS_GENERIC_ERROR(id, sr)
  return zh_create_jv_response_ok(id);
}

sqlres process_ack_admin_add_user(lua_State *L, jv *jreq, jv *result) {
  bool ok = (*result)["ok"].asBool();
  LD("process_ack_admin_add_user: OK: " << ok);
  RETURN_EMPTY_SQLRES
}

sqlres process_ack_admin_grant_user(lua_State *L, jv *jreq, jv *result) {
  bool ok = (*result)["ok"].asBool();
  LD("process_ack_admin_grant_user: OK: " << ok);
  RETURN_EMPTY_SQLRES
}

sqlres process_ack_admin_remove_user(lua_State *L, jv *jreq, jv *result) {
  bool ok = (*result)["ok"].asBool();
  LD("process_ack_admin_remove_user: OK: " << ok);
  RETURN_EMPTY_SQLRES
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE CLIENT FETCH -------------------------------------------------------

static jv handle_agent_fetch(string &id, string &kqk) {
  sqlres sc;
  FETCH_CRDT(kqk);
  PROCESS_GENERIC_ERROR(id, sc)
  jv *jcrdt = &(sc.jres);
  if (zjn(jcrdt)) {
    return zh_process_error(id, -32007, ZS_Errors["NoDataFound"]);
  } else {
    sqlres sr                          = zcreap_set_lru_local_read(kqk);
    PROCESS_GENERIC_ERROR(id, sr)
    jv     response                    = zh_create_jv_response_ok(id);
    response["result"]["datacenter"]   = MyDataCenterUUID;
    sr                                 = fetch_agent_connected();
    PROCESS_GENERIC_ERROR(id, sr)
    response["result"]["connected"]    = sr.ures ? true : false;
    response["result"]["crdt"]         = *jcrdt;
    sr                                 = zdelt_get_key_subscriber_versions(kqk);
    PROCESS_GENERIC_ERROR(id, sr)
    response["result"]["dependencies"] = sr.jres;
    return response;
  }
}

jv process_client_fetch(lua_State *L, string id, jv *params) {
  LT("process_client_fetch");
  jv      jns   = (*params)["data"]["namespace"];
  jv      jcn   = (*params)["data"]["collection"];
  jv      jkey  = (*params)["data"]["key"];
  string  kqk   = zs_create_kqk(jns, jcn, jkey);
  return handle_agent_fetch(id, kqk);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT FIND ---------------------------------------------------------------

static jv respond_client_find(string &id, jv *jjsons) {
  jv response = zh_create_jv_response_ok(id);
  response["result"]["jsons"] = *jjsons;
  return response;
}

jv process_client_find(lua_State *L, string id, jv *params) {
  jv     *jauth  = &((*params)["authentication"]);
  jv      jns    = (*params)["data"]["namespace"];
  jv      jcn    = (*params)["data"]["collection"];
  string  ns     = jns.asString();
  string  cn     = jcn.asString();
  string  qstr   = (*params)["data"]["query"].asString();
  LT("process_client_find: NS: " << ns << " CN: " << cn << " Q: " << qstr);
  jv      jquery = zh_parse_json_text(qstr, false);
  if (zjn(&jquery)) {
    return zh_process_error(id, -32007, ZS_Errors["QueryParseError"]);
  }
  if (!zh_jv_is_member(&jquery, "_id")) {
    return zh_process_error(id, -32007, ZS_Errors["QueryEngineMissing"]);
  }
  jv      jkey   = jquery["_id"];
  string  kqk    = zs_create_kqk(jns, jcn, jkey);
  jv      jjsons = JARRAY;
  sqlres  sr     = zauth_has_read_permissions(jauth, kqk);
  bool    ok     = sr.ures ? true : false;
  LT("process_client_find: K: " << kqk << " OK: " << ok);
  if (ok) {
    sqlres sc;
    FETCH_CRDT(kqk);
    PROCESS_GENERIC_ERROR(id, sc)
    jv     *jfcrdt = &(sc.jres);
    if (zjd(jfcrdt)) {
      jv jjson = zh_create_pretty_json(jfcrdt);
      zh_jv_append(&jjsons, &jjson);
    }
  }
  return respond_client_find(id, &jjsons);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT ISOLATION -----------------------------------------------------------

static jv respond_client_isolation(string &id, bool b) {
  jv response = zh_create_jv_response_ok(id);
  response["result"]["isolation"] = b;
  return response;
}

jv process_client_isolation(lua_State *L, string id, jv *params) {
  LT("process_client_isolation");
  bool   isolate = (*params)["data"]["value"].asBool();
  LD("process_client_isolation: isolate: " << isolate);
  sqlres sr      = fetch_isolation();
  PROCESS_GENERIC_ERROR(id, sr)
  bool isolation = sr.ures ? true : false;
  if (isolate == isolation) {
    LD("Repeat isolation set -> NO-OP");
    return respond_client_isolation(id, isolate);
  }
  sr = zisl_set_agent_isolation(isolate);
  PROCESS_GENERIC_ERROR(id, sr)
  if (isolate) { // Send AgentOffline() to Central
    LD("ISOLATION BEGINS");
    (void)zexh_send_central_agent_online(0, false);
  } else { // No longer Isolated, Lets' Reconnect to central
    LD("ISOLATION ENDS");
    (void)zisl_reconnect_to_central();
  }
  return respond_client_isolation(id, isolate);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET-STATIONED-USERS/GET-AGENT-SUBSCRIPTIONS -------------------------------

static jv respond_client_get_stationed_users(string &id, jv *jsusers) {
  jv response = zh_create_jv_response_ok(id);
  if (zjd(jsusers)) response["result"]["users"] = *jsusers;
  return response;
}

jv process_client_get_stationed_users(lua_State *L, string id, jv *params) {
  LT("ZSU.AgentGetStationedUsers");
  sqlres  sr      = fetch_all_stationed_users();
  PROCESS_GENERIC_ERROR(id, sr)
  jv     *jsusers = &(sr.jres);
  return respond_client_get_stationed_users(id, jsusers);
}

static jv respond_client_get_agent_subscriptions(string &id, jv *subs) {
  jv response = zh_create_jv_response_ok(id);
  response["result"]["subscriptions"] = *subs;
  return response;
}

jv process_client_get_agent_subscriptions(lua_State *L, string id, jv *params) {
  LT("ZChannel.AgentGetAgentSubscriptions");
  sqlres  sr    = fetch_all_user_subscriptions();
  PROCESS_GENERIC_ERROR(id, sr)
  jv     *usubs = &(sr.jres); 
  jv      subs  = JARRAY;
  vector<string> mbrs = zh_jv_get_member_names(usubs);
  for (uint i = 0; i < mbrs.size(); i++) {
    string username = mbrs[i];
    string schanid  = (*usubs)[username].asString();
    subs.append(schanid);
  }
  return respond_client_get_agent_subscriptions(id, &subs);
}

static jv respond_client_get_user_subscriptions(string &id, jv *subs) {
  jv response = zh_create_jv_response_ok(id);
  response["result"]["subscriptions"] = *subs;
  return response;
}

jv process_client_get_user_subscriptions(lua_State *L, string id, jv *params) {
  string  username = (*params)["authentication"]["username"].asString();
  LT("ZChannel.AgentGetUserSubscriptions: UN: " << username);
  sqlres  sr     = fetch_user_subscriptions(username);
  PROCESS_GENERIC_ERROR(id, sr)
  jv     *jcsubs = &(sr.jres);
  jv      subs   = JARRAY;
  vector<string> mbrs = zh_jv_get_member_names(jcsubs);
  for (uint i = 0; i < mbrs.size(); i++) {
    string schanid = mbrs[i];
    subs.append(schanid);
  }
  return respond_client_get_user_subscriptions(id, &subs);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZSU -----------------------------------------------------------------------

sqlres process_ack_agent_station_user(lua_State *L, jv *jreq, jv *result) {
  LT("process_ack_agent_station_user");
  jv     *jauth    = &((*jreq)["params"]["authentication"]);
  string  username = (*jauth)["username"].asString();
  jv     *perms    = &((*result)["permissions"]);
  jv     *subs     = &((*result)["subscriptions"]);
  jv     *pkss     = &((*result)["pkss"]);

  sqlres sr = persist_stationed_user(username); // ZSU.store_stationed_user()
  RETURN_SQL_ERROR(sr)
  sr = persist_agent_permissions(perms);        // ZSU.agent_store_permissions()
  RETURN_SQL_ERROR(sr)
  sr = persist_user_permissions(&username, perms);
  RETURN_SQL_ERROR(sr)

  // ZChannel.AgentStoreUserSubscriptions
  sr = persist_agent_subscriptions(subs);
  RETURN_SQL_ERROR(sr)
  sr = persist_user_subscriptions(&username, subs);
  RETURN_SQL_ERROR(sr)

  // ZSU.agent_sync_keys
  sr = zas_agent_sync_channel_kss(pkss, false);
  RETURN_SQL_ERROR(sr)
  RETURN_EMPTY_SQLRES
}

static sqlres destation_remove_channel_device(string &schanid) {
  return remove_device_subscriptions(schanid);
}

static sqlres destation_remove_device_subscription(string &schanid) {
  return remove_channel_device(schanid);
}

static sqlres destation_channel(string &schanid) {
  sqlres sr = destation_remove_channel_device(schanid);
  RETURN_SQL_ERROR(sr)
  return destation_remove_device_subscription(schanid);
}

static sqlres store_unsubscribe(string &schanid, string username) {
  sqlres sr = destation_channel(schanid);
  RETURN_SQL_ERROR(sr)
  return remove_user_subscription(username, schanid);
}

static sqlres zchan_agent_remove_user_subscriptions(jv *jcsubs,
                                                    string &username) {
  zh_type_object_assert(jcsubs, "LOGIC(zchan_agent_remove_user_subscriptions)");
  for (jv::iterator it = jcsubs->begin(); it != jcsubs->end(); it++) {
    jv     jschanid = *it;
    string schanid  = jschanid.asString();
    sqlres sr       = store_unsubscribe(schanid, username);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres agent_remove_permissions(string &username) {
  return remove_user_permissions(username);
}

static sqlres do_remove_stationed_user(string &username) {
  return remove_stationed_user(username);
}

static sqlres agent_destation_user(jv *jcsubs, string &username) {
  sqlres sr = zchan_agent_remove_user_subscriptions(jcsubs, username);
  RETURN_SQL_ERROR(sr)
  sr        = agent_remove_permissions(username);
  RETURN_SQL_ERROR(sr)
  return do_remove_stationed_user(username);
}

static sqlres agent_process_destation_user(string username) {
  sqlres sr = fetch_user_subscriptions(username);
  RETURN_SQL_ERROR(sr)
  jv *jcsubs = &(sr.jres);
  return agent_destation_user(jcsubs, username);
}

sqlres process_ack_agent_destation_user(lua_State *L, jv *jreq, jv *result) {
  LT("process_ack_agent_destation_user");
  jv     *jauth    = &((*jreq)["params"]["authentication"]);
  string  username = (*jauth)["username"].asString();
  return agent_process_destation_user(username);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZCHAN ---------------------------------------------------------------------

sqlres process_ack_agent_subscribe(lua_State *L, jv *jreq, jv *result) {
  RETURN_EMPTY_SQLRES
}

static sqlres station_channel(string &schanid) {
  sqlres sr = persist_channel_device(schanid);
  RETURN_SQL_ERROR(sr)
  return persist_device_subscription(schanid);
}

static sqlres store_subscribe(string &username, string &schanid, string &perm) {
  sqlres sr = station_channel(schanid);
  RETURN_SQL_ERROR(sr)
  return persist_user_subscription(username, schanid, perm);
}

jv process_propogate_subscribe(lua_State *L, string id, jv *result) {
  LT("process_propogate_subscribe");
  string  schanid  = (*result)["data"]["channel"]["id"].asString();
  string  perm     = (*result)["data"]["channel"]["permissions"].asString();
  string  username = (*result)["data"]["username"].asString();
  jv     *pkss     = &((*result)["data"]["pkss"]);
  sqlres  sr       = store_subscribe(username, schanid, perm);
  PROCESS_GENERIC_ERROR(id, sr)
  sr               = zas_agent_sync_channel_kss(pkss, true);
  PROCESS_GENERIC_ERROR(id, sr)
  return zh_create_jv_response_ok(id);
}

sqlres process_ack_agent_unsubscribe(lua_State *L, jv *jreq, jv *result) {
  jv     *jauth    = &((*jreq)["params"]["authentication"]);
  jv     *jchanid  = &((*jreq)["params"]["data"]["channel"]["id"]);
  string  username = (*jauth)["username"].asString();
  string  schanid  = jchanid->asString();
  return remove_user_subscription(username, schanid);
}


