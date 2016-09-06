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
#include "deltas.h"
#include "isolated.h"
#include "subscriber_merge.h"
#include "dack.h"
#include "creap.h"
#include "agent_daemon.h"
#include "activesync.h"
#include "datastore.h"
#include "storage.h"

using namespace std;


extern jv     zh_nobody_auth;
extern string zh_wildcard_user;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FORWARD DECLARATIONS ------------------------------------------------------

static sqlres post_cache_lru_update(string &kqk, bool pin, bool sticky);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HELPERS -------------------------------------------------------------

static sqlres set_key_watch(string &kqk) {
  sqlres sr = persist_agent_watch_keys(kqk);
  RETURN_SQL_ERROR(sr)
  return persist_lru_key_field(kqk, "WATCH", 1);
}

static sqlres store_cache_key_metadata(string &kqk, bool watch) {
  // NOTE: ZMDC.SetKeyToDevices not used on AGENT
  sqlres sr = persist_cached_keys(kqk);
  RETURN_SQL_ERROR(sr)
  if (watch) return set_key_watch(kqk);
  else       return remove_agent_watch_keys(kqk);
}

sqlres zcache_agent_store_cache_key_metadata(string &kqk, bool watch,
                                             jv *jauth) {
  LT("zcache_agent_store_cache_key_metadata");
  string username = (*jauth)["username"].asString();
  sqlres sr       = store_cache_key_metadata(kqk, watch);
  RETURN_SQL_ERROR(sr)
  sr              = persist_key_cached_by_user(kqk, username);
  RETURN_SQL_ERROR(sr)
  sr              = remove_evicted(kqk);
  RETURN_SQL_ERROR(sr)
  return zcreap_set_lru_local_read(kqk);
}

static sqlres check_already_subscribed(jv *jauth, jv *jrchans) {
  if (zjn(jrchans)) RETURN_EMPTY_SQLRES // FALSE
  else {
    string  username = (*jauth)["username"].asString();
    sqlres  sr       = fetch_user_subscriptions(username);
    RETURN_SQL_ERROR(sr)
    jv     *jcsubs   = &(sr.jres);
    vector<string> mbrs = zh_jv_get_member_names(jcsubs);
    for (uint i = 0; i < mbrs.size(); i++) {
      string schanid = mbrs[i];
      for (uint i = 0; i < jrchans->size(); i++) {
        string rchan = (*jrchans)[i].asString();
        if (!schanid.compare(rchan)) RETURN_SQL_RESULT_UINT(1) // TRUE
      }
    }
    RETURN_EMPTY_SQLRES // FALSE
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT CACHE ---------------------------------------------------------------

static sqlres set_wildcard_perms(jv *jrchans, string &perms) {
  string username = zh_wildcard_user;
  for (uint i = 0; i < jrchans->size(); i++) {
    string rchan = (*jrchans)[i].asString();
    sqlres sr    = persist_user_permission(username, rchan, perms);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres check_wildcard_perms(jv *jcrdt, jv *jcperms, jv *jauth) {
  if (jcperms->compare("R*") && jcperms->compare("W*")) RETURN_EMPTY_SQLRES
  else {
    jv     *jrchans = &((*jcrdt)["_meta"]["replication_channels"]);
    string  perms   = (!jcperms->compare("R*")) ? "R*" : "W*";
    return set_wildcard_perms(jrchans, perms);
  }
}

static void cache_process_subscriber_merge(jv *jmd, bool is_cache) {
  jv     *jks      = &((*jmd)["ks"]);
  jv     *jcrdt    = &((*jmd)["crdt"]);
  jv     *jcavrsns = &((*jmd)["central_agent_versions"]);
  jv     *jgcsumms = &((*jmd)["gc_summary"]);
  jv     *jether   = &((*jmd)["ether"]);
  bool    remove   = zh_get_bool_member(jmd, "remove");
  (void)do_process_subscriber_merge(jks, jcrdt, jcavrsns, jgcsumms, jether,
                                    remove, is_cache);
}

static sqlres do_process_ack_agent_cache(jv *jauth, bool pin, bool watch,
                                         bool sticky, jv *jmd, string &kqk) {
  LD("<<<<(C): HandleCentralAckCache: K: " << kqk);
  jv     *jcrdt     = &((*jmd)["crdt"]);;
  sqlres  sr        = zcache_agent_store_cache_key_metadata(kqk, watch, jauth);
  RETURN_SQL_ERROR(sr)
  sr                = post_cache_lru_update(kqk, pin, sticky);
  RETURN_SQL_ERROR(sr)
  jv     *jcreateds = &((*jcrdt)["_meta"]["created"]);
  sr                =  zdack_set_agent_createds(jcreateds);
  RETURN_SQL_ERROR(sr)
  jv     *jperms    = &((*jmd)["permissions"]);
  sr                = check_wildcard_perms(jcrdt, jperms, jauth);
  RETURN_SQL_ERROR(sr)
  if (!watch) cache_process_subscriber_merge(jmd, true);
  else {
    jv *jrchans = &((*jcrdt)["_meta"]["replication_channels"]);
    sr          = persist_key_repchans(kqk, jrchans);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

sqlres process_ack_agent_cache(lua_State *L, jv *jreq, jv *result) {
  LT("process_ack_agent_cache");
  jv     *jauth  = &((*jreq)["params"]["authentication"]);
  bool    pin    = (*jreq)["params"]["data"]["pin"].asBool();
  bool    watch  = (*jreq)["params"]["data"]["watch"].asBool();
  bool    sticky = (*jreq)["params"]["data"]["sticky"].asBool();
  jv     *jks    = &((*result)["ks"]);
  jv     *jmd    = &((*result)["merge_data"]);
  string  kqk    = (*jks)["kqk"].asString();
  if (!zh_jv_is_member(jmd, "crdt")) {
    RETURN_SQL_RESULT_ERROR(ZS_Errors["NoDataFound"])
  } else {
    return do_process_ack_agent_cache(jauth, pin, watch, sticky, jmd, kqk);
  }
}

static sqlres do_agent_cache_miss() {
  sqlres sr;
  sr.jres["miss"] = true;
  return sr;
}

static sqlres add_crdt_to_applied(string &kqk, jv *jcrdt) {
  jv     applied; //NOTE: Response to request
  applied["crdt"]         = *jcrdt;
  sqlres sr               = zdelt_get_key_subscriber_versions(kqk);
  RETURN_SQL_ERROR(sr)
  applied["dependencies"] = sr.jres;
  RETURN_SQL_RESULT_JSON(applied)
}

static sqlres do_handle_already_cached(string &kqk, bool watch, jv *jfcrdt,
                                       jv *jauth) {
  sqlres sr = zcache_agent_store_cache_key_metadata(kqk, watch, jauth);
  RETURN_SQL_ERROR(sr)
  return add_crdt_to_applied(kqk, jfcrdt);
}

static sqlres handle_already_cached(string &kqk, bool watch,
                                    jv *jfcrdt, jv *jauth) {
  if (zjn(jfcrdt)) return do_agent_cache_miss();
  else             return do_handle_already_cached(kqk, watch, jfcrdt, jauth);
}

static sqlres do_handle_cacheable(string &kqk, bool pin, bool watch, jv *jfcrdt,
                                  jv *jauth) {
  sqlres sr     = fetch_cached_keys(kqk);
  RETURN_SQL_ERROR(sr)
  bool   cached = sr.ures ? true : false;
  if (!cached) return do_agent_cache_miss(); // NOT-CACHED
  else {                                     // ALREADY-CACHED
    LD("ALREADY-CACHED: K: " << kqk);
    if (zjd(jfcrdt)) {
      return handle_already_cached(kqk, watch, jfcrdt, jauth);
    } else {
      LD("CACHED BUT NOT PRESENT -> MISS: K: " << kqk);
      return do_agent_cache_miss();
    }
  }
}

static sqlres do_agent_cache(string &kqk, bool pin, bool watch, bool force,
                             jv *jauth) {
  LD("ZCache.AgentCache: K: " << kqk << " P: " << pin << " W: " << watch);
  sqlres  sr = zds_retrieve_crdt(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jfcrdt   = &(sr.jres);
  jv      jrchans  = zjd(jfcrdt) ?
                       (*jfcrdt)["_meta"]["replication_channels"] : JNULL;
  sr               = check_already_subscribed(jauth, &jrchans);
  RETURN_SQL_ERROR(sr)
  bool    issubbed = sr.ures ? true : false;
  LD("check_already_subscribed: HIT: " << issubbed);
  if (issubbed) RETURN_SQL_RESULT_ERROR(ZS_Errors["CacheOnSubscribe"])
  else {
    if (force || watch) return do_agent_cache_miss();
    else {
      return do_handle_cacheable(kqk, pin, watch, jfcrdt, jauth);
    }
  }
}

static sqlres agent_pin_cache_key(string &kqk, bool pin) {
  if (!pin) return remove_lru_key_field(kqk, "PIN");
  else      return persist_lru_key_field(kqk, "PIN", 1);
}

static sqlres agent_sticky_cache_key(string &kqk, bool sticky) {
  if (!sticky) return remove_key_info_field(kqk, "STICKY");
  else         return persist_key_info_field(kqk, "STICKY", 1);
}

static sqlres post_cache_lru_update(string &kqk, bool pin, bool sticky) {
  LT("post_cache_lru_update: PIN: " << pin << " STICKY: " << sticky);
  sqlres sr = agent_pin_cache_key(kqk, pin);
  RETURN_SQL_ERROR(sr)
  return agent_sticky_cache_key(kqk, sticky);
}

static sqlres agent_cache(string &kqk, bool pin, bool watch, bool sticky,
                          bool force, jv *jauth) {
  sqlres rsr  = do_agent_cache(kqk, pin, watch, force, jauth);
  RETURN_SQL_ERROR(rsr)
  bool   creq = rsr.ures ? true : false;
  if (!creq && !zh_jv_is_member(&(rsr.jres), "miss")) {
    sqlres sr = post_cache_lru_update(kqk, pin, sticky);
    RETURN_SQL_ERROR(sr)
  }
  return rsr; // NOTE rsr NOT sr
}

static jv respond_client_cache(string &id, jv *resp) {
  jv response        = zh_create_jv_response_ok(id);
  response["result"] = *resp;
  return response;
}

jv process_client_cache(lua_State *L, string id, jv *params) {
  LT("process_client_cache");
  jv     *jauth  = &((*params)["authentication"]);
  jv     *jpd    = &((*params)["data"]);
  jv      jns    = (*params)["data"]["namespace"];
  jv      jcn    = (*params)["data"]["collection"];
  jv      jkey   = (*params)["data"]["key"];
  bool    pin    = zh_get_bool_member(jpd, "pin");
  bool    watch  = zh_get_bool_member(jpd, "watch");
  bool    sticky = zh_get_bool_member(jpd, "sticky");
  string  kqk    = zs_create_kqk(jns, jcn, jkey);
  LD("process_client_cache: key: " << kqk << " P: " << pin <<
     " W: " << watch << " S: " << sticky);
  bool    force = false;
  sqlres sr = agent_cache(kqk, pin, watch, sticky, force, jauth);
  PROCESS_GENERIC_ERROR(id, sr)
  bool    creq  = sr.ures ? true : false;
  jv     *jresp = &(sr.jres);
  if (!creq) return respond_client_cache(id, jresp);
  else       return *jresp;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT EVICT ---------------------------------------------------------------

static sqlres agent_remove_subscriber_versions(string &kqk) {
  LT("agent_remove_subscriber_versions");
  sqlres sr = fetch_subscriber_delta_aversions(kqk);
  RETURN_SQL_ERROR(sr)
  jv *jsvrsns = &(sr.jres);
  for (uint i = 0; i < jsvrsns->size(); i++) {
    string svrsn = (*jsvrsns)[i].asString();
    sr           = remove_subscriber_delta_version(kqk, svrsn);
    RETURN_SQL_ERROR(sr)
    //TODO remove_delta() ??
    vector<string> res    = split(svrsn, '|');
    string         sauuid = res[0];
    UInt64         auuid  = strtoul(sauuid.c_str(), NULL, 10);
    sr                    = remove_device_subscriber_version(kqk, auuid);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres agent_remove_key_metadata(string &kqk) {
  LT("agent_remove_key_metadata");
  // NOTE: ZMDC.RemoveKeyToDevices() not needed -> not tracked
  sqlres sr = remove_gc_version(kqk);
  RETURN_SQL_ERROR(sr)
  sr        = remove_agent_key_max_gc_version(kqk);
  RETURN_SQL_ERROR(sr)
  sr        = remove_key_repchans(kqk);
  RETURN_SQL_ERROR(sr)
  sr        = agent_remove_subscriber_versions(kqk);
  RETURN_SQL_ERROR(sr)
  return remove_lru_key(kqk);
}

static sqlres remove_cache_key_metadata(string &kqk) {
  LT("remove_cache_key_metadata");
  return remove_cached_keys(kqk);
  // NOTE: ZS.GetDeviceToKeys() NOT USED
}

static sqlres agent_remove_cache_key_user_to_key(string &kqk) {
  LT("agent_remove_cache_key_user_to_key");
  sqlres          sr     = fetch_users_caching_key(kqk);
  RETURN_SQL_ERROR(sr)
  jv             *jusers = &(sr.jres);
  vector<string>  mbrs   = zh_jv_get_member_names(jusers);
  for (uint i = 0; i < mbrs.size(); i++) {
    string username = mbrs[i];
    sr              = remove_key_cached_by_user(kqk, username);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres agent_remove_cache_key_metadata(string &kqk) {
  LT("agent_remove_cache_key_metadata");
  sqlres sr = remove_cache_key_metadata(kqk);
  RETURN_SQL_ERROR(sr)
  sr        = remove_agent_watch_keys(kqk);
  RETURN_SQL_ERROR(sr)
  return agent_remove_cache_key_user_to_key(kqk);
}

static sqlres do_evict_remove_key(string &kqk) {
  sqlres sr = zsm_set_key_in_sync(kqk);
  RETURN_SQL_ERROR(sr)
  sr        = zds_remove_key(kqk);
  RETURN_SQL_ERROR(sr)
  return persist_evicted(kqk);
}

static sqlres do_agent_evict(jv *jks, bool nm) {
  string kqk = (*jks)["kqk"].asString();
  LT("do_agent_evict: K: " << kqk);
  sqlres sr  = agent_remove_key_metadata(kqk);
  RETURN_SQL_ERROR(sr)
  sr         = agent_remove_cache_key_metadata(kqk);
  RETURN_SQL_ERROR(sr)
  sr         = do_evict_remove_key(kqk);
  RETURN_SQL_ERROR(sr)
  if (!nm) RETURN_EMPTY_SQLRES
  else     return zas_set_agent_sync_key_signal_agent_to_sync_keys(jks);
}

static sqlres do_agent_local_evict(string &kqk) {
  sqlres sr = do_evict_remove_key(kqk);
  RETURN_SQL_ERROR(sr)
  return set_key_watch(kqk);
}

static bool send_central_evict(jv *jks, bool nm) {
  string kqk      = (*jks)["kqk"].asString();
  LT("send_central_evict: K: " << kqk << " NM: " << nm);
  jv     prequest = zh_create_json_rpc_body("AgentEvict", &zh_nobody_auth);
  prequest["params"]["data"]["ks"]         = *jks;
  prequest["params"]["data"]["need_merge"] = nm;
  return zexh_send_central_https_request(0, prequest, false);
}

sqlres zcache_internal_evict(jv *jks, bool send_central, bool nm) {
  LT("zcache_internal_evict: SC: " << send_central << " NM: " << nm);
  if (send_central) { // ACK AGENT-EVICT will call do_agent_evict
    (void)send_central_evict(jks, nm);
  } else {
    sqlres sr = do_agent_evict(jks, nm);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

sqlres process_ack_agent_local_evict(lua_State *L, jv *jreq, jv *result) {
  LT("process_ack_agent_local_evict");
  jv     *jks = &((*result)["ks"]);
  string  kqk = (*jks)["kqk"].asString();
  return do_agent_local_evict(kqk);
}

sqlres process_ack_agent_evict(lua_State *L, jv *jreq, jv *result) {
  LT("process_ack_agent_evict");
  jv   *jks = &((*result)["ks"]);
  bool  nm  = zh_get_bool_member(result, "need_merge");
  return do_agent_evict(jks, nm);
}

