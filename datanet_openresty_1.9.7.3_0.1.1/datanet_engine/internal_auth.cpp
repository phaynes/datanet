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
#include "shared.h"
#include "auth.h"
#include "storage.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG SETTINGS ------------------------------------------------------------

bool DebugFailInternalAuthorization = false;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INTERNAL/PRIMARY AUTHORIZATION --------------------------------------------

static jv respond_client_authorize(string &id, bool miss, bool ok) {
  jv response                = zh_create_jv_response_ok(id);
  response["result"]["miss"] = miss;
  response["result"]["ok"]   = ok;
  return response;
}

static jv parse_client_authorize(jv *params) {
  jv jinfo;
  jv *jpd = &((*params)["data"]);
  jinfo["atype"] = (*params)["data"]["authorization_type"].asString();
  if (zh_jv_is_member(jpd, "kqk")) {
    jinfo["kqk"] = (*jpd)["kqk"];
  }
  if (zh_jv_is_member(jpd, "channel") &&
      zh_jv_is_member(&((*jpd)["channel"]), "id")) {
    jinfo["schanid"] = (*jpd)["channel"]["id"];
  }
  if (zh_jv_is_member(jpd, "replication_channels")) {
    jinfo["rchans"] = (*jpd)["replication_channels"];
  }
  return jinfo;
}

jv process_internal_client_authorize(lua_State *L, string id, jv *params) {
  jv     *jauth = &((*params)["authentication"]);
  jv      jinfo = parse_client_authorize(params);
  string  atype = jinfo["atype"].asString();
  string  kqk;
  jv      jrchans;
  string  schanid;
  if (zh_jv_is_member(&jinfo, "kqk"))     kqk     = jinfo["kqk"].asString();
  if (zh_jv_is_member(&jinfo, "rchans"))  jrchans = jinfo["rchans"];
  if (zh_jv_is_member(&jinfo, "schanid")) schanid = jinfo["schanid"].asString();
  LD("process_internal_client_authorize: atype: " << atype <<
     " kqk: " << kqk << " schanid: " << schanid);
  bool miss = false;
  sqlres sr;
  if        (!atype.compare("BASIC")) {
    sr   = zauth_basic_authentication(jauth);
    miss = !sr.err.compare(ZS_Errors["AuthenticationMiss"]);
  } else if (!atype.compare("READ")) {
    sr   = zauth_has_read_permissions(jauth, kqk);
    miss = !sr.err.compare(ZS_Errors["NoDataFound"]);
  } else if (!atype.compare("WRITE")) {
    sr   = zauth_has_write_permissions_on_key(jauth, kqk);
    miss = !sr.err.compare(ZS_Errors["NoDataFound"]);
  } else if (!atype.compare("STORE")) {
    sr   = zauth_has_agent_store_permissions(jauth, kqk, &jrchans);
    miss = !sr.err.compare(ZS_Errors["AuthenticationMiss"]);
  } else if (!atype.compare("SUBSCRIBE")) {
    sr   = zauth_has_subscribe_permissions(jauth, schanid);
    miss = !sr.err.compare(ZS_Errors["AuthenticationMiss"]);
  }
  bool ok = sr.ures ? true : false;
  if (DebugFailInternalAuthorization) { miss = true; ok = false; }
  LD("process_internal_client_authorize: MISS: " << miss << " OK: " << ok);
  return respond_client_authorize(id, miss, ok);
}

static sqlres store_agent_authenticated_user(jv *jauth, string &role) {
  string username = (*jauth)["username"].asString();
  LT("store_agent_authenticated_user: U: " << username);
  string password = (*jauth)["password"].asString();
  string hash     = zh_local_hash(password);
  return persist_user_authentication(username, hash, role);
}

sqlres process_ack_agent_authenticate(lua_State *L, jv *jreq, jv *result) {
  LD("ACK AGENT AUTHENTICATE");
  bool ok = (*result)["ok"].asBool();
  if (!ok) RETURN_EMPTY_SQLRES
  else {
    jv     *jauth = &((*jreq)["params"]["authentication"]);
    string  role  = (*result)["role"].asString();
    return store_agent_authenticated_user(jauth, role);
  }
}

static jv transform_perms_for_persist(jv *perms) {
  jv jperms;
  for (uint i = 0; i < jperms.size(); i++) {
    jv     *jperm   = &((*perms)[i]);
    string  schanid = (*jperm)["channel"]["id"].asString();
    string  sperms  = (*jperm)["channel"]["permissions"].asString();
    jperms[schanid] = sperms;
  }
  return jperms;
}

sqlres process_ack_agent_get_user_chan_perms(lua_State *L, jv *jreq,
                                             jv *result) {
  LD("ACK AgentGetUserChannelPermissions");
  string status = (*result)["status"].asString();
  bool   ok     = status.size() && !status.compare("OK");
  if (!ok) RETURN_EMPTY_SQLRES
  else {
    jv     *jauth    = &((*jreq)["params"]["authentication"]);
    jv     *jrchans  = &((*jreq)["params"]["data"]["replication_channels"]);
    string  kqk      = (*jreq)["params"]["data"]["kqk"].asString();
    string  username = (*jauth)["username"].asString();
    jv     *perms    = &((*result)["permissions"]);
    jv      jperms   = transform_perms_for_persist(perms);
    string  role     = "USER"; // NOTE: JERRY RIG
    sqlres  sr       = store_agent_authenticated_user(jauth, role);
    RETURN_SQL_ERROR(sr)
    sr               = persist_agent_permissions(&jperms);
    RETURN_SQL_ERROR(sr)
    sr               = persist_user_permissions(&username, &jperms);
    RETURN_SQL_ERROR(sr)
    return zauth_has_simple_store_permissions(jauth, kqk, jrchans);
  }
}

sqlres process_ack_agent_has_subscribe_perms(lua_State *L, jv *jreq,
                                            jv *result) {
  LD("ACK AgentHasSubscribePermissions");
  RETURN_EMPTY_SQLRES
}

