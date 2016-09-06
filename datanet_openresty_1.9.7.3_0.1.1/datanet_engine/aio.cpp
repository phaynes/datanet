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

#include "deltas.h"
#include "dack.h"
#include "isolated.h"
#include "engine.h"

using namespace std;

extern string AgentCallbackKey;

extern jv zh_nobody_auth;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEVICE ONLINE -------------------------------------------------------------

jv zaio_build_agent_online_request(bool b) {
  sqlres sr       = zdack_get_agent_last_created();
  RETURN_SQL_ERROR_AS_JNULL(sr)
  Int64  created  = sr.ures;
  sr              = zisl_get_agent_info();
  RETURN_SQL_ERROR_AS_JNULL(sr)
  jv     ainfo    = sr.jres;
  jv     prequest = zh_create_json_rpc_body("AgentOnline", &zh_nobody_auth);
  prequest["params"]["data"]["created"] = created;
  prequest["params"]["data"]["value"]   = b;
  if (zjd(&ainfo)) {
    if (zh_jv_is_member(&ainfo, "permissions")) {
      prequest["params"]["data"]["permissions"]    = ainfo["permissions"];
    }
    if (zh_jv_is_member(&ainfo, "subscriptions")) {
      prequest["params"]["data"]["subscriptions"]  = ainfo["subscriptions"];
    }
    if (zh_jv_is_member(&ainfo, "stationedusers")) {
      prequest["params"]["data"]["stationedusers"] = ainfo["stationedusers"];
    }
  }
  sr = fetch_callback_server_hostname();
  RETURN_SQL_ERROR_AS_JNULL(sr)
  string cbhost = sr.sres;
  prequest["params"]["data"]["server"]["hostname"] = cbhost;
  sr = fetch_callback_server_port();
  RETURN_SQL_ERROR_AS_JNULL(sr)
  UInt64 cbport = sr.ures;
  prequest["params"]["data"]["server"]["port"]     = cbport;
  prequest["params"]["data"]["server"]["key"]      = AgentCallbackKey;
  return prequest;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DELTA ---------------------------------------------------------------

void zaio_cleanup_delta_before_send_central(jv *jdentry) {
  LT("cleanup_delta_before_send_central");
  jv *jmeta = &((*jdentry)["delta"]["_meta"]);
  jmeta->removeMember("dirty_central");
  zdelt_delete_meta_timings(jdentry);
  (*jmeta)["agent_sent"] = zh_get_ms_time();
}


