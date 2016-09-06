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
#include "shared.h"
#include "deltas.h"
#include "subscriber_delta.h"
#include "apply_delta.h"
#include "auth.h"
#include "datastore.h"
#include "trace.h"
#include "storage.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HEARTBEAT -----------------------------------------------------------------

static void init_increment_heartbeat(jv &adh);
static void init_timestamp_heartbeat(jv &adh);
static void init_array_heartbeat    (jv &adh);

void zhb_init_heartbeat(bool persist) {
  jv jinc;
  jinc["Auth"]     = JNULL;
  jinc["Field"]    = JNULL;
  jv juuid;
  juuid["Auth"]    = JNULL;
  juuid["UUID"]    = JNULL;
  juuid["MaxSize"] = JNULL;
  juuid["Trim"]    = JNULL;
  jv adh;
  adh["Started"]    = false;
  adh["Increment"]  = jinc;
  adh["Timestamp"]  = juuid;
  adh["Array"]      = juuid;
  adh["Namespace"]  = "production";
  adh["Collection"] = "statistics";
  adh["IKey"]       = "INCREMENT_HEARTBEAT";
  adh["LKey"]       = "TIMESTAMP_HEARTBEAT";
  adh["AKey"]       = "ARRAY_HEARTBEAT";
  if (persist) {
    LD("zhb_init_heartbeat->persist_agent_heartbeat_info");
    persist_agent_heartbeat_info(&adh);
  }
  init_increment_heartbeat(adh);
  init_timestamp_heartbeat(adh);
  init_array_heartbeat    (adh);
}

jv ADH_Increment_JSON;
jv ADH_Timestamp_JSON;
jv ADH_Array_JSON;

static void init_increment_heartbeat(jv &adh) {
  ADH_Increment_JSON["_id"]       = adh["IKey"];
  ADH_Increment_JSON["_channels"] = JARRAY;
  jv jzero = "0";
  zh_jv_append(&(ADH_Increment_JSON["_channels"]), &jzero);
}

static void init_timestamp_heartbeat(jv &adh) {
  ADH_Timestamp_JSON["_id"]               = adh["LKey"];
  ADH_Timestamp_JSON["_channels"]         = JARRAY;
  jv jzero = "0";
  zh_jv_append(&(ADH_Timestamp_JSON["_channels"]), &jzero);
  ADH_Timestamp_JSON["CONTENTS"]["_data"] = JARRAY;
  ADH_Timestamp_JSON["CONTENTS"]["_type"] = "LIST";
  jv jmd;
  jmd["ORDERED"] = true;
  ADH_Timestamp_JSON["CONTENTS"]["_metadata"] = jmd; //NOTE: METADATA:{ORDERED}
}

static void init_array_heartbeat(jv &adh) {
  ADH_Array_JSON["_id"]                   = adh["AKey"];
  ADH_Array_JSON["_channels"]             = JARRAY;
  jv jzero = "0";
  zh_jv_append(&(ADH_Array_JSON["_channels"]), &jzero);
  ADH_Array_JSON["CONTENTS"]["_data"]     = JARRAY;
  ADH_Array_JSON["CONTENTS"]["_type"]     = "LIST";
  ADH_Array_JSON["CONTENTS"]["_metadata"] = JOBJECT; //NOTE: EMPTY METADATA
}

static string get_adh_increment_kqk(jv &adh) {
  return zh_create_kqk(adh["Namespace"].asString(),
                       adh["Collection"].asString(),
                       adh["IKey"].asString());
}

static string get_adh_timestamp_kqk(jv &adh) {
  return zh_create_kqk(adh["Namespace"].asString(),
                       adh["Collection"].asString(),
                       adh["LKey"].asString());
}

static string get_adh_array_kqk(jv &adh) {
  return zh_create_kqk(adh["Namespace"].asString(),
                       adh["Collection"].asString(),
                       adh["AKey"].asString());
}

static jv create_adh_increment_json(string field) {
  jv jjson = ADH_Increment_JSON;
  jjson[field] = 1;
  return jjson;
}

static jv create_adh_uuid_json(bool isa, string uuid, Int64 mlen, Int64 trim) {
  jv jjson = isa ? ADH_Array_JSON : ADH_Timestamp_JSON;
  jjson["CONTENTS"]["_metadata"]["MAX-SIZE"] = mlen;
  jjson["CONTENTS"]["_metadata"]["TRIM"]     = trim;
  return jjson;
}

static jv create_adh_increment_oplog(string field, bool do_incr) {
  jv joplog = JARRAY;
  jv jop;
  jop["name"] = do_incr ? "increment" : "set";
  jop["path"] = field;
  jop["args"] = JARRAY;
  jv jone = 1;
  zh_jv_append(&(jop["args"]), &jone);
  zh_jv_append(&joplog, &jop);
  return joplog;
}

static string create_adh_uuid_entry(string uuid) {
  UInt64 now = zh_get_ms_time();
  // NOTE: 'now' comes first for sorting purposes
  return to_string(now) + "-" + uuid;
}

static jv create_adh_uuid_oplog(string uuid, UInt64 clen) {
  string entry  = create_adh_uuid_entry(uuid);
  jv     jentry = entry;
  jv     joplog = JARRAY;
  jv     jclen  = clen;
  jv jop;
  jop["name"] = "insert";
  jop["path"] = "CONTENTS";
  jop["args"] = JARRAY;
  zh_jv_append(&(jop["args"]), &jclen);
  zh_jv_append(&(jop["args"]), &jentry);
  zh_jv_append(&joplog, &jop);
  return joplog;
}

static sqlres do_uuid_heartbeat(string &id, jv *jauth,
                                string &kqk, bool isa, jv *params) {
  string  uuid = (*params)["data"]["uuid"].asString();
  Int64   mlen = (*params)["data"]["max_size"].asInt64();
  UInt64  trim = (*params)["data"]["trim"].asUInt64();
  LD("do_uuid_heartbeat: K: " << kqk << " ISA: " << isa << " U: " << uuid <<
     " M: " << mlen << " T: " << trim);
  sqlres  sr      = zds_retrieve_crdt(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jcrdt   = &(sr.jres);
  bool    initial = zjn(jcrdt);
  LD("do_uuid_heartbeat: INITIAL: " << initial);
  if (initial) {
    jv      jjson   = create_adh_uuid_json(isa, uuid, mlen, trim);
    jv     *jrchans = &(jjson["_channels"]);
    sqlres  sr      = zauth_has_write_permissions(jauth, kqk, jrchans);
    RETURN_SQL_ERROR(sr)
    bool    ok      = sr.ures ? true : false;
    if (!ok) RETURN_SQL_RESULT_ERROR(ZS_Errors["WritePermsFail"])
    (*params)["data"]["json"] = jjson;
    (void)internal_process_client_store(id, params, true);
  } else {
    jv     *jmeta   = &((*jcrdt)["_meta"]);
    jv     *jrchans = zh_get_replication_channels(jmeta);
    sqlres  sr      = zauth_has_write_permissions(jauth, kqk, jrchans);
    RETURN_SQL_ERROR(sr)
    bool    ok      = sr.ures ? true : false;
    if (!ok) RETURN_SQL_RESULT_ERROR(ZS_Errors["WritePermsFail"])
    jv      jjson   = zh_create_pretty_json(jcrdt);
    UInt64  clen    = jjson["CONTENTS"].size();
    jv      joplog  = create_adh_uuid_oplog(uuid, clen);
    //zh_debug_json_value("joplog", &joplog);
    UInt64   mc     = (*jmeta)["member_count"].asUInt64();
    (*jcrdt)["_meta"]["last_member_count"] = mc;
    (*params)["data"]["crdt"]              = *jcrdt;
    (*params)["data"]["oplog"]             = joplog;
    ztrace_start("DELT.ZHB.SEND.DO.internal_process_client_commit");
    (void)internal_process_client_commit(id, params, true);
    ztrace_finish("DELT.ZHB.SEND.DO.internal_process_client_commit");
  }
  RETURN_EMPTY_SQLRES
}

static sqlres do_increment_heartbeat(string &id, jv *jauth,
                                     string &kqk, jv *params) {
  string  field   = (*params)["data"]["field"].asString();
  LD("do_increment_heartbeat: K: " << kqk << " F: " << field);
  sqlres  sr      = zds_retrieve_crdt(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jcrdt   = &(sr.jres);
  bool    initial = zjn(jcrdt);
  if (initial) {
    jv      jjson   = create_adh_increment_json(field);
    jv     *jrchans = &(jjson["_channels"]);
    sqlres  sr      = zauth_has_write_permissions(jauth, kqk, jrchans);
    RETURN_SQL_ERROR(sr)
    bool    ok      = sr.ures ? true : false;
    if (!ok) RETURN_SQL_RESULT_ERROR(ZS_Errors["WritePermsFail"])
    (*params)["data"]["json"] = jjson;
    (void)internal_process_client_store(id, params, true);
  } else {
    jv     *jmeta   = &((*jcrdt)["_meta"]);
    jv     *jrchans = zh_get_replication_channels(jmeta);
    sqlres  sr      = zauth_has_write_permissions(jauth, kqk, jrchans);
    RETURN_SQL_ERROR(sr)
    bool    ok      = sr.ures ? true : false;
    if (!ok) RETURN_SQL_RESULT_ERROR(ZS_Errors["WritePermsFail"])
    jv      jjson   = zh_create_pretty_json(jcrdt);
    jv     *jfound  = zh_lookup_by_dot_notation(&jjson, field);
    bool    do_incr = jfound ? true : false;
    jv      joplog  = create_adh_increment_oplog(field, do_incr);
    //zh_debug_json_value("joplog", &joplog);
    UInt64  mc      = (*jmeta)["member_count"].asUInt64();
    (*jcrdt)["_meta"]["last_member_count"] = mc;
    (*params)["data"]["crdt"]              = *jcrdt;
    (*params)["data"]["oplog"]             = joplog;
    (void)internal_process_client_commit(id, params, true);
  }
  RETURN_EMPTY_SQLRES
}

static jv process_internal_heartbeat(string id, jv *params) {
  jv     *jauth = &((*params)["authentication"]);
  jv      jns   = (*params)["data"]["namespace"];
  jv      jcn   = (*params)["data"]["collection"];
  jv      jkey  = (*params)["data"]["key"];
  bool    isi   = (*params)["data"]["is_increment"].asBool();
  bool    isa   = (*params)["data"]["is_array"].asBool();
  string  kqk   = zs_create_kqk(jns, jcn, jkey);
  LE("process_internal_heartbeat: K: " << kqk << " ISI: " << isi <<
     " ISA: " << isa);
  if (isi) do_increment_heartbeat(id, jauth, kqk,      params);
  else     do_uuid_heartbeat     (id, jauth, kqk, isa, params);
  return JNULL;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DATA HEARBEAT FLOWS -------------------------------------------------

static jv create_internal_heartbeat_request(jv &adh, string &method, jv *jauth,
                                            bool isi, bool isa) {
  jv irequest = zh_create_json_rpc_body(method, jauth);
  irequest["params"]["data"]["namespace"]  = adh["Namespace"];
  irequest["params"]["data"]["collection"] = adh["Collection"];
  if (isi) {
    irequest["params"]["data"]["key"]      = adh["IKey"];
  } else if (isa) {
    irequest["params"]["data"]["key"]      = adh["AKey"];
  } else {
    irequest["params"]["data"]["key"]      = adh["LKey"];
  }
  return irequest;
}

static sqlres send_uuid_heartbeat(jv &adh, string &kqk, bool isa) {
  LT("send_uuid_heartbeat: isa: " << isa);
  ztrace_start("DELT.ZHB.SEND.send_uuid_heartbeat");
  jv     *jahb     = isa ? &(adh["Array"]) :
                           &(adh["Timestamp"]);
  jv     *jauth    = &((*jahb)["Auth"]);
  string  uuid     = (*jahb)["UUID"].asString();
  Int64   mlen     = (*jahb)["MaxSize"].asInt64();
  Int64   trim     = (*jahb)["Trim"].asInt64();
  string  method   = "InternalHeartbeat";
  jv      irequest = create_internal_heartbeat_request(adh, method, jauth,
                                                       false, isa);
  string  id       = irequest["id"].asString();
  irequest["params"]["data"]["uuid"]         = uuid;
  irequest["params"]["data"]["max_size"]     = mlen;
  irequest["params"]["data"]["trim"]         = trim;
  irequest["params"]["data"]["is_increment"] = false;
  irequest["params"]["data"]["is_array"]     = isa;
  (void)process_internal_heartbeat(id, &(irequest["params"]));
  ztrace_finish("DELT.ZHB.SEND.send_uuid_heartbeat");
  RETURN_EMPTY_SQLRES
}

static sqlres send_increment_heartbeat(jv &adh, string &kqk) {
  sqlres  sr;
  jv     *jahb     = &(adh["Increment"]);
  jv     *jauth    = &((*jahb)["Auth"]);
  string  field    = (*jahb)["Field"].asString();
  string  method   = "InternalHeartbeat";
  jv      irequest = create_internal_heartbeat_request(adh, method, jauth,
                                                       true, false);
  string  id       = irequest["id"].asString();
  irequest["params"]["data"]["field"]        = field;
  irequest["params"]["data"]["is_increment"] = true;
  irequest["params"]["data"]["is_array"]     = false;
  (void)process_internal_heartbeat(id, &(irequest["params"]));
  RETURN_EMPTY_SQLRES
}

static sqlres do_flow_heartbeat(jv &adh, bool isi, bool isa) {
  string kqk = isi ? get_adh_increment_kqk(adh) :
               isa ? get_adh_array_kqk(adh)     :
                     get_adh_timestamp_kqk(adh);
  if (!zh_is_key_local_to_worker(kqk)) RETURN_EMPTY_SQLRES
  else {
    jv     md;
    sqlres sr     = zsd_get_agent_sync_status(kqk, &md);
    RETURN_SQL_ERROR(sr)
    bool   tosync = zh_get_bool_member(&md, "to_sync_key");
    bool   oosync = zh_get_bool_member(&md, "out_of_sync_key");
    if (oosync || tosync) {
      LE("ZHB: OUT-OF-SYNC/TO-SYNC K: " << kqk << " -> NO-OP");
      RETURN_EMPTY_SQLRES
    } else {
      if (isi) return send_increment_heartbeat(adh, kqk);
      else     return send_uuid_heartbeat     (adh, kqk, isa);
    }
  }
}

void zhb_do_heartbeat() {
  sqlres sr  = fetch_agent_heartbeat_info();
  if (sr.err.size()) return;
  jv     adh = sr.jres;
  bool running = adh["Started"].asBool();
  //LD("zhb_do_heartbeat: running: " << running);
  if (running) {
    bool isi = zjd(&adh["Increment"]["Field"]);
    bool isa = zjd(&adh["Array"]["UUID"]);
    bool ist = zjd(&adh["Timestamp"]["UUID"]);
    LD("zhb_do_heartbeat: isi: " << isi << " isa: " << isa << " ist: " << ist);
    ztrace_start("DELT.ZHB.do_heartbeat");
    if (isi) do_flow_heartbeat(adh, true,  false);
    if (isa) do_flow_heartbeat(adh, false, true);
    if (ist) do_flow_heartbeat(adh, false, false);
    ztrace_finish("DELT.ZHB.do_heartbeat");
  }
}

static void start_agent_data_heartbeat(jv &adh) {
  LD("start_agent_data_heartbeat");
  adh["Started"] = true;
}

static void stop_agent_data_heartbeat(jv &adh) {
  LD("stop_agent_data_heartbeat");
  adh["Started"] = false;
}

static jv respond_client_heartbeat(jv &adh, string &id) {
  jv response                     = zh_create_jv_response_ok(id);
  response["result"]["increment"] = adh["Increment"];
  response["result"]["array"]     = adh["Array"];
  response["result"]["timestamp"] = adh["Timestamp"];
  if (zjd(&(adh["Increment"]["Auth"]))) {
    response["result"]["increment"]["Auth"].removeMember("password");
  }
  if (zjd(&(adh["Array"]["Auth"]))) {
    response["result"]["array"]["Auth"].removeMember("password");
  }
  if (zjd(&(adh["Timestamp"]["Auth"]))) {
    response["result"]["timestamp"]["Auth"].removeMember("password");
  }
  bool iactive = zjd(&(adh["Increment"]["Field"]));
  response["result"]["increment"]["active"] = iactive;
  bool aactive = zjd(&(adh["Array"]["UUID"]));
  response["result"]["array"]["active"]     = aactive;
  bool tactive = zjd(&(adh["Timestamp"]["UUID"]));
  response["result"]["timestamp"]["active"] = tactive;
  return response;
}

static jv agent_data_heartbeat(string id, string cmd, string field,
                               string uuid, Int64 mlen, Int64 trim,
                               bool isi, bool isa, jv *jauth) {
  sqlres sr = fetch_agent_heartbeat_info();
  if (sr.err.size()) return JNULL;
  jv adh = sr.jres;
  transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
  if (field.size()) LD("cmd: " << cmd << " field: " << field);
  else              LD("cmd: " << cmd << " uuid: "  << uuid);
  if        (!cmd.compare("START")) {
    if (!field.size() && !uuid.size()) {
      return zh_process_error(id, -32007, ZS_Errors["AgentHeartbeatBadStart"]);
    }
    if (isi) {        // INCREMENT-HEARTBEAT
      adh["Increment"]["Auth"] = *jauth;
      if (field.size()) adh["Increment"]["Field"] = field;
    } else if (isa) { // ARRAY-HEARTBEAT
      adh["Array"]["Auth"] = *jauth;
      if (uuid.size()) adh["Array"]["UUID"]    = uuid;
      if (mlen)        adh["Array"]["MaxSize"] = mlen;
      if (trim)        adh["Array"]["Trim"]    = trim;
    } else {          // TIMESTAMP-HEARTBEAT
      adh["Timestamp"]["Auth"] = *jauth;
      if (uuid.size()) adh["Timestamp"]["UUID"]    = uuid;
      if (mlen)        adh["Timestamp"]["MaxSize"] = mlen;
      if (trim)        adh["Timestamp"]["Trim"]    = trim;
    }
    start_agent_data_heartbeat(adh);
    persist_agent_heartbeat_info(&adh);
    return respond_client_heartbeat(adh, id);
  } else if (!cmd.compare("STOP")) {
    if (isi) {        // INCREMENT-HEARTBEAT
      adh["Increment"]["Auth"]  = JNULL;
      adh["Increment"]["Field"] = JNULL;
    } else if (isa) { // ARRAY-HEARTBEAT
      adh["Array"]["Auth"]    = JNULL;
      adh["Array"]["UUID"]    = JNULL;
      adh["Array"]["MaxSize"] = JNULL;
      adh["Array"]["Trim"]    = JNULL;
    } else {          // TIMESTAMP-HEARTBEAT
      adh["Timestamp"]["Auth"]    = JNULL;
      adh["Timestamp"]["UUID"]    = JNULL;
      adh["Timestamp"]["MaxSize"] = JNULL;
      adh["Timestamp"]["Trim"]    = JNULL;
    }
    if (zjn(&(adh["Increment"]["Auth"])) &&
        zjn(&(adh["Array"]["Auth"])) &&
        zjn(&(adh["Timestamp"]["Auth"]))) {
      LD("ALL HEARTBEATS (INCREMENT,TIMESTAMP,ARRAY) STOPPED");
      stop_agent_data_heartbeat(adh);
    }
    persist_agent_heartbeat_info(&adh);
    return respond_client_heartbeat(adh, id);
  } else if (!cmd.compare("QUERY")) {
    return respond_client_heartbeat(adh, id);
  } else {
    return zh_process_error(id, -32007, ZS_Errors["AgentHeartbeatUsage"]);
  }
}

jv process_client_heartbeat(lua_State *L, string id, jv *params) {
  LT("process_client_heartbeat");
  jv     *jauth = &((*params)["authentication"]);
  jv     jcmd   = (*params)["data"]["command"];
  jv     jfield = (*params)["data"]["field"];
  jv     juuid  = (*params)["data"]["uuid"];
  jv     jmlen  = (*params)["data"]["max_size"];
  jv     jtrim  = (*params)["data"]["trim"];
  bool   isi    = (*params)["data"]["is_increment"].asBool();
  bool   isa    = (*params)["data"]["is_array"].asBool();
  string field  = zjn(&jfield) ? "" : jfield.asString(); // Either INCR or
  string uuid   = zjn(&juuid)  ? "" : juuid.asString();  // Ordered-List
  string smlen  = zjn(&jmlen)  ? "" : jmlen.asString();
  string strim  = zjn(&jtrim)  ? "" : jtrim.asString();
  Int64  mlen   = smlen.size() ? atol(smlen.c_str()) : 0;
  Int64  trim   = strim.size() ? atol(strim.c_str()) : 0;
  string cmd    = jcmd.asString();
  if (isi) {
    LD("ZHB.AgentDataHeartbeat: CMD: " << cmd << " F: " << field);
  } else {
    LD("ZHB.AgentDataHeartbeat: CMD: " << cmd << " U: " << uuid <<
       " MS: " << mlen << " TRIM: " << trim);
  }
  return agent_data_heartbeat(id, cmd, field, uuid,
                              mlen, trim, isi, isa, jauth);
}

