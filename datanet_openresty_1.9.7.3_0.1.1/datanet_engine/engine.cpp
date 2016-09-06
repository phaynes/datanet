#include <errno.h>
extern int errno;

#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <queue>
#include <algorithm>
#include <thread>
#include <chrono>
#include <csignal>

extern "C" {
  #include <time.h>
  #include <sys/time.h>
  #include <sys/types.h>
  #include <sys/socket.h>
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
INITIALIZE_EASYLOGGINGPP

#include "helper.h"
#include "shared.h"
#include "convert.h"
#include "oplog.h"
#include "merge.h"
#include "deltas.h"
#include "cache.h"
#include "subscriber_delta.h"
#include "subscriber_merge.h"
#include "apply_delta.h"
#include "ooo_replay.h"
#include "dack.h"
#include "handler.h"
#include "internal_auth.h"
#include "isolated.h"
#include "agent_daemon.h"
#include "activesync.h"
#include "gc.h"
#include "creap.h"
#include "gc_reaper.h"
#include "latency.h"
#include "heartbeat.h"
#include "fixlog.h"
#include "aio.h"
#include "dcompress.h"
#include "notify.h"
#include "signals.h"
#include "trace.h"
#include "storage.h"
#include "engine.h"
#include "lua_engine_hooks.h"
#include "engine_network.h"

using namespace std;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNS -------------------------------------------------------------------

extern UInt64 CacheMaxBytes;    // FROM cache.cpp

extern UInt64 DeltasMaxBytes;  // FROM storage.cpp

extern bool FixLogActive;       // FROM fixlog.cpp

extern pthread_t PrimaryTid;    // FROM engine_network.cpp
extern pthread_t WorkerTid;     // FROM engine_network.cpp
extern pthread_t ReconnectTid;  // FROM engine_network.cpp

extern jv     zh_nobody_auth;   // FROM helper.cpp
extern string DeviceKey;        // FROM helper.cpp

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

bool DisableTrace  = false;

bool DisableFixlog = false;

UInt64 AgentDirtyDeltaMaximumAllowedStaleness = 80;

bool zh_AgentDisableSubscriberLatencies    = true;

bool zh_AgentDisableAgentToSync            = false;
bool zh_AgentDisableAgentDirtyDeltasDaemon = false;
bool zh_AgentDisableAgentGCPurgeDaemon     = false;

bool DebugDaemons = true;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT GLOBALS -------------------------------------------------------------

// NOTE: C++ GLOBAL-LUA-STATE works because ALL C++ calls are 100% non-blocking
lua_State   *GL = NULL;

Int64        MyUUID               = -1;
string       MyDataCenterUUID     = "";
string       ConfigDataCenterName = "";
bool         AgentSynced          = false;
jv           AgentGeoNodes;
jv           CentralMaster;
UInt64       AgentLastReconnect   = 0;
bool         AgentConnected       = false;
bool         Isolation            = false;
string       AgentCallbackKey;
string       PidFile;

log_level_t  LogLevel             = TRACE; // DEFAULT: TRACE


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PROCESS GLOBALS -----------------------------------------------------------

Int64  MyWorkerUUID     = -1;
Int64  MyWorkerPort     = -1;
bool   AmPrimaryProcess = false;
bool   EnginedInited    = false;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// METHOD MAP ----------------------------------------------------------------

// NOTE: function pointer type
typedef sqlres (*AckProcessFunction)(lua_State *L, jv *jreq, jv *result);
typedef map<string, AckProcessFunction> AckProcessFunctionMap;
AckProcessFunctionMap AckMethodMap;

typedef jv (*LocalClientProcessFunction)(lua_State *L, string id, jv *params);
typedef map<string, LocalClientProcessFunction> LocalClientProcessFunctionMap;
LocalClientProcessFunctionMap LocalClientMethodMap;


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
// STARTUP -------------------------------------------------------------------

static bool write_pid_file() {
  LT("write_pid_file: PidFile: " << PidFile);
  pid_t pid = getpid();
  ofstream myfile;
  myfile.open(PidFile.c_str());
  myfile << pid << endl;
  myfile.close();
  return true;
}

static bool unlink_pid_file() {
  int ret = unlink(PidFile.c_str());
  if (ret == -1) {
    LE("ERROR: unlink_pid_file: " << PidFile << " DESC: " << strerror(errno));
    return false;
  } else {
    return true;
  }
}

static void crash_handler(int sig) {
  LE("CRASH");
  unlink_pid_file();
  el::base::debug::StackTrace();
  el::Helpers::crashAbort(sig);
}

static bool LoggerInited = false;
static void init_logger(const char *lcfile) {
  if (LoggerInited) return;
  el::Configurations conf(lcfile);
  el::Loggers::reconfigureLogger("default", conf);
  el::Loggers::reconfigureAllLoggers(conf);
  el::Loggers::addFlag(el::LoggingFlag::ImmediateFlush);
  el::Helpers::setCrashHandler(crash_handler);
  LD("init_logger: LCF: " << lcfile);
  LoggerInited = true;
}

static void init_client_method_map();
static void init_ack_method_map();

static void init_basic(const char *lcfile) {
  init_logger(lcfile);
  zh_init();
  zs_init_ZS_Errors();
  zcmp_initialize();
}

static bool init_engine_settings(lua_State *L,       const char *dfile,
                                 const char *lcfile, const char *ldbd) {
  init_basic(lcfile);
  if (!storage_init_nonprimary(L, dfile, ldbd)) return false;
  zhb_init_heartbeat(false); // DO NOT PERSIST
  init_client_method_map();
  init_ack_method_map();
  return true;
}

/* TODO(V2) ON_EXIT
static void cleanup() {
  storage_cleanup();
  LD("EXITING ------------>");
}
*/


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WORKER PORT MAPPINGS ------------------------------------------------------

Int64 engine_get_worker_uuid_for_operation(string kqk) {
  vector<Int64> wids = zh_get_worker_uuids();
  if (wids.size() == 0) return -1;
  unsigned int  hval = zh_crc32(kqk);
  unsigned int  slot = hval % wids.size();
  return wids[slot];
}

int engine_get_primary_worker_port() {
  sqlres sr    = fetch_primary_worker_port();
  if (sr.err.size()) return -1;
  Int64  wport = sr.ures;
  LD("engine_get_primary_worker_port: WP: " << wport);
  return wport;
}

int engine_get_key_worker_port(const char *ckqk) {
  string kqk   = ckqk;
  Int64  wid   = engine_get_worker_uuid_for_operation(kqk);
  sqlres sr    = fetch_worker_port(wid);
  if (sr.err.size()) return -1;
  Int64  wport = sr.ures;
  LD("engine_get_key_worker_port: K: " << kqk << " WP: " << wport);
  return wport;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// METHOD MAP INITIALIZATION -------------------------------------------------

static void init_client_method_map() {
  // LOCAL INTERNAL CLIENT CALLS
  LocalClientMethodMap.insert(make_pair("InternalClientAuthorize",  
                                        &process_internal_client_authorize));

  // LOCAL CLIENT CALLS
  LocalClientMethodMap.insert(make_pair("ClientGetLatencies",
                                      &process_client_get_latencies));
  LocalClientMethodMap.insert(make_pair("ClientGetClusterInfo",
                                      &process_client_get_cluster_info));
  LocalClientMethodMap.insert(make_pair("ClientGetStationedUsers",
                                      &process_client_get_stationed_users));
  LocalClientMethodMap.insert(make_pair("ClientGetAgentSubscriptions",
                                      &process_client_get_agent_subscriptions));
  LocalClientMethodMap.insert(make_pair("ClientGetUserSubscriptions",
                                      &process_client_get_user_subscriptions));

  LocalClientMethodMap.insert(make_pair("ClientFind",
                                        &process_client_find));
  LocalClientMethodMap.insert(make_pair("ClientFetch",
                                        &process_client_fetch));

  LocalClientMethodMap.insert(make_pair("ClientStore",
                                        &process_client_store));
  LocalClientMethodMap.insert(make_pair("ClientCommit",
                                        &process_client_commit));
  LocalClientMethodMap.insert(make_pair("ClientRemove",
                                        &process_client_remove));
  LocalClientMethodMap.insert(make_pair("InternalCommit",
                                        &process_internal_commit));

  LocalClientMethodMap.insert(make_pair("ClientCache",
                                        &process_client_cache));

  LocalClientMethodMap.insert(make_pair("ClientExpire",
                                        &process_client_expire));

  LocalClientMethodMap.insert(make_pair("ClientHeartbeat",
                                        &process_client_heartbeat));
  LocalClientMethodMap.insert(make_pair("InternalHeartbeat",
                                        &process_client_heartbeat));

  LocalClientMethodMap.insert(make_pair("ClientIsolation",
                                        &process_client_isolation));

  LocalClientMethodMap.insert(make_pair("ClientNotify",
                                        &process_client_notify));

  // SUBSCRIBER METHODS
  LocalClientMethodMap.insert(make_pair("PropogateSubscribe",
                                        &process_propogate_subscribe));
  LocalClientMethodMap.insert(make_pair("PropogateGrantUser",
                                        &process_propogate_grant_user));
  LocalClientMethodMap.insert(make_pair("SubscriberGeoStateChange",
                                        &process_subscriber_geo_state_change));
  LocalClientMethodMap.insert(make_pair("AgentBackoff",
                                        &process_agent_backoff));

  LocalClientMethodMap.insert(make_pair("SubscriberDelta",
                                        &process_subscriber_delta));
  LocalClientMethodMap.insert(make_pair("SubscriberDentries",
                                        &process_subscriber_dentries));
  LocalClientMethodMap.insert(make_pair("SubscriberCommitDelta",
                                        &process_subscriber_commit_delta));
  LocalClientMethodMap.insert(make_pair("SubscriberMerge",
                                        &process_subscriber_merge));
  LocalClientMethodMap.insert(make_pair("AgentDeltaError",
                                        &process_agent_delta_error));
  LocalClientMethodMap.insert(make_pair("SubscriberReconnect",
                                        &process_subscriber_reconnect));

  // BROADCAST CALLS
  LocalClientMethodMap.insert(make_pair("InternalRunCacheReaper",
                                        &process_internal_run_cache_reaper));
  LocalClientMethodMap.insert(make_pair("InternalAgentSettings",
                                        &process_internal_agent_settings));
  LocalClientMethodMap.insert(make_pair("InternalRunToSyncKeysDaemon",
                                        &process_internal_to_sync_keys_daemon));
}

static void init_ack_method_map() {
  AckMethodMap.insert(make_pair("AgentOnline",
                                &process_ack_agent_online));
  AckMethodMap.insert(make_pair("AgentRecheck",
                                &process_ack_agent_recheck));
  AckMethodMap.insert(make_pair("AgentStationUser",
                                &process_ack_agent_station_user));
  AckMethodMap.insert(make_pair("AgentDestationUser",
                                &process_ack_agent_destation_user));
  AckMethodMap.insert(make_pair("AgentSubscribe",
                                &process_ack_agent_subscribe));
  AckMethodMap.insert(make_pair("AgentUnsubscribe",
                                &process_ack_agent_unsubscribe));
  AckMethodMap.insert(make_pair("AgentAuthenticate",
                                &process_ack_agent_authenticate));
  AckMethodMap.insert(make_pair("AgentGetUserChannelPermissions",
                                &process_ack_agent_get_user_chan_perms));
  AckMethodMap.insert(make_pair("AgentHasSubscribePermissions",
                                &process_ack_agent_has_subscribe_perms));
  AckMethodMap.insert(make_pair("AdminAddUser",
                                &process_ack_admin_add_user));
  AckMethodMap.insert(make_pair("AdminGrantUser",
                                &process_ack_admin_grant_user));
  AckMethodMap.insert(make_pair("AdminRemoveUser",
                                &process_ack_admin_remove_user));

  AckMethodMap.insert(make_pair("AgentCache",
                                &process_ack_agent_cache));
  AckMethodMap.insert(make_pair("AgentEvict",
                                &process_ack_agent_evict));
  AckMethodMap.insert(make_pair("AgentLocalEvict",
                                &process_ack_agent_local_evict));
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INTERNAL METHOD HANDLERS --------------------------------------------------

static jv handle_local_client_method(lua_State *L, jv *jreq) {
  LT("handle_local_client_method");
  string id = (*jreq)["id"].asString();
  if (FixLogActive) {
    LD("CLIENT: METHOD: " << ZS_Errors["FixLogRunning"]);
    return zh_process_error(id, -32007, ZS_Errors["FixLogRunning"]);
  } else {
    string method = (*jreq)["method"].asString();
    LocalClientProcessFunctionMap::iterator it = 
                                             LocalClientMethodMap.find(method);
    if (it == LocalClientMethodMap.end()) {
      LE("LOCAL-CLIENT: METHOD: " << method << " NOT DEFINED");
      return zh_process_error(id, -32017, ZS_Errors["MethodNotDefined"]);
    }
    jv *params = &((*jreq)["params"]);
    string                     fname  = it->first;
    LocalClientProcessFunction cpfunc = it->second;
    ztrace_start(fname.c_str());
    jv response = cpfunc(L, id, params);
    ztrace_finish(fname.c_str());
    return response;
  }
}

static int client_respond(lua_State *L, jv *response) {
  LT("client_respond");
  if (*response == JNULL) {
    return lua_hook_client_respond(L, NULL);
  } else {
    string r = zh_convert_jv_to_string(response);
    return lua_hook_client_respond(L, r.c_str());
  }
}

int engine_local_client_method(lua_State *L, const char *jtext) {
  LT("engine_local_client_method");
  GL      = L; // SET C++ GLOBAL-LUA-STATE
  jv jreq = zh_parse_json_text(jtext, false);
  if (jreq == JNULL) {
    LE("ERROR: LUA_CLIENT PARSE ERROR");
    return 0;
  } else {
    jv response = handle_local_client_method(L, &jreq);
    return client_respond(L, &response);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE ACK METHOD ---------------------------------------------------------

static sqlres do_handle_ack_method(lua_State *L, string method,
                                   jv *jreq, jv *jres) {
  jv *error  = &((*jres)["error"]);
  jv *result = &((*jres)["result"]);
  LD("ACK: METHOD: " << method << " RESULT: "  << *result <<
     " ERROR: " << *error);
  if (zjd(error)) RETURN_EMPTY_SQLRES
  else {
    AckProcessFunctionMap::iterator it = AckMethodMap.find(method);
    if (it == AckMethodMap.end()) {
      LD("INBOUND-ACK: METHOD: " << method << " NOT DEFINED");
      RETURN_EMPTY_SQLRES
    } else {
      string             fname  = it->first;
      AckProcessFunction ppfunc = it->second;
      ztrace_start(fname.c_str());
      sqlres sr = ppfunc(L, jreq, result);
      ztrace_finish(fname.c_str());
      return sr;
    }
  }
}

static sqlres handle_ack_method(lua_State *L, string method,
                                jv *jreq, jv *jres) {
  if (!FixLogActive) {
    return do_handle_ack_method(L, method, jreq, jres);
  } else {
    LD("handle_ack_method: " << ZS_Errors["FixLogRunning"]);
    RETURN_SQL_RESULT_ERROR(ZS_Errors["FixLogRunning"]);
  }
}

int engine_handle_central_response(lua_State  *L, const char *qtext,
                                   const char *rtext) {
  LT("engine_handle_central_response");
  GL      = L; // SET C++ GLOBAL-LUA-STATE
  jv jreq = zh_parse_json_text(qtext, false);
  jv jres = zh_parse_json_text(rtext, false);
  if (jreq == JNULL || jres == JNULL) {
    LE("ERROR: LUA_CLIENT PARSE ERROR");
    return 0;
  } else {
    string method = jreq["method"].asString();
    (void)handle_ack_method(L, method, &jreq, &jres);
    return 0;
  }
}

int engine_handle_unexpected_subscriber_delta(lua_State    *L,
                                              const char   *kqk,
                                              const char   *sectok,
                                              unsigned int  auuid) {
  GL = L; // SET C++ GLOBAL-LUA-STATE
  LT("engine_handle_unexpected_subscriber_delta: (UNEXSD)");
  (void)zoor_handle_unexpected_subscriber_delta(kqk, sectok, auuid);
  return 0;
}

int engine_handle_generic_callback(lua_State *L, const char *cbname) {
  GL = L; // SET C++ GLOBAL-LUA-STATE
  LT("engine_handle_generic_callback: CB: " << cbname);
  (void)zisl_handle_generic_callback(cbname);
  return 0;
}

int engine_broken_central_connection(lua_State *L) {
  GL = L; // SET C++ GLOBAL-LUA-STATE
  LT("engine_broken_central_connection");
  (void)zisl_broken_central_connection();
  (void)zisl_reconnect_to_central();
  return 0;
}

int engine_get_agent_online_request_body(lua_State *L, unsigned int b) {
  jv     prequest = zaio_build_agent_online_request(b);
  string r        = zh_convert_jv_to_string(&prequest);
  return lua_hook_client_respond(L, r.c_str());
}

int engine_broadcast_workers_settings(lua_State *L) {
  GL = L; // SET C++ GLOBAL-LUA-STATE
  LT("engine_broadcast_workers_settings");
  if (!zisl_do_broadcast_workers_settings()) {
    LE("ERROR: broadcast_workers_settings");
  }
  return 0;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOCAL APPLY DELTA ---------------------------------------------------------

int engine_local_apply_dentry(lua_State  *L,      const char *kqk,
                              const char *socrdt, const char *sdentry) {
  LT("engine_local_apply_dentry");
  GL              = L; // SET C++ GLOBAL-LUA-STATE
  jv     response = zdelt_local_apply_dentry(kqk, socrdt, sdentry);
  string r        = zh_convert_jv_to_string(&response);
  return lua_hook_client_respond(L, r.c_str());
}

int engine_local_apply_oplog(lua_State  *L,        const char *kqk,
                             const char *scrdt,    const char *sopcontents,
                             const char *username, const char *password) {
  LT("engine_local_apply_oplog");
  GL              = L; // SET C++ GLOBAL-LUA-STATE
  jv     response = zdelt_local_apply_oplog(kqk, scrdt, sopcontents,
                                            username, password);
  string r        = zh_convert_jv_to_string(&response);
  return lua_hook_client_respond(L, r.c_str());
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZATION ------------------------------------------------------------

static bool init_isolation() {
  sqlres sr  = fetch_isolation();
  if (sr.err.size()) return false;
  else               return true;
}

static int fetch_uuids() {
  sqlres sr = startup_fetch_datacenter_uuid();
  if (sr.err.size()) {
    LE("startup_fetch_datacenter_uuid: " << sr.err);
    return -1;
  }
  sr = startup_fetch_agent_uuid();
  if (sr.err.size()) {
    LE("startup_fetch_agent_uuid: " << sr.err);
    return -1;
  }
  return 0;
}

static int initialize_worker() { LT("initialize_worker");
  if (!init_isolation()) {
    LE("DATABASE UNREACHABLE -> EXIT");
    return -1;
  }
  return fetch_uuids();
}

static int start_in_fixlog_mode() { LT("start_in_fixlog_mode");
  fixlog_activate();
  return 0;
}

static int init_engine() { LT("init_engine"); // POSSIBLY START IN FIXLOG MODE
  sqlres sr        = fetch_all_fixlog();
  bool   do_fixlog = zjd(&sr.jres);
  return do_fixlog ? start_in_fixlog_mode() : 0;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MAIN STARTUP FUNCTIONS ----------------------------------------------------

int engine_notify_dead_worker(unsigned int wid) {
  sqlres sr     = remove_worker(wid);
  if (sr.err.size()) return 0;
  sr            = increment_agent_num_workers(-1);
  if (sr.err.size()) return 0;
  UInt64 nwrkrs = sr.ures;
  LD("engine_notify_dead_worker: #W: " << nwrkrs);
#ifdef USING_MEMORY_LMDB_PLUGIN
  // NOTE: remove_all_agent_created() means RESET ALL KQK-DATA
  LD("engine_notify_dead_worker->remove_all_agent_created");
  remove_all_agent_created();
#endif
  return 0;
}

int engine_reset_worker_partition(unsigned int pid, unsigned int wpartition) {
  LD("engine_reset_worker_partition: P: " << pid <<
     " WPARTITION: " << wpartition);
  sqlres sr = persist_worker_partition(pid, wpartition);
  if (sr.err.size()) return 0;
  return 0;
}

int engine_run_primary_daemon_heartbeat(lua_State *L) {
  GL = L; // SET C++ GLOBAL-LUA-STATE
  if (DebugDaemons) LT("engine_run_primary_daemon_heartbeat");
  zhb_do_heartbeat();
  return 0;
}

int engine_run_primary_daemon_gc_purge(lua_State *L) {
  if (zh_AgentDisableAgentGCPurgeDaemon) return 0;
  GL = L; // SET C++ GLOBAL-LUA-STATE
  if (DebugDaemons) LT("engine_run_primary_daemon_gc_purge");
  do_gcv_summary_reaper();
  return 0;
}

int engine_run_primary_daemon_dirty_deltas(lua_State *L) {
  if (zh_AgentDisableAgentDirtyDeltasDaemon) return 0;
  GL = L; // SET C++ GLOBAL-LUA-STATE
  if (DebugDaemons) LT("engine_run_primary_daemon_dirty_deltas");
  zadaem_attempt_agent_dirty_deltas_reaper();
  return 0;
}

int engine_run_primary_daemon_to_sync_keys(lua_State *L) {
  if (zh_AgentDisableAgentToSync) return 0;
  GL = L; // SET C++ GLOBAL-LUA-STATE
  if (DebugDaemons) LT("engine_run_primary_daemon_to_sync_keys");
  zadaem_attempt_do_to_sync_keys_daemon();
  return 0;
}

int engine_initialize_primary(const char *pfile) {
  LT("engine_initialize_primary");
  AmPrimaryProcess = true;
  PidFile          = pfile;
  sqlres sr        = persist_worker(MyWorkerUUID, MyWorkerPort, true);
  if (sr.err.size())     return -1;
  if (!write_pid_file()) return -1;
  return 0;
}

int engine_initialize_worker(lua_State    *L,      const char   *dcname,
                             unsigned int  wid,    unsigned int  wport,
                             unsigned int  wpartition,
                             unsigned int  cmb,    unsigned int  dmb,
                             const char   *cbhost,
                             unsigned int  cbport, const char   *cbkey,
                             unsigned int  isp,    const char   *pfile,
                             const char   *lcfile, const char   *ldbd) {
  pid_t pid            = getpid();
  AmPrimaryProcess     = isp ? true : false;
  EnginedInited        = true;
  LD("engine_initialize_worker DC: " << dcname << " WID: " << wid <<
     " WPARTITION: " << wpartition << " CMB: " << cmb << " DMB: " << dmb <<
     " CBHOST: " << cbhost << " CBPORT: " << cbport << " ISP: " << isp <<
     " PIDFILE: " << pfile << " LCFILE: " << lcfile << " LDBD: " << ldbd);
  ConfigDataCenterName = dcname;
  MyDataCenterUUID     = dcname;
  MyWorkerUUID         = wid;
  MyWorkerPort         = wport;
  CacheMaxBytes        = cmb;
  DeltasMaxBytes       = dmb;
  AgentCallbackKey     = cbkey;
  PidFile              = pfile;
  if (!init_engine_settings(L, NULL, lcfile, ldbd)) return -1;
  add_signal_handlers();
  sqlres sr            = persist_worker(wid, wport, false);
  if (sr.err.size())     return -1;
  sr                   = increment_agent_num_workers(1);
  if (sr.err.size())     return -1;
  UInt64 nwrkrs        = sr.ures;
  LD("engine_initialize_worker: #W: " << nwrkrs);
  sr                   = persist_worker_partition(pid, wpartition);
  if (sr.err.size())     return -1;
  sr                   = persist_callback_server(cbhost, cbport);
  if (sr.err.size())     return -1;
  sr                   = fetch_device_key();
  if (sr.err.size())     return -1;
  DeviceKey            = sr.sres;
  LD("init_agent_data: DEVICE-KEY: " << DeviceKey);
  if (!write_pid_file()) return -1;
  if (initialize_worker() == -1) return -1;
  init_engine();
  return 0;
}

int engine_initialize_master(lua_State *L, const char *ldbd) {
  LD("engine_initialize_master: LDBD: " << ldbd);
  if (!storage_init_primary(L, NULL, ldbd)) return -1;
  zhb_init_heartbeat(true); // PERSIST
  return 0;
}

void engine_initialize_logging(const char *lcfile) {
  LD("engine_initialize_logging: LCF: " << lcfile);
  init_logger(lcfile);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ENGINE UTILITIES ----------------------------------------------------------

int engine_set_agent_uuid(unsigned int uuid) {
  LT("engine_set_agent_uuid: U: " << uuid);
  MyUUID = uuid;
  persist_agent_uuid();
  return 0;
}

int engine_get_agent_uuid() {
  LD("engine_get_agent_uuid: MyUUID: " << MyUUID);
  return MyUUID;
}

int engine_convert_crdt_element_to_json(lua_State *L, const char *ctxt) {
  LT("engine_convert_crdt_element_to_json");
  string s(ctxt);
  jv     jcval = zh_parse_json_text(s, false);
  jv     jjson = zconv_crdt_element_to_json(&jcval, false);
  string sj    = zh_convert_jv_to_string(&jjson);
  lua_pushstring(L, sj.c_str());
  return 1;
}


