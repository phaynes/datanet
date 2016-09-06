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
#include "deprecated_engine.h"
#include "lua_engine_hooks.h"
#include "engine_network.h"

using namespace std;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNS -------------------------------------------------------------------

extern Int64  MyUUID;;          // FROM engine.cpp
extern Int64  MyWorkerPort;     // FROM engine.cpp


extern UInt64 CacheMaxBytes;    // FROM cache.cpp

extern bool FixLogActive;       // FROM fixlog.cpp

extern pthread_t PrimaryTid;    // FROM engine_network.cpp
extern pthread_t WorkerTid;     // FROM engine_network.cpp
extern pthread_t ReconnectTid;  // FROM engine_network.cpp

extern jv     zh_nobody_auth;   // FROM helper.cpp
extern string DeviceKey;        // FROM helper.cpp

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEPRECATED ----------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------

int engine_start_worker(unsigned int iport) { LT("engine_start_worker");
  return network_worker_initialize(iport, MyWorkerPort);
}

int engine_start_primary_network(const char   *chost, unsigned int cport,
                                 unsigned int iport) {
  LT("engine_start_primary_network");
  network_primary_initialize(chost, cport, iport, MyWorkerPort);
  return 0;
}

typedef jv (*ServerProcessFunction)(string id, jv *params);
typedef map<string, ServerProcessFunction> ServerProcessFunctionMap;
ServerProcessFunctionMap PrimaryMethodMap;

ServerProcessFunctionMap ServerMethodMap;

static void handle_server_method(jv *jreq) {
  if (FixLogActive) {
    LD("handle_server_method: " << ZS_Errors["FixLogRunning"]);
  } else {
    string  id     = (*jreq)["id"].asString();
    string  method = (*jreq)["method"].asString();
    jv     *params = &((*jreq)["params"]);
    LD("SERVER-ID: " << id << " METHOD: " << method << " PARAMS: " << *params);
    jv response;
    ServerProcessFunctionMap::iterator it = ServerMethodMap.find(method);
    if (it == ServerMethodMap.end()) {
      LD("INBOUND-SERVER: METHOD: " << method << " NOT DEFINED");
    } else {
      ServerProcessFunction spfunc = it->second;
      response = spfunc(id, params);
    }
  }
}

void engine_process_server_method(jv *jreq) {
  LT("engine_process_server_method");
  handle_server_method(jreq);
}

map<string, int>      RemoteMethodCallIDtoFDMap;

static void initialize_erequest(erequest *ereq) {
  ereq->request  = JNULL;
  ereq->response = JNULL;
}

#if 0
void engine_process_primary_ack(jv *jreq) {
  LT("engine_process_primary_ack");
  string    id       = (*jreq)["id"].asString();
  erequest  ereq     = EngineRequestMap[id];
  jv       *prequest = &ereq.request;
  (*jreq)["method"]  = (*prequest)["method"];
  erequest  pereq    = handle_ack_method(jreq);
  EngineRequestMap.erase(id); // NOTE: comes after handle_ack_method()
}

void engine_process_central_ack(jv *jreq) {
  LT("engine_process_central_ack");
  string    id       = (*jreq)["id"].asString();
  erequest  ereq     = EngineRequestMap[id];
  jv       *prequest = &ereq.request;
  (*jreq)["method"]  = (*prequest)["method"];
  erequest  pereq    = handle_ack_method(jreq);
  EngineRequestMap.erase(id); // NOTE: comes after handle_ack_method()
  map<string, int>::iterator it = RemoteMethodCallIDtoFDMap.find(id);
  if (it != RemoteMethodCallIDtoFDMap.end()) {
    int    fd = it->second;
    LD("RemoteMethodCallIDtoFDMap: GOT: ID: " << id << " FD: " << fd);
    string r  = zh_convert_jv_to_string(&pereq.response);
    network_respond_remote_call(fd, r.c_str(), r.size());
  }
}
#else
void engine_process_primary_ack(jv *jreq) {
}
void engine_process_central_ack(jv *jreq) {
}
#endif

static void handle_primary_method(jv *jreq) {
  if (FixLogActive) {
    LD("handle_primary_method: " << ZS_Errors["FixLogRunning"]);
  } else {
    string  id     = (*jreq)["id"].asString();
    string  method = (*jreq)["method"].asString();
    jv     *params = &((*jreq)["params"]);
    LD("PRIMARY-ID: " << id << " METHOD: " << method << " PARAMS: " << *params);
    jv response;
    ServerProcessFunctionMap::iterator it = PrimaryMethodMap.find(method);
    if (it == PrimaryMethodMap.end()) {
      LD("INBOUND-PRIMARY: METHOD: " << method << " NOT DEFINED");
    } else {
      ServerProcessFunction spfunc = it->second;
      response = spfunc(id, params);
    }
  }
}

void engine_process_primary_method(jv *jreq) {
  handle_primary_method(jreq);
}

map<string, erequest> EngineRequestMap;

void engine_send_to_primary_worker(jv &irequest) {
  LD("engine_send_to_primary_worker");
  sqlres sr  = fetch_primary_worker_uuid();
  if (sr.err.size()) return;
  Int64  wid = (Int64)sr.ures;
  string r   = zh_convert_jv_to_string(&irequest);
  network_send_internal(wid, r);
}

void engine_send_to_key_worker(string &kqk, jv &irequest) {
  LD("engine_send_to_key_worker: K: " << kqk);
  Int64  wid = engine_get_worker_uuid_for_operation(kqk);
  string r   = zh_convert_jv_to_string(&irequest);
  network_send_internal(wid, r);
}

void engine_send_primary_to_proxy(jv *prequest, jv *addtl) {
  string r = zh_convert_jv_to_string(prequest);
  erequest ereq;
  initialize_erequest(&ereq);
  ereq.request = *prequest;
  if (addtl) ereq.addtl = *addtl;
  string pid   = (*prequest)["id"].asString();
  EngineRequestMap.insert(pair<string, erequest>(pid, ereq));
  LD("engine_send_primary_to_proxy: EngineRequestMap: INSERT: pid: " << pid);
  network_send_primary(r.c_str(), r.size());
}

void engine_send_central_need_merge(string &kqk, string &sectok, UInt64 gcv) {
  LD("NeedMerge: K: " << kqk << " SEC: " << sectok);
  jv prequest = zh_create_json_rpc_body("AgentNeedMerge", &zh_nobody_auth);
  prequest["params"]["data"]["ks"]         = zh_create_ks(kqk, sectok.c_str());
  prequest["params"]["data"]["gc_version"] = gcv;
  engine_send_primary_to_proxy(&prequest, NULL);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WORKER BROADCAST SETTINGS -------------------------------------------------

int engine_send_worker_settings(int wfd) {
  if (MyUUID == -1) return -1; // NO INFO TO SEND
  jv     skss     = JOBJECT;
  jv     rkss     = JARRAY;
  bool   isao     = false;
  bool   issub    = false;
  jv     prequest = create_agent_settings_json_rpc_body(&skss, &rkss,
                                                        isao, issub);
  string r        = zh_convert_jv_to_string(&prequest);
  return network_respond_remote_call(wfd, r.c_str(), r.size());
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RECONNECT THREAD CALLS ----------------------------------------------------

int engine_send_binary_fd(int pfd, jv *prequest) {
  string r = zh_convert_jv_to_string(prequest);
  erequest ereq;
  initialize_erequest(&ereq);
  ereq.request = *prequest;
  string pid   = (*prequest)["id"].asString();
  EngineRequestMap.insert(pair<string, erequest>(pid, ereq));
  LD("engine_send_binary_fd: EngineRequestMap: INSERT: pid: " << pid);
  return network_send_binary_fd(pfd, r.c_str(), r.size());
}

int engine_send_wakeup_worker(int wfd) {
  LT("engine_send_wakeup_worker");
  jv prequest = zh_create_json_rpc_body("WakeupWorker", &zh_nobody_auth);
  return engine_send_binary_fd(wfd, &prequest);
}

int engine_handle_reconnect_to_central_event(int pfd) {
  LT("engine_handle_reconnect_to_central_event: pfd: " << pfd);
  if (false) { // USED TO BE if(AgentBackoffActive) {
    LD("engine_handle_reconnect_to_central_event: AgentBackoffActive -> NO-OP");
    return 0;
  } else {
    jv prequest = zaio_build_agent_online_request(true);
    if (prequest == JNULL) return -1;
    return engine_send_binary_fd(pfd, &prequest);
  }
}

// NOTE CALLED FROM RECONNECT-THREAD
bool engine_handle_broken_central_connection_event() {
  LT("engine_handle_broken_central_connection_event");
  return zisl_broken_central_connection();
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PROCESS REMOTE METHOD CALL ------------------------------------------------

typedef jv (*RemoteClientProcessFunction)(string id, jv *params);
typedef map<string, RemoteClientProcessFunction> RemoteClientProcessFunctionMap;
RemoteClientProcessFunctionMap RemoteClientMethodMap;

static jv handle_remote_client_method(jv *jreq) {
  string id = (*jreq)["id"].asString();
  if (FixLogActive) {
    LD("CLIENT: METHOD: " << ZS_Errors["FixLogRunning"]);
    return zh_process_error(id, -32007, ZS_Errors["FixLogRunning"]);
  } else {
    string method = (*jreq)["method"].asString();
    RemoteClientProcessFunctionMap::iterator it = 
                                            RemoteClientMethodMap.find(method);
    if (it == RemoteClientMethodMap.end()) {
      LE("REMOTE: CLIENT: METHOD: " << method <<" NOT DEFINED");
      return zh_process_error(id, -32017, ZS_Errors["MethodNotDefined"]);
    }
    jv *params = &((*jreq)["params"]);
    LD("REMOTE-CLIENT: ID: " << id << " METHOD: " << method <<
       " PARAMS: " << *params);
    RemoteClientProcessFunction cpfunc = it->second;
    return cpfunc(id, params);
  }
}

void engine_process_remote_method_call(int fd, string &pdata) {
  LT("engine_process_remote_method_call");
  jv jreq = zh_parse_json_text(pdata.c_str(), false);
  if (jreq == JNULL) {
    LE("ERROR: engine_process_remote_method_call: PARSE: " << pdata);
  } else {
    jv response = handle_remote_client_method(&jreq);
    if (zh_jv_is_member(&response, "method")) { // REQUEST-> WAIT ON CENTRAL
      string id = response["id"].asString();
      LD("INSERT RemoteMethodCallIDtoFDMap: ID: " << id << " FD: " << fd);
      RemoteMethodCallIDtoFDMap.insert(make_pair(id, fd));
    } else {
      string id = response["id"].asString();
      string r  = zh_convert_jv_to_string(&response);
      network_respond_remote_call(fd, r.c_str(), r.size());
    }
  }
}


