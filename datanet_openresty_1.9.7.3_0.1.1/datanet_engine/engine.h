
#ifndef __DATANET_ENGINE__H
#define __DATANET_ENGINE__H

#include <string>

extern "C" {
  #include "lua.h"
}

#include "json/json.h"

#include "easylogging++.h"

#include "helper.h"

using namespace std;

jv process_error(string id, int code, string message);

// Called from lua_engine_hooks.cpp
int engine_initialize_master(lua_State *L, const char *ldbd);
int engine_initialize_primary(const char *pfile);
int engine_initialize_worker(lua_State    *L,      const char   *dcname,
                             unsigned int  wid,    unsigned int  wport,
                             unsigned int  wpartition,
                             unsigned int  cmb,    unsigned int  dmb,
                             const char   *cbhost,
                             unsigned int  cbport, const char   *cbkey,
                             unsigned int  isp,    const char   *pfile,
                             const char   *lcfile, const char   *ldbd);

int engine_set_agent_uuid(unsigned int uuid);
int engine_get_agent_uuid();

int engine_convert_crdt_element_to_json(lua_State *L, const char *ctxt);

int engine_run_primary_daemon_heartbeat(lua_State *L);
int engine_run_primary_daemon_gc_purge(lua_State *L);
int engine_run_primary_daemon_dirty_deltas(lua_State *L);
int engine_run_primary_daemon_to_sync_keys(lua_State *L);

int engine_notify_dead_worker(unsigned int wid);
int engine_reset_worker_partition(unsigned int pid, unsigned int wpartition);

int engine_local_client_method(lua_State *L, const char *jtext);
int engine_handle_central_response(lua_State *L, const char *qtext,
                                   const char *rtext);

int engine_handle_unexpected_subscriber_delta(lua_State    *L, 
                                              const char   *kqk, 
                                              const char   *sectok,
                                              unsigned int  auuid);
int engine_handle_generic_callback(lua_State *L, const char *cbname);

int engine_broken_central_connection(lua_State *L);
int engine_get_agent_online_request_body(lua_State *L, unsigned int b);
int engine_broadcast_workers_settings(lua_State *L);

int engine_local_apply_dentry(lua_State  *L,      const char *kqk,
                              const char *socrdt, const char *sdentry);
int engine_local_apply_oplog(lua_State  *L,        const char *kqk,
                             const char *scrdt,    const char *sopcontents,
                             const char *username, const char *password);


void engine_initialize_logging(const char *lcfile);

#endif
