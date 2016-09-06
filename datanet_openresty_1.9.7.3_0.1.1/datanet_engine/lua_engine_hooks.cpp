#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>

extern "C" {
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 

  #include "lua.h"
  #include "lualib.h"
  #include "lauxlib.h"
}

#include "easylogging++.h"

#include "engine_network.h"
#include "lua_engine_hooks.h"
#include "engine.h"
#include "helper.h"
#include "storage.h"

using namespace std;

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
// HELPERS -------------------------------------------------------------------

static int push_lua_number(lua_State *L, int i) {
  if (i == -1) return 0;
  else {
    lua_pushnumber(L, i);
    return 1;
  }
}

static int push_lua_string(lua_State *L, const char *r) {
  if (!r) return 0;
  else {
    LT("push_lua_string: " << r);
    lua_pushstring(L, r);
    return 1;
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ENGINE TO LUA HOOKS ---------------------------------------------------------

int lua_hook_client_respond(lua_State *L, const char *r) {
  return push_lua_string(L, r);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LUA TO ENGINE HOOKS ---------------------------------------------------------

static int do_lua_log(lua_State *L, const char *text, unsigned int level) {
  if        (level == TRACE) {
    if (LogLevel == TRACE) {
      LT("(LUA): " << text);
    }
  } else if (level == NOTICE) {
    if (LogLevel != ERROR) {
      LD("(LUA): " << text);
    }
  } else {         /* ERROR */
      LE("(LUA): " << text);
  }
  return 0;
}

static int do_set_log_level(lua_State *L, const char *level) {
  string lvl(level);
  zh_to_upper(lvl);
  if      (!lvl.compare("TRACE"))  LogLevel = TRACE;
  else if (!lvl.compare("NOTICE")) LogLevel = NOTICE;
  else if (!lvl.compare("ERROR"))  LogLevel = ERROR;
  else {
    string err = "ERROR: SUPPORTED LOG-LEVELS: [TRACE, NOTICE, ERROR]";
    return push_lua_string(L, err.c_str());
  }
  LE("LOG LEVEL SET TO " << lvl);
  return 0;
}

static int do_lua_initialize_logging(lua_State *L, const char *lcfile) {
  engine_initialize_logging(lcfile);
  return 0;
}

static int do_lua_local_client_method(lua_State *L, const char *text) {
  return engine_local_client_method(L, text);
}

static int do_lua_handle_central_response(lua_State *L, const char *qtext,
                                          const char *rtext) {
  return engine_handle_central_response(L, qtext, rtext);
}

static int do_lua_handle_unexpected_subscriber_delta(lua_State    *L,
                                                     const char   *kqk,
                                                     const char   *sectok,
                                                     unsigned int  auuid) {
  return engine_handle_unexpected_subscriber_delta(L, kqk, sectok, auuid);
}

static int do_lua_handle_generic_callback(lua_State *L, const char *cbname) {
  return engine_handle_generic_callback(L, cbname);
}

static int do_lua_broken_central_connection(lua_State *L) {
  return engine_broken_central_connection(L);
}

static int do_lua_get_agent_online_request_body(lua_State *L, unsigned int b) {
  return engine_get_agent_online_request_body(L, b);
}

static int do_lua_broadcast_workers_settings(lua_State *L) {
  return engine_broadcast_workers_settings(L);
}

static int do_lua_local_apply_dentry(lua_State  *L, const char *kqk,
                                    const char *socrdt, const char *sdentry) {
  return engine_local_apply_dentry(L, kqk, socrdt, sdentry);
}

static int do_lua_local_apply_oplog(lua_State  *L,
                                    const char *kqk,
                                    const char *scrdt,
                                    const char *sopcontents,
                                    const char *username,
                                    const char *password) {
  return engine_local_apply_oplog(L, kqk, scrdt, sopcontents,
                                  username, password);
}

static int do_lua_notify_dead_worker(lua_State *L, unsigned int wid) {
  return engine_notify_dead_worker(wid);
}

static int do_lua_reset_worker_partition(lua_State *L, unsigned int pid,
                                         unsigned int wpartition) {
  return engine_reset_worker_partition(pid, wpartition);
}

static int do_lua_run_primary_daemon_to_sync_keys(lua_State *L) {
  return engine_run_primary_daemon_to_sync_keys(L);
}

static int do_lua_run_primary_daemon_dirty_deltas(lua_State *L) {
  return engine_run_primary_daemon_dirty_deltas(L);
}

static int do_lua_run_primary_daemon_gc_purge(lua_State *L) {
  return engine_run_primary_daemon_gc_purge(L);
}

static int do_lua_run_primary_daemon_heartbeat(lua_State *L) {
  return engine_run_primary_daemon_heartbeat(L);
}

static int do_lua_initialize_primary(lua_State *L, const char *pfile) {
  int ret = engine_initialize_primary(pfile);
  return push_lua_number(L, ret);
}

static int do_lua_initialize_worker(lua_State    *L,      const char   *dcname,
                                    unsigned int  wid,    unsigned int  wport,
                                    unsigned int  wpartition,
                                    unsigned int  cmb,    unsigned int dmb,
                                    const char   *cbhost,
                                    unsigned int  cbport, const char   *cbkey,
                                    unsigned int  isp,    const char   *pfile,
                                    const char   *lcfile, const char   *ldbd) {
  int ret = engine_initialize_worker(L, dcname, wid, wport, wpartition,
                                     cmb, dmb, cbhost, cbport, cbkey, isp,
                                     pfile, lcfile, ldbd);
  return push_lua_number(L, ret);
}

static int do_lua_convert_crdt_element_to_json(lua_State *L, const char *ctxt) {
  return engine_convert_crdt_element_to_json(L, ctxt);
}

static int do_lua_initialize_master(lua_State *L, const char *ldbd) {
  int ret = engine_initialize_master(L, ldbd);
  return push_lua_number(L, ret);
}

static int do_lua_set_agent_uuid(unsigned int uuid) {
  return engine_set_agent_uuid(uuid);
}

static int do_lua_get_agent_uuid(lua_State *L) {
  int uuid = engine_get_agent_uuid();
  return push_lua_number(L, uuid);
}

extern "C" {
  static int lua_log(lua_State *L) {
    const char   *text  = lua_tostring(L, 1);
    unsigned int  level;
    if (lua_gettop(L) == 2) level = (unsigned int)lua_tonumber(L, 2);
    else                    level = 0;
    return do_lua_log(L, text, level);
  }

  static int lua_set_log_level(lua_State *L) {
    const char *level = lua_tostring(L, -1);
    return do_set_log_level(L, level);
  }

  static int lua_initialize_logging(lua_State *L) {
    const char *lcfile = lua_tostring(L, 1);
    return do_lua_initialize_logging(L, lcfile);
  }

  static int lua_local_client_method(lua_State *L) {
    const char *text = lua_tostring(L, -1);
    return do_lua_local_client_method(L, text);
  }

  static int lua_handle_central_response(lua_State *L) {
    const char *qtext  = lua_tostring(L, 1);
    const char *rtext  = lua_tostring(L, 2);
    return do_lua_handle_central_response(L, qtext, rtext);
  }

  static int lua_handle_unexpected_subscriber_delta(lua_State *L) {
    const char   *kqk    = lua_tostring(L, 1);
    const char   *sectok = lua_tostring(L, 2);
    unsigned int  auuid  = (unsigned int)lua_tonumber(L, 3);
    return do_lua_handle_unexpected_subscriber_delta(L, kqk, sectok, auuid);
  }

  static int lua_handle_generic_callback(lua_State *L) {
    const char  *cbname = lua_tostring(L, 1);
    return do_lua_handle_generic_callback(L, cbname);
  }

  static int lua_broken_central_connection(lua_State *L) {
    return do_lua_broken_central_connection(L);
  }

  static int lua_get_agent_online_request_body(lua_State *L) {
    unsigned int b = (unsigned int)lua_tonumber(L, 1);
    return do_lua_get_agent_online_request_body(L, b);
  }

  static int lua_broadcast_workers_settings(lua_State *L) {
    return do_lua_broadcast_workers_settings(L);
  }

  static int lua_local_apply_dentry(lua_State *L) {
    const char *kqk     = lua_tostring(L, 1);
    const char *socrdt  = lua_tostring(L, 2);
    const char *sdentry = lua_tostring(L, 3);
    return do_lua_local_apply_dentry(L, kqk, socrdt, sdentry);
  }

  static int lua_local_apply_oplog(lua_State *L) {
    const char *kqk         = lua_tostring(L, 1);
    const char *scrdt       = lua_tostring(L, 2);
    const char *sopcontents = lua_tostring(L, 3);
    const char *username    = lua_tostring(L, 4);
    const char *password    = lua_tostring(L, 5);
    return do_lua_local_apply_oplog(L, kqk, scrdt, sopcontents,
                                    username, password);
  }

  static int lua_notify_dead_worker(lua_State *L) {
    unsigned int wid = (unsigned int)lua_tonumber(L, 1);
    do_lua_notify_dead_worker(L, wid);
    return 0;
  }

  static int lua_reset_worker_partition(lua_State *L) {
    unsigned int pid        = (unsigned int)lua_tonumber(L, 1);
    unsigned int wpartition = (unsigned int)lua_tonumber(L, 2);
    do_lua_reset_worker_partition(L, pid, wpartition);
    return 0;
  }

  static int lua_run_primary_daemon_to_sync_keys(lua_State *L) {
    return do_lua_run_primary_daemon_to_sync_keys(L);
  }

  static int lua_run_primary_daemon_dirty_deltas(lua_State *L) {
    return do_lua_run_primary_daemon_dirty_deltas(L);
  }

  static int lua_run_primary_daemon_gc_purge(lua_State *L) {
    return do_lua_run_primary_daemon_gc_purge(L);
  }

  static int lua_run_primary_daemon_heartbeat(lua_State *L) {
    return do_lua_run_primary_daemon_heartbeat(L);
  }

  static int lua_initialize_primary(lua_State *L) {
    const char *pfile = lua_tostring(L, 1);
    return do_lua_initialize_primary(L, pfile);
  }

  static int lua_initialize_worker(lua_State *L) {
    const char   *dcname     =               lua_tostring(L, 1);
    unsigned int  wid        = (unsigned int)lua_tonumber(L, 2);
    unsigned int  wport      = (unsigned int)lua_tonumber(L, 3);
    unsigned int  wpartition = (unsigned int)lua_tonumber(L, 4);
    unsigned int  cmb        = (unsigned int)lua_tonumber(L, 5);
    unsigned int  dmb        = (unsigned int)lua_tonumber(L, 6);
    const char   *cbhost     =               lua_tostring(L, 7);
    unsigned int  cbport     = (unsigned int)lua_tonumber(L, 8);
    const char   *cbkey      =               lua_tostring(L, 9);
    unsigned int  isp        = (unsigned int)lua_tonumber(L, 10);
    const char   *pfile      =               lua_tostring(L, 11);
    const char   *lcfile     =               lua_tostring(L, 12);
    const char   *ldbd       =               lua_tostring(L, 13);
    return do_lua_initialize_worker(L, dcname, wid, wport, wpartition,
                                    cmb, dmb, cbhost, cbport, cbkey, isp,
                                    pfile, lcfile, ldbd);
  }

  static int lua_convert_crdt_element_to_json(lua_State *L) {
    const char   *ctxt = lua_tostring(L, 1);
    return do_lua_convert_crdt_element_to_json(L, ctxt);
  }

  static int lua_initialize_master(lua_State *L) {
    const char *ldbd = lua_tostring(L, 1);
    return do_lua_initialize_master(L, ldbd);
  }

  static int lua_set_agent_uuid(lua_State *L) {
    unsigned int uuid = (unsigned int)lua_tonumber(L, 1);
    do_lua_set_agent_uuid(uuid);
    return 0;
  }

  static int lua_get_agent_uuid(lua_State *L) {
    return do_lua_get_agent_uuid(L);
  }

  static int luaopen_datanet_engine(lua_State *L) {
    lua_register(L, "c_initialize_master",       lua_initialize_master);

    lua_register(L, "c_initialize_primary",      lua_initialize_primary);
    lua_register(L, "c_initialize_worker",       lua_initialize_worker);

    lua_register(L, "c_set_agent_uuid",          lua_set_agent_uuid);
    lua_register(L, "c_get_agent_uuid",          lua_get_agent_uuid);

    lua_register(L, "c_convert_crdt_element_to_json",
                                        lua_convert_crdt_element_to_json);

    lua_register(L, "c_run_primary_daemon_to_sync_keys",
                                        lua_run_primary_daemon_to_sync_keys);
    lua_register(L, "c_run_primary_daemon_dirty_deltas",
                                        lua_run_primary_daemon_dirty_deltas);
    lua_register(L, "c_run_primary_daemon_gc_purge",
                                        lua_run_primary_daemon_gc_purge);
    lua_register(L, "c_run_primary_daemon_heartbeat",
                                        lua_run_primary_daemon_heartbeat);

    lua_register(L, "c_notify_dead_worker",      lua_notify_dead_worker);
    lua_register(L, "c_reset_worker_partition",  lua_reset_worker_partition);

    lua_register(L, "c_local_client_method",     lua_local_client_method);
    lua_register(L, "c_handle_central_response", lua_handle_central_response);

    lua_register(L, "c_engine_handle_unexpected_subscriber_delta",
                                        lua_handle_unexpected_subscriber_delta);
    lua_register(L, "c_engine_handle_generic_callback",
                                        lua_handle_generic_callback);

    lua_register(L, "c_broken_central_connection",
                                        lua_broken_central_connection);
    lua_register(L, "c_get_agent_online_request_body",
                                        lua_get_agent_online_request_body);
    lua_register(L, "c_broadcast_workers_settings",
                                        lua_broadcast_workers_settings);

    lua_register(L, "c_local_apply_dentry",      lua_local_apply_dentry);
    lua_register(L, "c_local_apply_oplog",       lua_local_apply_oplog);

    lua_register(L, "c_initialize_logging",      lua_initialize_logging);
    lua_register(L, "c_set_log_level",           lua_set_log_level);
    lua_register(L, "c_log",                     lua_log);

    return 0;
  }

  int luaopen_datanet_sqlite_engine(lua_State *L) {      // SAME-ENGINE
    return luaopen_datanet_engine(L);
  }

  int luaopen_datanet_ngx_shared_engine(lua_State *L) {  // SAME-ENGINE
    return luaopen_datanet_engine(L);
  }

  int luaopen_datanet_lmdb_engine(lua_State *L) {        // SAME-ENGINE
    return luaopen_datanet_engine(L);
  }

  int luaopen_datanet_memory_lmdb_engine(lua_State *L) { // SAME-ENGINE
    return luaopen_datanet_engine(L);
  }
}

