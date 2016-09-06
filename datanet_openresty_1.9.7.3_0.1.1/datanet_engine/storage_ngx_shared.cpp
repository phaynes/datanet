#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>
#include <queue>
#include <mutex>

extern "C" {
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

#include "storage.h"

using namespace std;

//TODO extern UInt64 DeltasMaxBytes; // FROM storage.cpp

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

extern bool StorageInitialized;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

lua_State *L = NULL;

mutex      LuaMutex;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG SETTINGS ------------------------------------------------------------

bool DebugDataCalls = false;

#define LDATA(text) if (DebugDataCalls) LOG(DEBUG) << text << endl;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STARTUP -------------------------------------------------------------------

bool storage_cleanup() { // NO-OP
  lua_close(L);
  return true;
}

bool storage_init_nonprimary(void *p, const char *dfile, const char *ldbd) {
  storage_populate_table_primary_key_map();
  L = (lua_State *)p;
  LDATA("storage_init_nonprimary: L: " << L);
  return true;
}

bool storage_init_primary(void *p, const char *dfile, const char *ldbd) {
  storage_populate_table_primary_key_map();
  L            = (lua_State *)p;
  LDATA("storage_init_primary: L: " << L);
  string tname = "WORKERS";
  plugin_delete_table(tname);
  tname        = "LOCKS";
  plugin_delete_table(tname);
  return true;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SHARED MEMORY I/O ---------------------------------------------------------

static bool __shm_set_key(string &tname, string &pks, jv &jval) {
  LDATA("shm_set_key: T: " << tname << " PK: " << pks);
  string sval = zh_convert_jv_to_string(&jval);
  lua_getglobal (L, "SHM");
  lua_getfield  (L, -1, "SetKey");
  lua_pushstring(L, tname.c_str());
  lua_pushstring(L, pks.c_str());
  lua_pushstring(L, sval.c_str());
  if (lua_pcall(L, 3, 1, 0) != 0) {
    return zh_handle_lua_error(L);
  }
  lua_Integer n = lua_tointeger(L, -1);
  if (n != 1) return false;
  else        return true;
}

static jv __shm_get_key(string &tname, string &pks) {
  LDATA("shm_get_key: T: " << tname << " PK: " << pks);
  lua_getglobal (L, "SHM");
  lua_getfield  (L, -1, "GetKey");
  lua_pushstring(L, tname.c_str());
  lua_pushstring(L, pks.c_str());
  if (lua_pcall(L, 2, 1, 0) != 0) {
    zh_handle_lua_error(L);
    return JNULL;
  }
  const char *s = lua_tostring(L, -1);
  if (!s) return JNULL;
  else {
    string sval(s);
    return zh_parse_json_text(sval, true);
  }
}

static bool __shm_add_key(string &tname, string &pks) {
  LDATA("shm_add_key: T: " << tname << " PK: " << pks);
  lua_getglobal (L, "SHM");
  lua_getfield  (L, -1, "AddKey");
  lua_pushstring(L, tname.c_str());
  lua_pushstring(L, pks.c_str());
  if (lua_pcall(L, 2, 1, 0) != 0) {
    return zh_handle_lua_error(L);
  }
  int ret = lua_tonumber(L, -1);
  return ret ? true : false;
}

static bool __shm_delete_key(string &tname, string &pks) {
  LDATA("shm_delete_key: T: " << tname << " PK: " << pks);
  lua_getglobal (L, "SHM");
  lua_getfield  (L, -1, "DeleteKey");
  lua_pushstring(L, tname.c_str());
  lua_pushstring(L, pks.c_str());
  if (lua_pcall(L, 2, 1, 0) != 0) {
    return zh_handle_lua_error(L);
  }
  lua_Integer n = lua_tointeger(L, -1);
  if (n != 1) return false;
  else        return true;
}

static bool __shm_delete_table(string &tname) {
  LDATA("shm_delete_table: T: " << tname);
  lua_getglobal (L, "SHM");
  lua_getfield  (L, -1, "DeleteTable");
  lua_pushstring(L, tname.c_str());
  if (lua_pcall(L, 1, 1, 0) != 0) {
    return zh_handle_lua_error(L);
  }
  lua_Integer n = lua_tointeger(L, -1);
  if (n != 1) return false;
  else        return true;
}

static jv __shm_get_table(string &tname) {
  LDATA("shm_get_table T: " << tname);
  lua_getglobal (L, "SHM");
  lua_getfield  (L, -1, "GetTableSemiSerialized");
  lua_pushstring(L, tname.c_str());
  if (lua_pcall(L, 1, 1, 0) != 0) {
    zh_handle_lua_error(L);
    return JNULL;
  }
  if (!lua_istable(L, -1)) {
    return JNULL;
  } else {
    jv jval;
    lua_pushnil(L);
    while(lua_next(L, -2) != 0) {
      if (!lua_isstring(L, -1)) return JNULL;
      const char *k    = lua_tostring(L, -2);
      const char *sval = lua_tostring(L, -1);
      LDATA("convert_lua_shmtable_to_json: K: " << k << " SVAL: " << sval);
      jval[k]          = zh_parse_json_text(sval, true);
      lua_pop(L, 1);
    }
    LDATA("END: convert_lua_shmtable_to_json" << jval);
    return jval;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOCK LUA WRAPPERS ---------------------------------------------------------

static void LockLua() {
  LuaMutex.lock();
}

static void UnlockLua() {
  LuaMutex.unlock();
}

bool plugin_set_key(string &tname, string &pks, jv &jval) {
  if (!StorageInitialized) return false;
  LockLua();                                // LOCK -------------------------->
  bool b = __shm_set_key(tname, pks, jval); //                                |
  UnlockLua();                              // UNLOCK <-----------------------+
  return b;
}

jv plugin_get_key_field(string &tname, string &pks, string &fname) {
  if (!StorageInitialized) return JNULL;
  jv jempty; // NOTE EMPTY
  jv jval = plugin_get_key(tname, pks);
  if (zjn(&jval))                     return jempty;
  if (!zh_jv_is_member(&jval, fname)) return jempty;
  return jval;
}

jv plugin_get_key(string &tname, string &pks) {
  if (!StorageInitialized) return JNULL;
  LockLua();                                // LOCK -------------------------->
  jv jval = __shm_get_key(tname, pks);      //                                |
  UnlockLua();                              // UNLOCK <-----------------------+
  return jval;
}

bool plugin_add_key(string &tname, string &pks) {
  if (!StorageInitialized) return false;
  LockLua();                                // LOCK -------------------------->
  bool b = __shm_add_key(tname, pks);       //                                |
  UnlockLua();                              // UNLOCK <-----------------------+
  return b;
}

bool plugin_delete_key(string &tname, string &pks) {
  if (!StorageInitialized) return false;
  LockLua();                                // LOCK -------------------------->
  bool b = __shm_delete_key(tname, pks);    //                                |
  UnlockLua();                              // UNLOCK <-----------------------+
  return b;
}

bool plugin_delete_table(string &tname) {
  if (!StorageInitialized) return false;
  LockLua();                                // LOCK -------------------------->
  bool b = __shm_delete_table(tname);       //                                |
  UnlockLua();                              // UNLOCK <-----------------------+
  return b;
}

jv plugin_get_table(string &tname) {
  if (!StorageInitialized) return JNULL;
  LockLua();                                // LOCK -------------------------->
  jv jval = __shm_get_table(tname);         //                                |
  UnlockLua();                              // UNLOCK <-----------------------+
  return jval;
}


