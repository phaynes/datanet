
#ifndef __DATANET_LUA_HOOKS__H
#define __DATANET_LUA_HOOKS__H

extern "C" {
  #include "lua.h"
}

#include "helper.h"
#include "storage.h"

using namespace std;

int lua_hook_client_respond(lua_State *L, const char *r);

extern "C" {
  int luaopen_datanet_sqlite_engine(lua_State *L);
  int luaopen_datanet_nginx_engine(lua_State *L);
}

#endif
