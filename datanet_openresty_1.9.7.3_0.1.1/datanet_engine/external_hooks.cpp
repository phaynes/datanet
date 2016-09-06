#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>
#include <algorithm>

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

#include "helper.h"
#include "storage.h"

using namespace std;

extern lua_State *GL;

extern jv     CentralMaster;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNAL LUA HOOKS --------------------------------------------------------

bool zexh_send_central_agent_online(UInt64 to, bool b) {
  string conname = zh_get_connection_name();
  lua_getglobal  (GL, "CppHook");
  lua_getfield   (GL, -1, "CentralRedirect");
  string mhost = CentralMaster[conname]["server"]["hostname"].asString();
  lua_pushstring(GL, mhost.c_str());
  UInt64 mport = CentralMaster[conname]["server"]["port"].asUInt64();
  lua_pushnumber (GL, mport);
  lua_pushboolean(GL, b);
  lua_pushnumber (GL, to);
  if (lua_pcall(GL, 4, 1, 0) != 0) {
    return zh_handle_lua_error(GL);
  }
  lua_Integer n = lua_tointeger(GL, -1);
  if (n != 1) return false;
  else        return true;
}

bool zexh_send_url_https_request(string &url, string &body) {
  lua_getglobal (GL, "CppHook");
  lua_getfield  (GL, -1, "SendURLHttpsRequest");
  lua_pushstring(GL, url.c_str());
  lua_pushstring(GL, body.c_str());
  if (lua_pcall(GL, 2, 1, 0) != 0) {
    return zh_handle_lua_error(GL);
  }
  lua_Integer n = lua_tointeger(GL, -1);
  if (n != 1) return false;
  else        return true;
}

bool zexh_send_central_https_request(UInt64 to, jv &prequest,
                                     bool is_dentries) {
  string r = zh_convert_jv_to_string(&prequest);
  lua_getglobal  (GL, "CppHook");
  lua_getfield   (GL, -1, "SendCentralHttpsRequest");
  lua_pushnumber (GL, to);
  lua_pushstring (GL, r.c_str());
  lua_pushboolean(GL, is_dentries);
  if (lua_pcall(GL, 3, 1, 0) != 0) {
    return zh_handle_lua_error(GL);
  }
  lua_Integer n = lua_tointeger(GL, -1);
  if (n != 1) return false;
  else        return true;
}

bool zexh_broadcast_internal_request(jv &prequest, pid_t xpid) {
  string r = zh_convert_jv_to_string(&prequest);
  lua_getglobal (GL, "CppHook");
  lua_getfield  (GL, -1, "InternalBroadcast");
  lua_pushstring(GL, r.c_str());
  lua_pushnumber(GL, xpid);
  if (lua_pcall(GL, 2, 1, 0) != 0) {
    return zh_handle_lua_error(GL);
  }
  lua_Integer n = lua_tointeger(GL, -1);
  if (n != 1) return false;
  else        return true;
}

bool zexh_broadcast_delta_to_worker_caches(jv &prequest, string &kqk,
                                           pid_t xpid) {
  string r = zh_convert_jv_to_string(&prequest);
  lua_getglobal (GL, "CppHook");
  lua_getfield  (GL, -1, "InternalDeltaBroadcast");
  lua_pushstring(GL, r.c_str());
  lua_pushstring(GL, kqk.c_str());
  lua_pushnumber(GL, xpid);
  if (lua_pcall(GL, 3, 1, 0) != 0) {
    return zh_handle_lua_error(GL);
  }
  lua_Integer n = lua_tointeger(GL, -1);
  if (n != 1) return false;
  else        return true;
}

//TODO AUUID not needed
bool zexh_set_timer_unexpected_subscriber_delta(string &kqk,  string &sectok,
                                                UInt64 auuid, UInt64 to) {
  lua_getglobal (GL, "CppHook");
  lua_getfield  (GL, -1, "SetUnexpectedSDTimer");
  lua_pushstring(GL, kqk.c_str());
  lua_pushstring(GL, sectok.c_str());
  lua_pushnumber(GL, auuid);
  lua_pushnumber(GL, to);
  if (lua_pcall(GL, 4, 1, 0) != 0) {
    return zh_handle_lua_error(GL);
  }
  lua_Integer n = lua_tointeger(GL, -1);
  if (n != 1) return false;
  else        return true;
}

bool zexh_set_timer_callback(const char *cbname, UInt64 to) {
  LD("zexh_set_timer_callback: CB: " << cbname);
  lua_getglobal (GL, "CppHook");
  lua_getfield  (GL, -1, "SetTimerCallback");
  lua_pushstring(GL, cbname);
  lua_pushnumber(GL, to);
  if (lua_pcall(GL, 2, 1, 0) != 0) {
    return zh_handle_lua_error(GL);
  }
  lua_Integer n = lua_tointeger(GL, -1);
  if (n != 1) return false;
  else        return true;
}

static void repair_lua_crdt_element(jv *jcdata) {
  jv     *jcval = &((*jcdata)["V"]);
  string  ctype = (*jcdata)["T"].asString();
  if        (!ctype.compare("A")) {
    if (jcval->size() == 0 && jcval->isObject()) *jcval = JARRAY; // REPAIR
    for (uint i = 0; i < jcval->size(); i++) {
      jv *jval = &((*jcval)[i]);
      repair_lua_crdt_element(jval);
    }
  } else if (!ctype.compare("O")) {
    vector<string> mbrs = zh_jv_get_member_names(jcval);
    for (uint i = 0; i < mbrs.size(); i++) {
      jv *jval = &((*jcval)[mbrs[i]]);
      repair_lua_crdt_element(jval);
    }
  }
}

// LUA only has OBJECTs (i.e. tables) it has no ARRAYs
void zexh_repair_lua_crdt(jv *jcrdt) {
  if (zjn(jcrdt)) return;
  jv *jcrdtd = &((*jcrdt)["_data"]);
  repair_lua_crdt_element(jcrdtd);
}

void zexh_repair_lua_oplog(jv *joplog) {
  for (uint i = 0; i < joplog->size(); i++) {
    jv *jop = &((*joplog)[i]);
    if (zh_jv_is_member(jop, "sargs")) {
      string spath = (*jop)["path"].asString();
      string sargs = (*jop)["sargs"].asString();
      string sop = "{\"path\":\"" + spath + 
                   "\",\"name\":\"set\",\"args\":[" + sargs + "]}";
      *jop       = zh_parse_json_text(sop, false);
    }
  }
}

