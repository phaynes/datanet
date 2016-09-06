
#ifndef __DATANET_EXTERNAL_HOOKS__H
#define __DATANET_EXTERNAL_HOOKS__H

#include <string>
#include <vector>

extern "C" {
  #include "lua.h"
  #include "lualib.h"
  #include "lauxlib.h"
}

#include "helper.h"
#include "storage.h"

using namespace std;

bool zexh_send_central_agent_online(UInt64 to, bool b);
bool zexh_send_url_https_request(string &url, string &body);
bool zexh_send_central_https_request(UInt64 to, jv &prequest, bool is_dentries);

bool zexh_broadcast_internal_request(jv &prequest, pid_t xpid);
bool zexh_broadcast_delta_to_worker_caches(jv &prequest, string &kqk,
                                           pid_t xpid);

bool zexh_set_timer_unexpected_subscriber_delta(string &kqk,  string &sectok,
                                              UInt64 auuid, UInt64 to);
bool zexh_set_timer_callback(const char *cbname, UInt64 to);

void zexh_repair_lua_crdt(jv *jcrdt);
void zexh_repair_lua_oplog(jv *joplog);

#endif
