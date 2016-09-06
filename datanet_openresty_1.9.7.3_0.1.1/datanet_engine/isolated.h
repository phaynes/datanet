
#ifndef __DATANET_ISOLATED__H
#define __DATANET_ISOLATED__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

bool   zisl_do_broadcast_workers_settings();

sqlres process_ack_agent_recheck(lua_State *L, jv *jreq, jv *params);

sqlres zisl_set_agent_synced();

sqlres zisl_worker_sync_keys(jv *skss, jv *rkss);
sqlres process_ack_agent_online(lua_State *L, jv *jreq, jv *params);

bool   zisl_reconnect_to_central();
bool   zisl_broken_central_connection();

bool   zisl_handle_generic_callback(const char *cbname);
bool   zisl_set_geo_nodes(jv *jgn);
jv     process_client_get_cluster_info(lua_State *L, string id, jv *params);

jv     process_agent_backoff(lua_State *L, string id, jv *params);

sqlres zisl_get_agent_info();

sqlres zisl_set_agent_isolation(bool b);

#endif
