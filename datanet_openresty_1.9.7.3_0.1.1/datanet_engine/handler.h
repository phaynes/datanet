
#ifndef __DATANET_HANDLER__H
#define __DATANET_HANDLER__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

// INTERNAL CALLS
jv process_internal_agent_settings(lua_State *L, string id, jv *params);

jv create_agent_settings_json_rpc_body(jv *skss, jv *rkss,
                                       bool isao, bool issub);
bool broadcast_workers_settings(jv *skss, jv *rkss, bool isao, bool issub);


// CLIENT CALLS
jv process_client_fetch(lua_State *L, string id, jv *params);
jv process_client_find(lua_State *L, string id, jv *params);

jv process_client_isolation(lua_State *L, string id, jv *params);

jv process_client_get_stationed_users    (lua_State *L, string id, jv *params);
jv process_client_get_agent_subscriptions(lua_State *L, string id, jv *params);
jv process_client_get_user_subscriptions (lua_State *L, string id, jv *params);


// FROM CENTRAL: ACKs & PROPOGATEs
jv process_subscriber_reconnect(lua_State *L, string id, jv *params);

jv process_subscriber_geo_state_change(lua_State *L, string id, jv *params);

jv process_propogate_grant_user(lua_State *L, string id, jv *result);

sqlres process_ack_agent_station_user(lua_State *L, jv *jreq, jv *result);
sqlres process_ack_agent_destation_user(lua_State *L, jv *jreq, jv *result);

jv process_propogate_subscribe(lua_State *L, string id, jv *result);
sqlres process_ack_agent_subscribe(lua_State *L, jv *jreq, jv *result);
sqlres process_ack_agent_unsubscribe(lua_State *L, jv *jreq, jv *result);

sqlres process_ack_admin_add_user(lua_State *L, jv *jreq, jv *result);
sqlres process_ack_admin_grant_user(lua_State *L, jv *jreq, jv *result);
sqlres process_ack_admin_remove_user(lua_State *L, jv *jreq, jv *result);

#endif
