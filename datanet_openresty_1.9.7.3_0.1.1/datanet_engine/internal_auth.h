
#ifndef __DATANET_INTERNAL_AUTH__H
#define __DATANET_INTERNAL_AUTH__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

jv process_internal_client_authorize(lua_State *L, string id, jv *params);

sqlres process_ack_agent_authenticate(lua_State *L, jv *jreq, jv *result);
sqlres process_ack_agent_get_user_chan_perms(lua_State *L, jv *jreq,
                                             jv *result);
sqlres process_ack_agent_has_subscribe_perms(lua_State *L, jv *jreq,
                                             jv *result);

#endif
