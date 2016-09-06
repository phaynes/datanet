
#ifndef __DATANET_DACK__H
#define __DATANET_DACK__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

sqlres zdack_set_key_out_of_sync(jv *jks);

sqlres zdack_do_flow_remove_agent_delta(jv *jks, jv *jauthor);

sqlres retry_remove_agent_delta(jv *pc);
jv process_agent_delta_error(lua_State *L, string id, jv *result);

sqlres zdack_get_agent_last_created();
sqlres zdack_set_agent_createds(jv *jcreateds);

#endif
