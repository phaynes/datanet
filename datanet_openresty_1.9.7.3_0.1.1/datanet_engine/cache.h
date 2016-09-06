
#ifndef __DATANET_CACHE__H
#define __DATANET_CACHE__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

sqlres zcache_agent_store_cache_key_metadata(string &kqk, bool watch,
                                             jv *jauth);

jv process_client_cache(lua_State *L, string id, jv *params);
sqlres process_ack_agent_cache(lua_State *L, jv *jreq, jv *result);

sqlres process_ack_agent_local_evict(lua_State *L, jv *jreq, jv *result);
sqlres process_ack_agent_evict(lua_State *L, jv *jreq, jv *result);
sqlres zcache_internal_evict(jv *jks, bool send_central, bool nm);

#endif
