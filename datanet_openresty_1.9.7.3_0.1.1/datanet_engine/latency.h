
#ifndef __DATANET_LATENCY__H
#define __DATANET_LATENCY__H

#include <string>

#include "helper.h"

using namespace std;

sqlres zlat_add_to_subscriber_latencies(string kqk, jv *jdentry);

jv process_client_get_latencies(lua_State *L, string id, jv *params);

#endif
