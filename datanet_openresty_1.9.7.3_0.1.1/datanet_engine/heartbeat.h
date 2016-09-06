
#ifndef __DATANET_HEARTBEAT__H
#define __DATANET_HEARTBEAT__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

void zhb_init_heartbeat(bool persist);
void zhb_do_heartbeat();

jv process_client_heartbeat(lua_State *L, string id, jv *params);

#endif
