
#ifndef __DATANET_NOTIFY__H
#define __DATANET_NOTIFY__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

void znotify_notify_on_data_change(jv *pc, bool full_doc, bool selfie);

void znotify_debug_notify(string &kqk, jv *debug);

jv process_client_notify(lua_State *L, string id, jv *params);

#endif
