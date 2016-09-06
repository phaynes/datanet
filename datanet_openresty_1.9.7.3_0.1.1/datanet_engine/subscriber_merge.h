
#ifndef __DATANET_SUBSCRIBER_MERGE__H
#define __DATANET_SUBSCRIBER_MERGE__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

sqlres zsm_set_key_in_sync(string &kqk);

sqlres retry_subscriber_merge(jv *pc);
sqlres do_process_subscriber_merge(jv *jks, jv *jxcrdt, jv *jcavrsns, 
                                   jv *jgcsumms, jv *jether,
                                   bool remove, bool is_cache);
jv process_subscriber_merge(lua_State *L, string id, jv *params);

#endif
