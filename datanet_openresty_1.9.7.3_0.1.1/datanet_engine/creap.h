
#ifndef __DATANET_CACHE_REAPER__H
#define __DATANET_CACHE_REAPER__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

jv     process_internal_run_cache_reaper(lua_State *L, string id, jv *params);

sqlres zcreap_set_lru_local_read(string &kqk);

UInt64 zcreap_get_ocrdt_num_bytes(jv *md);

sqlres zcreap_store_num_bytes(jv *pc, UInt64 o_nbytes, bool selfie);

#endif
