
#ifndef __DATANET_SUBSCRIBER_DELTA__H
#define __DATANET_SUBSCRIBER_DELTA__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

sqlres zsd_fetch_subscriber_delta(string &kqk, jv *jauthor);

sqlres zsd_get_agent_sync_status(string &kqk, jv *md);
sqlres zsd_fetch_local_key_metadata(string &kqk, jv *md);

sqlres zsd_set_subscriber_agent_version(string &kqk, UInt64 auuid, 
                                        string &avrsn);
sqlres zsd_conditional_persist_subscriber_delta(string &kqk, jv *jdentry);

sqlres zsd_create_sd_pc(jv *jks, jv *jdentry, jv *md);

sqlres retry_subscriber_delta(jv *pc);

void zsd_handle_unexpected_ooo_delta(jv *jks, jv *einfo);

sqlres zsd_internal_handle_subscriber_delta(jv *jks, jv *jdentry, jv *md);

jv process_subscriber_delta   (lua_State *L, string id, jv *params);
jv process_subscriber_dentries(lua_State *L, string id, jv *params);

#endif
