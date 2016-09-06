
#ifndef __DATANET_DELTAS__H
#define __DATANET_DELTAS__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

void zdelt_on_data_change(jv *pc, bool full_doc, bool selfie);

sqlres zdelt_get_key_subscriber_versions(string &kqk);
sqlres zdelt_get_agent_delta_dependencies(string &kqk);

void zdelt_cleanup_agent_delta_before_send(jv *jdentry);

sqlres rollback_client_commit(jv *pc);

void zdelt_delete_meta_timings(jv *jdentry);

 // NOTE: Used by ZHB
jv internal_process_client_store(string &id, jv *params, bool internal);
jv process_client_store (lua_State *L, string id, jv *params);
 // NOTE: Used by ZHB
jv internal_process_client_commit(string &id, jv *params, bool internal);
jv process_client_commit(lua_State *L, string id, jv *params);
jv process_client_remove(lua_State *L, string id, jv *params);
jv process_client_expire(lua_State *L, string id, jv *params);

jv process_internal_commit(lua_State *L, string id, jv *params);
jv zdelt_local_apply_dentry(const char *skqk, const char *socrdt,
                            const char *sdentry);
jv zdelt_local_apply_oplog(const char *skqk, const char *scrdt,
                           const char *sopcontents,
                           const char *username, const char *password);

#endif
