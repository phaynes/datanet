
#ifndef __DATANET_ACTIVESYNC__H
#define __DATANET_ACTIVESYNC__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

sqlres retry_subscriber_commit_delta(jv *pc);
sqlres zas_do_commit_delta(jv *jks, jv *jauthor);
sqlres zas_post_apply_delta_conditional_commit(jv *jks, jv *jdentry);
sqlres zas_check_flow_subscriber_commit_delta(jv *jks, jv *jauthor);
jv     process_subscriber_commit_delta(lua_State *L, string id, jv *params);

sqlres zas_set_agent_sync_key(string &kqk, string &sectok);
sqlres zas_set_agent_sync_key_signal_agent_to_sync_keys(jv *jks);

sqlres zas_sync_missing_keys(jv *skss, bool issub);
sqlres zas_agent_sync_channel_kss(jv *pkss, bool issub);

#endif
