
#ifndef __DATANET_AGENT_DAEMON__H
#define __DATANET_AGENT_DAEMON__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

void zadaem_reset_key_drain_map();

void zadaem_local_signal_to_sync_keys_daemon(); // KQK-WORKER
jv process_internal_to_sync_keys_daemon(lua_State *L, string id, jv *params);
bool zadaem_signal_to_sync_keys_daemon();       // ALL-WORKERS

sqlres zadaem_get_agent_key_drain(string &kqk, bool internal);

void zadaem_attempt_do_to_sync_keys_daemon();
void zadaem_attempt_agent_dirty_deltas_reaper();

#endif
