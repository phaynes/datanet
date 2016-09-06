
#ifndef __DATANET_OOO_REPLAY__H
#define __DATANET_OOO_REPLAY__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

sqlres zoor_set_gc_wait_incomplete(string &kqk);
sqlres zoor_end_gc_wait_incomplete(string &kqk);
sqlres zoor_agent_in_gc_wait(string &kqk);

sqlres zoor_analyze_key_gcv_needs_reorder_range(jv *jks, UInt64 gcv,
                                                jv *jdiffs);

sqlres zoor_do_remove_ooo_delta(string &kqk, jv *jauthor);
sqlres zoor_remove_subscriber_delta(string &kqk, jv *jauthor);

sqlres zoor_decrement_reorder_delta(jv *pc);
sqlres zoor_forward_crdt_gc_version_apply_reference_delta(jv *pc,
                                                          jv *rodentry);

sqlres zoor_apply_reorder_delta(jv *pc, bool is_sub);

sqlres zoor_handle_ooo_gcv_delta(jv *jks, jv *jdentry, jv *md, bool is_sub);
sqlres zoor_unset_ooo_gcv_delta(string &kqk, jv *jauthor);

sqlres zoor_handle_unexpected_subscriber_delta(const char *kqk,
                                               const char *sectok,
                                               UInt64      auuid);

sqlres zoor_persist_ooo_delta(jv *jks, jv *jdentry, jv *einfo);

sqlres zoor_dependencies_sort_authors(string &kqk, bool is_sub, jv *jauthors);

sqlres zoor_replay_subscriber_merge_deltas(jv *pc, bool is_sub);
sqlres zoor_prepare_local_deltas_post_subscriber_merge(jv *pc);

sqlres zoor_check_replay_ooo_deltas(jv *pc, bool do_check, bool is_sub);

#endif
