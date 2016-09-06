
#ifndef __DATANET_GC__H
#define __DATANET_GC__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

sqlres zgc_remove_gcv_summary(string &kqk, UInt64 gcv);
sqlres zgc_remove_gcv_summaries(string &kqk);

sqlres zgc_add_reorder_to_gc_summaries(string &kqk, jv *reorder);
sqlres zgc_add_ooogcw_to_gc_summary(jv *pc, UInt64 dgcv,
                                    jv *rometa, jv *reorder);

sqlres zgc_save_gcv_summaries(string &kqk, jv *jgcsumms);
jv     zgc_union_gcv_summaries(jv *jagcsumms);
sqlres zgc_get_gcv_summary_range_check(string &kqk, UInt64 bgcv, UInt64 egcv);
sqlres zgc_get_unioned_gcv_summary(string &kqk, UInt64 dgcv, UInt64 cgcv);

UInt64 zgc_get_min_gcv(jv *jgcsumms);

sqlres zgc_finish_agent_gc_wait(jv *pc, UInt64 bgcv, UInt64 egcv);
sqlres zgc_cancel_agent_gc_wait(string &kqk);

string zgc_get_array_id(jv *jcval);
string zgc_get_tombstone_id(jv *jcval);

bool   zgc_do_garbage_collection(jv *pc, UInt64 gcv, jv *jtsumm, jv *jlhnr);
sqlres zgc_garbage_collect(jv *pc, jv *jdentry);

void   zgc_agent_post_merge_update_lhn(jv *aentry, jv *clm);

sqlres zgc_reorder(string &kqk, jv *jcrdt, jv *jdentry);

sqlres zgc_get_gcversion(string &kqk);

#endif
