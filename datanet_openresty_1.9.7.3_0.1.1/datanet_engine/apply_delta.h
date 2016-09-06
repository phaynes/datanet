
#ifndef __DATANET_APPLY_DELTA__H
#define __DATANET_APPLY_DELTA__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

void   zad_assign_post_merge_deltas(jv *pc, jv *jmerge);

sqlres zad_store_crdt_and_gc_version(string &kqk, jv *jcrdt,
                                     UInt64 gcv, UInt64 xgcv);
sqlres zad_remove_crdt_store_gc_version(string &kqk, jv *jcrdt, UInt64 gcv,
                                        bool oexists);

sqlres zad_apply_agent_delta(jv *pc);
sqlres zad_apply_subscriber_delta(jv *pc);

#endif
