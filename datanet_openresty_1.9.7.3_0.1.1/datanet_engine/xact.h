
#ifndef __DATANET_XACT__H
#define __DATANET_XACT__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

UInt64 zxact_get_operation_count(jv *jccrdt, jv *joplog, bool remove,
                                 bool expire, jv *jdelta);

sqlres zxact_reserve_op_id_range(UInt64 cnt);

sqlres zxact_reserve_op_id_range(UInt64 cnt);

UInt64 zxact_update_crdt_meta_and_added(jv *jcrdt, UInt64 cnt, UInt64 dvrsn,
                                        UInt64 ts);

void zxact_finalize_delta_for_commit(jv *jcrdt, jv *jdelta,
                                     UInt64 dvrsn, UInt64 ts);

jv zxact_create_auto_dentry(string &kqk, jv *cmeta, UInt64 new_count);

#endif
