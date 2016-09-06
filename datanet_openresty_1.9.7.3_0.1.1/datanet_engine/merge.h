
#ifndef __DATANET_MERGE__H
#define __DATANET_MERGE__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

void zmerge_check_apply_reorder_element(jv *cval, jv *reo);

void zmerge_do_array_merge(jv *carr);

jv   *zmerge_find_nested_element(jv *clm, jv *op_path);
jv   *zmerge_find_nested_element_base(jv *clm, jv *op_path);
uint  zmerge_get_last_key_array_adding(jv *clm, jv *val, jv *op_path);

void zmerge_deep_debug_merge_crdt(jv *jmeta, jv *jcrdtdv, bool is_reo);
void zmerge_debug_crdt(jv *jcrdt);

jv zmerge_apply_deltas(jv *jocrdt, jv *jdelta, bool is_reo);
bool zmerge_compare_zdoc_authors(jv *m1, jv *m2);

sqlres zmerge_forward_crdt_gc_version(jv *pc, jv *jgcsumms);

jv zmerge_rewind_crdt_gc_version(string &kqk, jv *jcrdt, jv *jgcsumms);

jv zmerge_create_virgin_json_from_oplog(jv *jks, jv *jrchans, jv *joplog);

sqlres zmerge_freshen_agent_delta(string &kqk, jv *jdentry,
                                  jv *jocrdt, UInt64 gcv);


jv zmerge_freshen_stale_agent_delta(jv *jdentry, jv *jxcrdt, UInt64 ngcv);

jv zmerge_add_reorder_to_reference(jv *dentry, jv *toreo_dentry);

#endif
