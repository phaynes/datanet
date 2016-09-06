
#ifndef __DATANET_CONV__H
#define __DATANET_CONV__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

jv zconv_add_OR_set_member(jv *jmember, string ctype, jv *jmeta, bool adding);
jv zconv_json_element_to_crdt(string vtype, jv *jval, jv *jdmeta, bool adding);

jv convert_json_element_to_crdt(jv *jval, jv *jdmeta, bool adding);
jv zconv_convert_json_object_to_crdt(jv *jjson, jv *jdmeta, bool adding);

string zconv_get_crdt_type_from_json_element(jv *jval);
jv zconv_convert_json_to_crdt_type_UInt64(UInt64 val);

jv zconv_crdt_element_to_json(jv *cval, bool debug);
jv zconv_convert_crdt_data_value(jv *jcrdtdv);

UInt64 zconv_calculate_meta_size(jv *jmeta);
void   zconv_calculate_crdt_member_size(jv *cval, UInt64 &size);
UInt64 zconv_calculate_crdt_bytes(jv *jcrdt);

#endif
