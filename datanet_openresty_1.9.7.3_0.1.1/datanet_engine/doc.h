
#ifndef __DATANET_DOC__H
#define __DATANET_DOC__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

vector<string> zdoc_parse_op_key(string path);

string zdoc_write_key_check(string path);

string zdoc_s_arg_check(bool iso, bool isa, string key, string vtype, 
                        bool adding);

string zdoc_insert_arg_pre_check(UInt64 index, char *endptr);

string zdoc_insert_post_arg_check(string ptype);

#endif
