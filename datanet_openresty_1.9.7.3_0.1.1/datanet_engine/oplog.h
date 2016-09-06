
#ifndef __DATANET_OPLOG__H
#define __DATANET_OPLOG__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

jv zoplog_create_op_path_entry(jv *jkey, jv *jval);
jv zoplog_create_delta_entry(jv *jop_path, jv *jval);
jv zoplog_create_delta(jv *jcrdt, jv *joplog);

#endif
