
#ifndef __DATANET_DT__H
#define __DATANET_DT__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

bool cmp_crdt_ordered_list(jv *ace, jv *bce);

void zdt_run_data_type_functions(jv *jcrdtd, jv *jdmeta, jv merge, jv *dts);

#endif
