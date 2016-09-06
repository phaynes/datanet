
#ifndef __DATANET_LOGIC_LMDB_STORAGE__H
#define __DATANET_LOGIC_LMDB_STORAGE__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

bool lmdb_storage_cleanup();
bool lmdb_storage_init_nonprimary(void *p, const char *dfile, const char *ldbd);
bool lmdb_storage_init_primary(void *p, const char *dfile, const char *ldbd);

bool   lmdb_plugin_set_key(string &tname, string &pks, jv &jval);
jv     lmdb_plugin_get_key_field(string &tname, string &pks, string &fname);
jv     lmdb_plugin_get_key(string &tname, string &pks);
bool   lmdb_plugin_add_key(string &tname, string &pks);
bool   lmdb_plugin_delete_key(string &tname, string &pks);
bool   lmdb_plugin_delete_table(string &tname);
jv     lmdb_plugin_get_table(string &tname);

#endif
