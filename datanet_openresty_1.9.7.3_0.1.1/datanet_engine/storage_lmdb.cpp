#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>

extern "C" {
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

extern "C" {
  #include "lmdb.h"
}

#include "json/json.h"
#include "easylogging++.h"

#include "logic_storage_lmdb.h"
#include "helper.h"
#include "storage.h"

using namespace std;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

lua_State *L = NULL;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG SETTINGS ------------------------------------------------------------

#define DISABLE_LT
#ifdef DISABLE_LT
  #undef LT
  #define LT(text) /* text */
#endif

#define DISABLE_LDATA
#ifdef DISABLE_LDATA
  #undef LDATA
  #define LDATA(text) /* text */
#else
  #define LDATA(text) LD(text)
#endif


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LMDB DEFINE HELPERS -------------------------------------------------------

#define L_CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
                            "%s:%d: %s: %s\n", __FILE__, __LINE__, \
                            msg, mdb_strerror(rc)), abort()))

#define L_E(expr) L_CHECK((rc = (expr)) == MDB_SUCCESS, #expr)


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STARTUP -------------------------------------------------------------------

bool storage_cleanup() {
  return lmdb_storage_cleanup();
}

bool storage_init_nonprimary(void *p, const char *dfile, const char *ldbd) {
  return lmdb_storage_init_nonprimary(p, dfile, ldbd);
}

bool storage_init_primary(void *p, const char *dfile, const char *ldbd) {
  return lmdb_storage_init_primary(p, dfile, ldbd);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LMDB I/O ------------------------------------------------------------------

bool plugin_set_key(string &tname, string &pks, jv &jval) {
  return lmdb_plugin_set_key(tname, pks, jval);
}

jv plugin_get_key_field(string &tname, string &pks, string &fname) {
  return lmdb_plugin_get_key_field(tname, pks, fname);
}

jv plugin_get_key(string &tname, string &pks) {
  return lmdb_plugin_get_key(tname, pks);
}

bool plugin_add_key(string &tname, string &pks) {
  return lmdb_plugin_add_key(tname, pks);
}

bool plugin_delete_key(string &tname, string &pks) {
  return lmdb_plugin_delete_key(tname, pks);
}

bool plugin_delete_table(string &tname) {
  return lmdb_plugin_delete_table(tname);
}

jv plugin_get_table(string &tname) {
  return lmdb_plugin_get_table(tname);
}

