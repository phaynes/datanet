#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>
#include <queue>

extern "C" {
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

#include <sqlite3.h>

#include "json/json.h"

#include "easylogging++.h"

#include "shared.h"
#include "storage.h"

using namespace std;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNS -------------------------------------------------------------------

//TODO extern UInt64 DeltasMaxBytes; // FROM storage.cpp

extern bool StorageInitialized;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

//bool     DebugSQL = false;
bool DebugSQL = true;

sqlite3 *SQL_DB   = NULL;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STARTUP -------------------------------------------------------------------

static sqlres exec_sql(string qbuf);

bool storage_cleanup() {
  sqlite3_close(SQL_DB);
  return true;
}

bool storage_init_nonprimary(void *p, const char *dfile, const char *ldbd) {
  storage_populate_table_primary_key_map();
  pid_t     pid = getpid();
  pthread_t tid = pthread_self();
  LOG(DEBUG) << "storage_init_nonprimary: " << dfile << " PID: " << pid <<
                " TID: " << tid << " SQL_DB: " << &SQL_DB << endl;
  int rc = sqlite3_open(dfile, &SQL_DB);
  if (rc) {
    LOG(ERROR) << "Can't open database: " << sqlite3_errmsg(SQL_DB) << endl;
    sqlite3_close(SQL_DB);
    return false;
  }
  return true;
}

bool storage_init_primary(void *p, const char *dfile, const char *ldbd) {
  LOG(DEBUG) << "storage_init_primary" << endl;
  if (!storage_init_nonprimary(p, dfile, ldbd)) return false;
  string qstr = "DELETE FROM WORKERS;";
  exec_sql(qstr);
  qstr        = "DELETE FROM LOCKS;";
  exec_sql(qstr);
  return true;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATABASE I/O --------------------------------------------------------------

static sqlres exec_sql(string qbuf) {
  sqlres sr;
  char *qb  = (char *)qbuf.c_str();
  if (DebugSQL) LOG(DEBUG) << "DEBUG: EXEC_SQL: " << qb << endl;
  char *zem = 0;
  int   rc  = sqlite3_exec(SQL_DB, qb, NULL, 0, &zem);
  if (rc != SQLITE_OK) {
    sr.err =  "-ERROR: SQL: ";
    sr.err += zem;
    LOG(ERROR) << sr.err << endl;
    LOG(ERROR) << "SQL_QUERY: " << qbuf << endl;
    sqlite3_free(zem);
  }
  return sr;
}

static int UInt64_cb(void *vu, int argc, char **argv, char **cn) {
  UInt64 *u   = (UInt64 *)vu;
  char   *res = argv[0];
  if (res) *u = strtoul(argv[0], NULL, 10);
  else     *u = 0;
  return 0;
}

static int text_cb(void *vs, int argc, char **argv, char **cn) {
  string *s = (string *)vs;
  if (argv[0]) *s = argv[0];
  return 0;
}

static int json_array_cb(void *vjva, int argc, char **argv, char **cn) {
  jv *jva  = (jv *)vjva;
  jv  jarg = argv[0];
  zh_jv_append(jva, &jarg);
  return 0;
}

// NOTE: both JSON & STRINGs are valid column values
static int json_map_cb(void *vjm, int argc, char **argv, char **cn) {
  jv     *jmap = (jv *)vjm;
  string  key  = argv[0];
  string  val  = argv[1];
  jv      json = zh_parse_json_text(val, true);
  (*jmap)[key] = zjd(&json) ? json : val;
  return 0;
}

static sqlres fetch_UInt64(string qstr) {
  sqlres sr;
  if (DebugSQL) LOG(DEBUG) << "DEBUG: FETCH: " << qstr << endl;
  char *zem = 0;
  sr.ures   = UINT64_MAX;
  int   rc  = sqlite3_exec(SQL_DB, qstr.c_str(), UInt64_cb, &sr.ures, &zem);
  if (rc != SQLITE_OK) {
    LOG(ERROR) << "SQL error: " << zem << endl;
    sqlite3_free(zem);
  }
  return sr;
}

static sqlres fetch_text(string qstr) {
  sqlres sr;
  if (DebugSQL) LOG(DEBUG) << "DEBUG: FETCH: " << qstr << endl;
  char   *zem = 0;
  int     rc  = sqlite3_exec(SQL_DB, qstr.c_str(), text_cb, &sr.sres, &zem);
  if (rc != SQLITE_OK) {
    LOG(ERROR) << "SQL error: " << zem << endl;
    sqlite3_free(zem);
  }
  return sr;
}

static sqlres fetch_json_array(string qstr) {
  sqlres sr;
  sr.jres   = JARRAY;
  if (DebugSQL) LOG(DEBUG) << "DEBUG: FETCH: " << qstr << endl;
  char *zem = 0;
  int   rc  = sqlite3_exec(SQL_DB, qstr.c_str(),
                           json_array_cb, &sr.jres, &zem);
  if (rc != SQLITE_OK) {
    LOG(ERROR) << "SQL error: " << zem << endl;
    sqlite3_free(zem);
  }
  return sr;
}

static sqlres fetch_json_map(string qstr) {
  sqlres sr;
  if (DebugSQL) LOG(DEBUG) << "DEBUG: FETCH: " << qstr << endl;
  char *zem = 0;
  int   rc  = sqlite3_exec(SQL_DB, qstr.c_str(), json_map_cb, &sr.jres, &zem);
  if (rc != SQLITE_OK) {
    LOG(ERROR) << "SQL error: " << zem << endl;
    sqlite3_free(zem);
  }
  return sr;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ABSTRACT DATA CALL HELPERS ------------------------------------------------

static string create_comma_separated_list(vector<string> &fields) {
  string ret;
  bool   first = true;
  for (vector<string>::iterator it = fields.begin(); it != fields.end(); it++) {
    if (!first) ret += ", ";
    ret   += *it;
    first  = false;
  }
  return ret;
}

static string create_comma_separated_list(vector<StorageValue> &vals) {
  string ret;
  bool   first = true;
  for (vector<StorageValue>::iterator it = vals.begin();
       it != vals.end(); it++) {
    if (!first) ret += ", ";
    ret   += it->to_string();
    first  = false;
  }
  return ret;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ABSTRACT DATA CALLS -------------------------------------------------------

#define CHECK_STORAGE_INITIALIZATION {                           \
  if (!StorageInitialized) {                                     \
    RETURN_SQL_RESULT_ERROR(ZS_Errors["StorageNotInitialized"])  \
  }                                                              \
}

sqlres lock_field(string &tname, PrimaryKey &pk, StorageCondition &cond) {
  CHECK_STORAGE_INITIALIZATION
  sqlres sr;
  zh_fatal_error("LOGIC(SQLITE: lock_field: NOT IMPLEMENTED)");
  //TODO IMPLEMENT as sqlite transaction
  return sr;
}

sqlres unlock_field(string &tname, PrimaryKey &pk, StorageCondition &cond) {
  CHECK_STORAGE_INITIALIZATION
  sqlres sr;
  zh_fatal_error("LOGIC(SQLITE: unlock_field: NOT IMPLEMENTED)");
  //TODO IMPLEMENT as sqlite transaction
  return sr;
}

static sqlres __fetch_field(string &qstr, value_type vt) {
  if (DebugSQL) LOG(DEBUG) << "__fetch_field: qstr: " << qstr << endl;
  if      (vt == VT_UINT)   return fetch_UInt64    (qstr);
  else if (vt == VT_STRING) return fetch_text      (qstr);
  else if (vt == VT_ARRAY)  return fetch_json_array(qstr);
  zh_fatal_error("LOGIC(__fetch_field)");
  RETURN_EMPTY_SQLRES // compiler warning
}

static sqlres __fetch_fields(string &qstr, value_type vt) {
  if (DebugSQL) LOG(DEBUG) << "fetch_fields: qstr: " << qstr << endl;
  if      (vt == VT_JMAP)  return fetch_json_map  (qstr);
  else if (vt == VT_ARRAY) return fetch_json_array(qstr);
  zh_fatal_error("LOGIC(__fetch_fields)");
  RETURN_EMPTY_SQLRES // compiler warning
}

sqlres set_fields(string &tname, PrimaryKey &pk, vector<string> &fields,
                  vector<StorageValue> &vals, bool update) {
  CHECK_STORAGE_INITIALIZATION
  string qstr  = "INSERT OR REPLACE INTO " + tname + " (";
  qstr        += create_comma_separated_list(fields);
  qstr        += ") VALUES (";
  qstr        += create_comma_separated_list(vals);
  qstr        += ");";
  if (DebugSQL) LOG(DEBUG) << "set_fields: qstr: " << qstr << endl;
  return exec_sql(qstr);
}

sqlres fetch_field(string &tname, PrimaryKey &pk, string fname,
                   StorageCondition &cond, value_type vt) {
  CHECK_STORAGE_INITIALIZATION
  string qstr = "SELECT " + fname + " FROM " + tname +
                " WHERE " + cond.to_string() + ";";
  return __fetch_field(qstr, vt);
}

sqlres scan_fetch_single_field(string &tname, string fname,
                               StorageCondition &cond, value_type vt) {
  CHECK_STORAGE_INITIALIZATION
  PrimaryKey pk;
  return fetch_field(tname, pk, fname, cond, vt);
}

static sqlres __scan_fetch_fields(string &tname, vector<string> &fields,
                                  value_type vt) {
  string qstr  = "SELECT ";
  qstr        += create_comma_separated_list(fields);
  qstr        += " FROM " + tname + ";";
  return __fetch_fields(qstr, vt);
}

sqlres scan_fetch_fields_json_map(string &tname, vector<string> &fields) {
  CHECK_STORAGE_INITIALIZATION
  return __scan_fetch_fields(tname, fields, VT_JMAP);
}

sqlres scan_fetch_fields_array(string &tname, vector<string> &fields) {
  CHECK_STORAGE_INITIALIZATION
  return __scan_fetch_fields(tname, fields, VT_ARRAY);
}

// TODO: need a new sql_cb (& VT_TYPE) that adds ALL columns
//       VT_JMAP only saves 2 columns
sqlres scan_fetch_all_fields(string &tname, string &pkfield) {
  CHECK_STORAGE_INITIALIZATION
  string qstr = "SELECT " + pkfield + ",* FROM " + tname + ";";
  return __fetch_fields(qstr, VT_JMAP);
}

sqlres update_increment_field(string &tname, PrimaryKey &pk, string &fname,
                              Int64 byval, StorageCondition &cond) {
  CHECK_STORAGE_INITIALIZATION
  string qstr = "UPDATE " + tname + " SET " + fname + " = " +
                fname + " + " + to_string(byval) +
                " WHERE " + cond.to_string() + ";";
  if (DebugSQL) LOG(DEBUG) << "update_increment_field: qstr: " << qstr << endl;
  return exec_sql(qstr);
}

sqlres delete_key(string &tname, PrimaryKey &pk, StorageCondition &cond) {
  CHECK_STORAGE_INITIALIZATION
  string qstr = "DELETE FROM " + tname + " WHERE " + cond.to_string() + ";";
  return exec_sql(qstr);
}

