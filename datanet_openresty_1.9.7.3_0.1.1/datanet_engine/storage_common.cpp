#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>
#include <queue>
#include <mutex>

extern "C" {
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

#include "json/json.h"

#include "easylogging++.h"

#include "storage.h"

using namespace std;


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
// #DEFINES ------------------------------------------------------------------

#define USING_MEMORY_LMDB_PLUGIN


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static sqlres json_to_sqlres(sqlres &sr, value_type vt) {
  jv *jval = &(sr.jres);
  LDATA("json_to_sqlres: VT: "   << vt << " JVAL: " << *jval);
  if (vt == VT_STRING) {
    if (zjn(jval)) RETURN_EMPTY_SQLRES
    else           RETURN_SQL_RESULT_STRING(jval->asString())
  } else if (vt == VT_UINT) {
    if (zjn(jval)) RETURN_EMPTY_SQLRES
    else           RETURN_SQL_RESULT_UINT(jval->asUInt64())
  } else if (vt == VT_ARRAY) {
    sqlres  srret;
    srret.jres    = JARRAY;
    jv     *jrval = &(srret.jres);
    if (zjd(jval)) {
      if (jval->isObject()) {
        for (jv::iterator it = jval->begin(); it != jval->end(); it++) {
          jv jel = *it;
          zh_jv_append(jrval, &jel);
        }
      } else { // ARRAY
        for (uint i = 0; i < jval->size(); i++) {
          jv *jel = &((*jval)[i]);
          zh_jv_append(jrval, jel);
        }
      }
    }
    return srret;
  } else if (vt == VT_JMAP) {
    return sr;
  }
  zh_fatal_error("json_to_sqlres");
  RETURN_EMPTY_SQLRES // compiler warning
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CONDITION MATCHING --------------------------------------------------------

static bool equal_condition_value(jv &jval, SingleStorageCondition &sc) {
  if (sc.ivalue) {
    Int64 i = jval.asInt64();
    LDATA("equal_condition_value: INT: i: " << i << " CV: " << sc.ivalue);
    return (i == sc.ivalue);
  } else {
    string s = jval.asString();
    LDATA("equal_condition_value: STRING: s: " << s << " CV: " << sc.value);
    return (s.compare(sc.value) == 0);
  }
}

static bool storage_eq_condition_match(jv &jrow, StorageCondition &cond) {
  LDATA("storage_eq_condition_match: JROW: " << jrow <<
        " COND: " << cond.to_string());
  for (unsigned int i = 0; i < cond.size(); i++) {
    SingleStorageCondition sc = cond.get(i);
    if (!zh_jv_is_member(&jrow, sc.key)) return false;
    jv jval = jrow[sc.key];
    if (!equal_condition_value(jval, sc)) return false;
  }
  return true;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ABSTRACT DATA CALLS -------------------------------------------------------

sqlres lock_field(string &tname, PrimaryKey &pk, StorageCondition &cond) {
  LDATA("lock_field: T: " << tname << " PK: " << pk.to_string());
  bool success = plugin_add_key(tname, pk.to_string());
  if (!success) RETURN_EMPTY_SQLRES       // LOCKED
  else          RETURN_SQL_RESULT_UINT(1) // NOT LOCKED
}

sqlres unlock_field(string &tname, PrimaryKey &pk, StorageCondition &cond) {
  LDATA("unlock_field: T: " << tname << " PK: " << pk.to_string());
  return delete_key(tname, pk, cond);
}

static sqlres __set_fields(string &tname, PrimaryKey &pk,
                           vector<string> &fields, vector<StorageValue> &vals,
                           jv &jval) {
  vector<string>::iterator       itf  = fields.begin();
  vector<StorageValue>::iterator itv  = vals.begin();
  for (; itf != fields.end() && itv != vals.end(); itf++, itv++) {
    jval[*itf] = itv->raw();
  }
  LDATA("__set_fields: JVAL " << jval);
  bool ok = plugin_set_key(tname, pk.to_string(), jval);
  if (!ok) RETURN_SQL_RESULT_ERROR("__set_fields: plugin_set_key FAILED")
  else     RETURN_EMPTY_SQLRES
}

sqlres set_fields(string &tname, PrimaryKey &pk, vector<string> &fields,
                  vector<StorageValue> &vals, bool update) {
  LDATA("set_fields: T: " << tname << " PK: " << pk.to_string());
  jv jval = update ? plugin_get_key(tname, pk.to_string()) : JNULL;
  return __set_fields(tname, pk, fields, vals, jval);
}

static void row_fetch_two_fields_json_map(jv &jres, jv &jrow,
                                          vector<string> &fields) {
  string kname = fields[0];
  string vname = fields[1];
  if (zh_jv_is_member(&jrow, kname) && zh_jv_is_member(&jrow, vname)) {
    string kval = jrow[kname].asString();
    jres[kval]  = jrow[vname];
  }
}

sqlres scan_fetch_fields_json_map(string &tname, vector<string> &fields) {
  LDATA("scan_fetch_fields_json_map: T: " << tname);
  jv jtbl = plugin_get_table(tname);
  LDATA("scan_fetch_fields_json_map: JTBL: " << jtbl);
  if (zjn(&jtbl) || jtbl.size() == 0) RETURN_EMPTY_SQLRES
  else {
    sqlres sr;
    sr.jres = JOBJECT;
    zh_type_object_assert(&jtbl, "LOGIC(scan_fetch_fields_json_map)");
    for (jv::iterator it = jtbl.begin(); it != jtbl.end(); it++) {
      jv jrow  = (*it);
      LDATA("scan_fetch_fields_json_map: JROW: " << jrow);
      row_fetch_two_fields_json_map(sr.jres, jrow, fields);
    }
    LDATA("FETCHED: scan_fetch_fields_json_map: T: " << tname <<
          " JRES: " << sr.jres);
    return json_to_sqlres(sr, VT_JMAP);
  }
}

sqlres scan_fetch_fields_array(string &tname, vector<string> &fields) {
  LDATA("scan_fetch_fields_array: T: " << tname);
  jv jtbl = plugin_get_table(tname);
  LDATA("scan_fetch_fields_array: JTBL: " << jtbl);
  if (zjn(&jtbl) || jtbl.size() == 0) RETURN_EMPTY_SQLRES
  else {
    sqlres sr;
    sr.jres = JARRAY;
    zh_type_object_assert(&jtbl, "LOGIC(scan_fetch_fields_array)");
    for (jv::iterator it = jtbl.begin(); it != jtbl.end(); it++) {
      jv     jrow  = (*it);
      LDATA("scan_fetch_fields_array: JROW: " << jrow);
      string kname = fields[0];
      if (zh_jv_is_member(&jrow, kname)) {
        jv jkval = jrow[kname];
        zh_jv_append(&(sr.jres), &jkval);
      }
    }
    LDATA("FETCHED: scan_fetch_fields_array: T: " << tname <<
          " JRES: " << sr.jres);
    return json_to_sqlres(sr, VT_ARRAY);
  }
}

sqlres scan_fetch_all_fields(string &tname, string &pkfield) {
  LDATA("scan_fetch_all_fields: T: " << tname);
  jv jtbl = plugin_get_table(tname);
  LDATA("scan_fetch_all_fields: JTBL: " << jtbl);
  if (zjn(&jtbl) || jtbl.size() == 0) RETURN_EMPTY_SQLRES
  else {
    sqlres sr;
    sr.jres = JOBJECT;
    zh_type_object_assert(&jtbl, "LOGIC(scan_fetch_all_fields)");
    for (jv::iterator it = jtbl.begin(); it != jtbl.end(); it++) {
      jv jrow  = (*it);
      LDATA("scan_fetch_all_fields JROW: " << jrow);
      if (zh_jv_is_member(&jrow, pkfield)) {
        string kval   = jrow[pkfield].asString();
        sr.jres[kval] = jrow;
      }
    }
    LDATA("FETCHED: scan_fetch_all_fields T: " << tname <<
          " JRES: " << sr.jres);
    return json_to_sqlres(sr, VT_JMAP);
  }
}

sqlres scan_fetch_single_field(string &tname, string fname,
                               StorageCondition &cond, value_type vt) {
  LDATA("scan_fetch_single_field: T: " << tname << " F: " << fname);
  jv jtbl = plugin_get_table(tname);
  LDATA("scan_fetch_single_field: JTBL: " << jtbl);
  if (zjn(&jtbl) || jtbl.size() == 0) RETURN_EMPTY_SQLRES
  else {
    zh_type_object_assert(&jtbl, "LOGIC(scan_fetch_single_field)");
    for (jv::iterator it = jtbl.begin(); it != jtbl.end(); it++) {
      jv     jrow = (*it);
      jv     jkey = it.key();
      string key  = jkey.asString();
      if (storage_eq_condition_match(jrow, cond) &&
          zh_jv_is_member(&jrow, fname)) {
        sqlres sr;
        sr.jres = jrow[fname];
        LDATA("FETCHED: scan_fetch_single_field: T: " << tname << 
              " JRES: " << sr.jres);
        return json_to_sqlres(sr, vt); // HIT
      }
    }
    RETURN_EMPTY_SQLRES // MISS
  }
}

sqlres update_increment_field(string &tname, PrimaryKey &pk, string &fname,
                              Int64 byval, StorageCondition &cond) {
  LDATA("update_increment_field: T: " << tname << " PK: " << pk.to_string());
  Int64 nval;
  jv jval = plugin_get_key(tname, pk.to_string());
  if (zjn(&jval)) {
    nval = 0;
    LDATA("update_increment_field: ROW DOES NOT EXIST -> INITIALIZE(ZERO)");
  } else {
    if (!zh_jv_is_member(&jval, fname)) {
      nval = 0;
      LDATA("update_increment_field: FIELD DOES NOT EXIST -> INITIALIZE(ZERO)");
    } else {
      jv jfval = jval[fname];
      if (!jfval.isIntegral()) {
        RETURN_SQL_RESULT_ERROR("update_increment_field on NON-number")
      } else {
        nval = jfval.asInt64();
      }
    }
  }
  sqlres sr;
  nval        += byval;
  jval[fname]  = nval;
  sr.jres      = jval;
  sr.ures      = nval;
  bool   ok    = plugin_set_key(tname, pk.to_string(), jval);
  if (!ok) {
    RETURN_SQL_RESULT_ERROR("update_increment_field: plugin_set_key FAILED")
  }
  return sr;
}

sqlres delete_key(string &tname, PrimaryKey &pk, StorageCondition &cond) {
  LDATA("delete_key: T: " << tname << " PK: " << pk.to_string());
  bool ok = plugin_delete_key(tname, pk.to_string());
  if (!ok) RETURN_SQL_RESULT_ERROR("delete_key: plugin_delete_key FAILED")
  else     RETURN_EMPTY_SQLRES
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FETCH-FIELD ---------------------------------------------------------------

static sqlres normal_fetch_field(string &tname, PrimaryKey &pk, string fname,
                                 StorageCondition &cond, value_type vt) {
  LDATA("fetch_field: T: " << tname << " PK: " << pk.to_string() <<
        " F: " << fname);
  sqlres sr;
  sr.jres = plugin_get_key_field(tname, pk.to_string(), fname);
  LDATA("FETCHED: T: " << tname << " PK: " << pk.to_string() <<
        " VAL: " << sr.jres);
  return json_to_sqlres(sr, vt);
}

#ifdef USING_MEMORY_LMDB_PLUGIN

#include "logic_storage_lmdb.h"
extern map<string, MemoryTable> MemoryStorage;

sqlres fetch_field(string &tname, PrimaryKey &pk, string fname,
                   StorageCondition &cond, value_type vt) {
  if (vt == VT_JMAP) {
    sqlres sr;
    string pks = pk.to_string();
    bool isst  = is_shared_table(tname);
    if (isst) {
      sr.jres = lmdb_plugin_get_key_field(tname, pks, fname);
      return sr;
    } else {
      uint cnt = MemoryStorage.count(tname);
      if (cnt == 0) return sr;
      cnt = MemoryStorage[tname].count(pks);
      if (cnt == 0) return sr;
      if (!zh_jv_is_member(&(MemoryStorage[tname][pks]), fname)) return sr;
      sr.jres = MemoryStorage[tname][pks][fname]; // COPY
      return sr;
    }
  } else {
    return normal_fetch_field(tname, pk, fname, cond, vt);
  }
}
#else
sqlres fetch_field(string &tname, PrimaryKey &pk, string fname,
                   StorageCondition &cond, value_type vt) {
  return normal_fetch_field(tname, pk, fname, cond, vt);
}
#endif

