#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>

extern "C" {
  #include <time.h>
  #include <sys/time.h>
  #include <sys/types.h>
  #include <sys/un.h>
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

#include "json/json.h"
#include "easylogging++.h"

#include "helper.h"
#include "shared.h"
#include "storage.h"

using namespace std;

extern Int64 MyUUID;

extern string zh_wildcard_user;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

map<string, string> UserPasswordMap;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ERROR HELPERS -------------------------------------------------------------


#define RETURN_AUTH_YES  {sqlres sr(1); return sr;}
#define RETURN_AUTH_NO   {sqlres sr(0); return sr;}
#define RETURN_AUTH_MISS {                  \
  sqlres sr;                                \
  sr.err = ZS_Errors["AuthenticationMiss"]; \
  return sr;                                \
}

#define RETURN_WRITE_PERMISSION { \
    sqlres srperms;               \
    srperms.sres = "W";           \
    return srperms;               \
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static sqlres authenticate(jv *jauth) {
  string username = (*jauth)["username"].asString();
  LD("authenticate: USER: " << username);
  string password = (*jauth)["password"].asString();
  map<string, string>::iterator it = UserPasswordMap.find(username);
  if (it != UserPasswordMap.end()) {
    string mem_pass = it->second;
    bool   ok       = !mem_pass.compare(password);
    LT("authenticate: MEMORY: OK: " << ok);
    if (ok) RETURN_AUTH_YES
    else    RETURN_AUTH_NO
  }
  sqlres sr      = fetch_user_password_hash(username);
  RETURN_SQL_ERROR(sr)
  string phash   = sr.sres;
  if (!phash.size()) {
    LT("authenticate: MISS");
    RETURN_AUTH_MISS;
  }
  string hash    = zh_local_hash(password);
  bool   ok      = !hash.compare(hash);
  LT("authenticate: DB: OK: " << ok);
  if (ok) UserPasswordMap.insert(make_pair(username, password));
  if (ok) RETURN_AUTH_YES
  else    RETURN_AUTH_NO
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEFAULT BACKDOOR ----------------------------------------------------------

static string DefaultChannelID = "0";

static bool check_default_backdoor(string &schanid) {
  if (schanid.compare(DefaultChannelID)) return false;
  LD("check_default_backdoor: TRUE");
  return true;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BASIC AUTHENTICATION ------------------------------------------------------

sqlres zauth_basic_authentication(jv *jauth) {
  LT("zauth_basic_authentication");
  return authenticate(jauth);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WILDCARD PERMISSIONS ------------------------------------------------------

static sqlres get_wildcard_perms(string &schanid) {
  string username = zh_wildcard_user;
  return fetch_user_permission(username, schanid);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET DATA/SUBSCRIBE PERMISSIONS --------------------------------------------

static sqlres get_key_to_user_perms(string &schanid, jv *jauth) {
  LT("get_key_to_user_perms");
  string username = (*jauth)["username"].asString();
  sqlres sr       = fetch_user_permission(username, schanid);
  RETURN_SQL_ERROR(sr)
  string perms    = sr.sres;
  if (perms.size()) return sr;
  else {
    sr            = get_wildcard_perms(schanid);
    RETURN_SQL_ERROR(sr)
    string operms = sr.sres;
    if (!operms.size()) RETURN_EMPTY_SQLRES
    else {
      if (!operms.compare("R*")) sr.sres = "R";
      else                       sr.sres = "W";
      return sr;
    }
  }
}

// NOTE: used in exports.HasAgentStorePermissions()
static sqlres get_simple_rchan_data_perms(string &kqk,     UInt64  duuid,
                                          string &schanid, jv     *jauth) {
  string username = (*jauth)["username"].asString();
  LT("get_simple_rchan_data_perms: K: " << kqk << " R: " << schanid <<
       " UN: " << username <<" U: " << duuid);
  if (check_default_backdoor(schanid)) RETURN_WRITE_PERMISSION
  sqlres sr       = get_key_to_user_perms(schanid, jauth);
  RETURN_SQL_ERROR(sr)
  string kperms   = sr.sres;
  if (kperms.size()) return sr;
  else               return fetch_user_subscription(username, schanid);
}

// AGENT needs permission and SUBSCRIBED or CACHED
static sqlres get_agent_rchan_data_perms(string &kqk, string &schanid,
                                         jv *jauth) {
  string username = (*jauth)["username"].asString();
  LT("get_agent_rchan_data_perms: K: " << kqk << " R: " << schanid <<
       " UN: " << username);
  if (check_default_backdoor(schanid)) RETURN_WRITE_PERMISSION
  sqlres sr       = fetch_user_subscription(username, schanid);
  RETURN_SQL_ERROR(sr)
  string uperms   = sr.sres;
  if (uperms.size()) return sr; // SUBSCRIBED
  sr              = fetch_key_cached_by_user(kqk, username);
  RETURN_SQL_ERROR(sr)
  bool   cached   = sr.sres.size() ? true : false;
  if (!cached) RETURN_EMPTY_SQLRES // NOT CACHED & NOT SUBSCRIBED
  return get_key_to_user_perms(schanid, jauth);
}

static sqlres get_rchan_data_perms(string &kqk, UInt64 duuid, string &schanid,
                                   bool simple, jv *jauth) {
  if (simple) return get_simple_rchan_data_perms(kqk, duuid, schanid, jauth);
  else        return get_agent_rchan_data_perms(kqk, schanid, jauth);
}

static sqlres get_all_write_perms(string &kqk, UInt64 duuid, jv *jrchans,
                                  bool simple, jv *jauth) {
  LT("get_all_write_perms: " << *jrchans);
  if (!jrchans->isArray()) {
    RETURN_SQL_RESULT_ERROR(ZS_Errors["RChansNotArray"])
  }
  if (jrchans->size()) {
    string schanid = (*jrchans)[0].asString();
    if (check_default_backdoor(schanid)) RETURN_WRITE_PERMISSION
  }
  sqlres srperms;
  for (uint i = 0; i < jrchans->size(); i++) {
    string rchan = (*jrchans)[i].asString();
    sqlres sr    = get_rchan_data_perms(kqk, duuid, rchan, simple, jauth);
    RETURN_SQL_ERROR(sr)
    string p     = sr.sres;
    if        (!p.size()) RETURN_EMPTY_SQLRES // ALL or NOTHING
    else if   (!p.compare("R")) {             // 'R' is dominant
      srperms.sres = "R";
    } else if (!srperms.sres.size()) {        // 'W' is recessive
      srperms.sres = "W";
    }
  }
  return srperms;
}

static sqlres get_rw_perms(string &kqk, UInt64 duuid, jv *jrchans,
                           bool simple, jv *jauth) {
  LT("get_rw_perms: RCHANS: " << *jrchans);
  if (!jrchans->isArray()) {
    RETURN_SQL_RESULT_ERROR(ZS_Errors["RChansNotArray"])
  }
  sqlres srperms;
  for (uint i = 0; i < jrchans->size(); i++) {
    string rchan = (*jrchans)[i].asString();
    sqlres sr    = get_rchan_data_perms(kqk, duuid, rchan, simple, jauth);
    RETURN_SQL_ERROR(sr)
    string p     = sr.sres;
    if        (!p.compare("W")) { // 'W' is dominant
      srperms.sres = "W";
      return srperms;
    } else if (!p.compare("R")) { // 'R' is recessive
      srperms.sres = "R";
    }
  }
  return srperms;
}


static sqlres get_data_perms(string &kqk, UInt64 duuid, jv *jrchans,
                             bool all, bool simple, jv *jauth) {
  if (all) return get_all_write_perms(kqk, duuid, jrchans, simple, jauth);
  else     return get_rw_perms       (kqk, duuid, jrchans, simple, jauth);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// READ & WRITE PERMISSIONS --------------------------------------------------

sqlres zauth_has_read_permissions(jv *jauth, string &kqk) {
  LT("zauth_has_read_permissions: K: " << kqk);
  sqlres srmatch = zauth_basic_authentication(jauth);
  RETURN_SQL_ERROR(srmatch)
  if (!srmatch.ures) RETURN_AUTH_NO;
  sqlres  sc;
  FETCH_CRDT(kqk);
  RETURN_SQL_ERROR(sc)
  jv     *jocrdt   = &(sc.jres);
  if (zjn(jocrdt)) {
    LT("zauth_has_read_permissions: MISS");
    RETURN_SQL_RESULT_ERROR(ZS_Errors["NoDataFound"])
  }
  jv     *jrchans  = &((*jocrdt)["_meta"]["replication_channels"]);
  string  username = (*jauth)["username"].asString();
  bool    all      = false;
  bool    simple   = false;
  sqlres  srperm   = get_data_perms(kqk, MyUUID, jrchans, all, simple, jauth);
  RETURN_SQL_ERROR(srperm)
  if (srperm.sres.size()) RETURN_AUTH_YES // BOTH [R,W]
  else                    RETURN_AUTH_NO
}

sqlres zauth_has_write_permissions(jv *jauth, string &kqk, jv *jrchans) {
  LT("zauth_has_write_permissions");
  sqlres srmatch = zauth_basic_authentication(jauth);
  RETURN_SQL_ERROR(srmatch)
  if (!srmatch.ures) RETURN_AUTH_NO;
  string username = (*jauth)["username"].asString();
  bool   all      = true;
  bool   simple   = false;
  sqlres srperm   = get_data_perms(kqk, MyUUID, jrchans, all, simple, jauth);
  RETURN_SQL_ERROR(srperm)
  if (srperm.sres.size()) RETURN_AUTH_YES // BOTH [R,W]
  else                    RETURN_AUTH_NO
}

sqlres zauth_has_write_permissions_on_key(jv *jauth, string &kqk) {
  LT("zauth_has_write_permissions_on_key");
  sqlres  sc;
  FETCH_CRDT(kqk);
  RETURN_SQL_ERROR(sc)
  jv     *jocrdt  = &(sc.jres);
  if (zjn(jocrdt)) {
    RETURN_SQL_RESULT_ERROR(ZS_Errors["NoDataFound"])
  }
  jv     *jrchans = &((*jocrdt)["_meta"]["replication_channels"]);
  return zauth_has_write_permissions(jauth, kqk, jrchans);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBE PERMISSIONS -----------------------------------------------------

static sqlres get_subscribe_perms(jv *jauth, string &schanid) {
  string username = (*jauth)["username"].asString();
  return fetch_user_permission(username, schanid);
}

sqlres zauth_has_subscribe_permissions(jv *jauth, string &schanid) {
  LT("zauth_has_subscribe_permissions");
  sqlres srmatch = zauth_basic_authentication(jauth);
  RETURN_SQL_ERROR(srmatch)
  if (!srmatch.ures) RETURN_AUTH_NO;
  sqlres sr      = get_subscribe_perms(jauth, schanid);
  RETURN_SQL_ERROR(sr)
  string priv    = sr.sres;
  if (priv.size()) RETURN_AUTH_YES
  else             RETURN_AUTH_MISS
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HAS AGENT STORE PERMISSIONS -----------------------------------------------

sqlres zauth_has_simple_store_permissions(jv *jauth, string &kqk, jv *jrchans) {
  LT("zauth_has_simple_store_permissions");
  bool   all    = true;
  bool   simple = true; // NOTE: simple is true, SUBSCRIBE or CACHE NOT NEEDED
  sqlres srperm = get_data_perms(kqk, MyUUID, jrchans, all, simple, jauth);
  RETURN_SQL_ERROR(srperm)
  if (!srperm.sres.size()) RETURN_AUTH_MISS
  else {
    if (!srperm.sres.compare("W")) RETURN_AUTH_YES
    else                           RETURN_AUTH_NO
  }
}

sqlres zauth_has_agent_store_permissions(jv *jauth, string &kqk, jv *jrchans) {
  LT("zauth_has_agent_store_permissions");
  sqlres srmatch = zauth_basic_authentication(jauth);
  RETURN_SQL_ERROR(srmatch)
  if (!srmatch.ures) RETURN_AUTH_NO;
  bool   all      = true;
  bool   simple   = false; // NOTE simple is false, this is the AGENT check
  sqlres srperm   = get_data_perms(kqk, MyUUID, jrchans, all, simple, jauth);
  RETURN_SQL_ERROR(srperm)
  if (!srperm.sres.size()) RETURN_AUTH_MISS
  else {
    if (!srperm.sres.compare("W")) RETURN_AUTH_YES
    else                           RETURN_AUTH_NO
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// IS AUTO CACHE KEY ---------------------------------------------------------

static sqlres is_subscribed(jv *jauth, string &kqk, jv *jrchans) {
  string username = (*jauth)["username"].asString();
  for (uint i = 0; i < jrchans->size(); i++) {
    string schanid = (*jrchans)[i].asString();
    sqlres sr      = fetch_user_subscription(username, schanid);
    RETURN_SQL_ERROR(sr)
    string uperms   = sr.sres;
    if (uperms.size()) RETURN_SQL_RESULT_UINT(1) // SUBSCRIBED
  }
  RETURN_EMPTY_SQLRES // NOT SUBSCRIBED
}

sqlres zauth_is_agent_auto_cache_key(jv *jauth, string &kqk, jv *jrchans) {
  string username   = (*jauth)["username"].asString();
  LD("ZAuth.IsAgentAutoCacheKey: U: " << username << " K: " << kqk);
  sqlres sr         = is_subscribed(jauth, kqk, jrchans);
  RETURN_SQL_ERROR(sr)
  bool   subscribed = sr.ures ? true : false;
  if (subscribed) RETURN_EMPTY_SQLRES // SUBSCRIBED -> NOT AUTO-CACHE
  else {
    sqlres sr     = fetch_cached_keys(kqk);
    RETURN_SQL_ERROR(sr)
    bool   cached = sr.ures ? true : false;
    sr.ures       = !cached;
    return sr;
  }
}

