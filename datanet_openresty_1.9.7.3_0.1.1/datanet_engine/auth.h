
#ifndef __DATANET_AUTH__H
#define __DATANET_AUTH__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

sqlres zauth_basic_authentication(jv *jauth);

sqlres zauth_has_read_permissions(jv *jauth, string &kqk);
sqlres zauth_has_write_permissions(jv *jauth, string &kqk, jv *jrchans);
sqlres zauth_has_write_permissions_on_key(jv *jauth, string &kqk);

sqlres zauth_has_subscribe_permissions(jv *jauth, string &schanid);

sqlres zauth_has_agent_store_permissions(jv *jauth, string &kqk, jv *jrchans);
sqlres zauth_has_simple_store_permissions(jv *jauth, string &kqk, jv *jrchans);

sqlres zauth_has_agent_cache_permissions(jv *jauth, string &kqk);

sqlres zauth_is_agent_auto_cache_key(jv *jauth, string &kqk, jv *jrchans);

#endif
