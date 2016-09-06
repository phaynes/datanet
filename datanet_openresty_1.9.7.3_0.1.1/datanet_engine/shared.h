
#ifndef __DATANET_SHARED__H
#define __DATANET_SHARED__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

string zs_get_dirty_subscriber_delta_key(string &kqk, UInt64 auuid,
                                         string &avrsn);
string zs_get_dirty_subscriber_delta_key(string &kqk, jv *jauthor);
string zs_get_dirty_agent_delta_key(string &kqk, string &avrsn);

string zs_create_kqk_from_string(string &ns, string &cn, string &id);
string zs_create_kqk(jv &jns, jv &jcn, jv &jid);
jv zs_create_jks(string &kqk);

string zs_get_persist_delta_key(string &kqk, string &avrsn);

string zs_get_ka_key(string &kqk, UInt64 auuid);
string zs_get_kav_key(string &kqk, string &avrsn);
string zs_get_g_key(string &kqk, UInt64 gcv);

void zs_init_ZS_Errors();

extern map<string, string> ZS_Errors;

#endif
