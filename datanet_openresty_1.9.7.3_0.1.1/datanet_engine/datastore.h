
#ifndef __DATANET_DATASTORE__H
#define __DATANET_DATASTORE__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

sqlres zds_remove_agent_delta(string kqk, string avrsn);
sqlres zds_store_subscriber_delta(string kqk, jv *jdentry);

sqlres zds_remove_subscriber_delta(string kqk, jv *jauthor);
sqlres zds_store_agent_delta(string kqk, jv *jdentry, jv *jauth);

void zds_remove_channels(jv *jmeta);

sqlres zds_store_crdt(string kqk, jv *jcrdt);
sqlres zds_retrieve_crdt(string kqk);

sqlres zds_remove_key(string kqk);

#endif
