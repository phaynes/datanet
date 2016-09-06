
#ifndef __DATANET_DEPRECATED_ENGINE__H
#define __DATANET_DEPRECATED_ENGINE__H

#include <string>

extern "C" {
  #include "lua.h"
}

#include "json/json.h"

#include "easylogging++.h"

#include "helper.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEPRECATED ----------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------

// Called from lua_engine_hooks.cpp
int engine_start_primary_network(const char   *chost, unsigned int cport,
                                 unsigned int  iport);
int engine_start_worker(unsigned int iport);

typedef struct engine_request_t {
  jv request;
  jv response;
  jv addtl;
} erequest;

// Called from engine_network.cpp
int engine_get_primary_worker_port();
int engine_get_key_worker_port(const char *ckqk);

Int64 engine_get_worker_uuid_for_operation(string kqk);

void engine_process_remote_method_call(int fd, string &pdata);

void engine_process_server_method(jv *jreq);
void engine_process_primary_method(jv *jreq);
void engine_process_primary_ack(jv *jreq);
void engine_process_central_ack(jv *jreq);

void engine_send_to_primary_worker(jv &irequest);
void engine_send_to_key_worker(string &kqk, jv &irequest);
void engine_send_primary_to_proxy(jv *prequest, jv *addtl);
void engine_send_central_need_merge(string &kqk, string &sectok, UInt64 gcv);

int engine_send_binary_fd(int pfd, jv *prequest);
int engine_send_wakeup_worker(int wfd);
int engine_send_worker_settings(int wfd);

int  engine_handle_reconnect_to_central_event(int pfd);
bool engine_handle_broken_central_connection_event();

#endif
