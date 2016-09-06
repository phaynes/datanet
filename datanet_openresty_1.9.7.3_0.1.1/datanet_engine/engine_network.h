
#ifndef __DATANET_ENGINE_NETWORK__H
#define __DATANET_ENGINE_NETWORK__H

#include "json/json.h"

#include "engine.h"

int network_send_binary_fd(int fd, const char *rp, int rlen);
int network_send_central(string &r);
int network_send_primary(const char *rp, int rlen);
int network_send_internal(Int64 wid, string &r);
int network_respond_remote_call(int fd, const char *rp, int rlen);
int network_broadcast_method(const char *rp, int rlen);

int network_reconnect_central_client(bool start);
jv  network_finalize_worker_primary_connection();

int network_worker_initialize(unsigned int iport, unsigned int wport);
int network_primary_initialize(const char   *chost,  unsigned int cport,
                               unsigned int  iport, unsigned int  wport);
int network_central_initialize();

#endif
