
#ifndef __DATANET_AIO__H
#define __DATANET_AIO__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

jv zaio_build_agent_online_request(bool b);

void zaio_cleanup_delta_before_send_central(jv *jdentry);

#endif
