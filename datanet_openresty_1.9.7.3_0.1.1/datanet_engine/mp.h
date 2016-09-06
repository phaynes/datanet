
#ifndef __DATANET_MSGPACKER__H
#define __DATANET_MSGPACKER__H

#include <string>

#include <msgpack.h>

#include "storage.h"

using namespace std;

msgpack_sbuffer zmp_pack(jv *jval);

jv              zmp_unpack(const char *buf, size_t len);

#endif
