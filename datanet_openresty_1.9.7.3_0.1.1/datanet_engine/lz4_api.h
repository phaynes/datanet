
#ifndef __DATANET_LZ4_API__H
#define __DATANET_LZ4_API__H

#include "helper.h"
#include "storage.h"

using namespace std;

void        lz4_compress_append(byte_buffer &b, const char *in, size_t in_len);
byte_buffer lz4_compress (const char *in,  size_t in_len);

byte_buffer lz4_decompress(const char *in, size_t in_len);

#endif
