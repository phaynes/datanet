
#ifndef __DATANET_DCOMPRESS__H
#define __DATANET_DCOMPRESS__H

#include <string>

#include "helper.h"
#include "storage.h"

using namespace std;

void zcmp_initialize();

void zcmp_compress_meta  (jv *jmeta);
void zcmp_decompress_meta(jv *jmeta);

void zcmp_compress_crdt  (jv *jcrdt);
void zcmp_decompress_crdt(jv *jcrdt);

#endif
