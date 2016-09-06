#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>
#include <iostream>
#include <vector>

#include "json/json.h"
#include "easylogging++.h"

#include "lz4_api.h"
#include "helper.h"
#include "storage.h"

using namespace std;

extern "C" {
  #include <errno.h>
  #include <stdlib.h>
  #include <memory.h>

  #include "lua-lz4/lz4/lz4frame.h"

  #include "lua-lz4/lz4/lz4.h"
  #include "lua-lz4/lz4/lz4hc.h"
} // EXTERN C


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG SETTINGS ------------------------------------------------------------

#define DISABLE_LT
#ifdef DISABLE_LT
  #undef LT
  #define LT(text) /* text */
#endif


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PUBLIC API HELPERS --------------------------------------------------------

#if 0
void set_compress_settings(LZ4F_preferences_t *settings) {
  LZ4F_preferences_t stack_settings;
  memset(&stack_settings, 0, sizeof(stack_settings));
  settings = &stack_settings; settings->compressionLevel, settings->autoFlush
  settings->frameInfo.blockSizeID, settings->frameInfo.blockMode
  settings->frameInfo.contentChecksumFlag=
}
#endif

static byte_buffer decompression_failed(LZ4F_errorCode_t            code,
                                        LZ4F_decompressionContext_t ctx) {
  byte_buffer b;
  if (ctx != NULL) LZ4F_freeDecompressionContext(ctx);
  LE("ERROR: lz4_decompress: " << LZ4F_getErrorName(code));
  return b;
}

static void append_buffer(char *out, byte_buffer &b, size_t len) {
  for (uint i = 0; i < len; i++) {
    char *c = (out + i);
    b.push_back(*c);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PUBLIC API ----------------------------------------------------------------

//TODO STREAM-COMPRESS: fixed-size buffer & compress-loop
void lz4_compress_append(byte_buffer &b, const char *in, size_t in_len) {
  LT("lz4_compress_append");
  LZ4F_preferences_t *settings = NULL;
  //set_compress_settings(settings);
  size_t  bound = LZ4F_compressFrameBound(in_len, settings);
  char   *out   = (char *) malloc(bound); // MALLOC -------------------------->
  if (!out) {
    LE("ERROR: MALLOC: OUT-OF-MEMORY");
    return;
  }
  size_t r   = LZ4F_compressFrame(out, bound, in, in_len, settings);
  bool   err = LZ4F_isError(r);
  if (!err) append_buffer(out, b, r);
  free(out);                              // FREE ---------------------------->
}

byte_buffer lz4_compress(const char *in, size_t in_len) {
  LT("lz4_compress");
  byte_buffer b;
  lz4_compress_append(b, in, in_len);
  return b;
}

byte_buffer lz4_decompress(const char *in, size_t in_len) {
  LT("lz4_decompress");
  const char *p     = in;
  size_t      p_len = in_len;
  LZ4F_decompressionContext_t ctx = NULL;
  //LZ4F_frameInfo_t info;
  LZ4F_errorCode_t code = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
  if (LZ4F_isError(code)) return decompression_failed(code, ctx);

  byte_buffer b;
  size_t      out_len = BUFFER_SIZE;
  char        out[BUFFER_SIZE];
  while (1) {
    size_t advance = p_len;
    code           = LZ4F_decompress(ctx, out, &out_len, p, &advance, NULL);
    if (LZ4F_isError(code)) return decompression_failed(code, ctx);
    if (out_len == 0) break;
    p     += advance;
    p_len -= advance;
    append_buffer(out, b, out_len);
  }

  LZ4F_freeDecompressionContext(ctx);
  return b;
}


