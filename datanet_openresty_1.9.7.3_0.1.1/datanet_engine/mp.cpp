
#include <iostream>
#include <string>

#include "json/json.h"
#include <msgpack.h>

#include "json/json.h"
#include "easylogging++.h"
#include <msgpack.h>

#include "helper.h"
#include "mp.h"
#include "storage.h"

using namespace std;

static void mp_json_object_pack(msgpack_packer *pk, jv *jdata);
static void mp_json_array_pack (msgpack_packer *pk, jv *jdata);

static void mp_pack_element(msgpack_packer *pk, string *k, jv *jval) {
  if (k) {
    msgpack_pack_str     (pk, k->size());
    msgpack_pack_str_body(pk, k->c_str(), k->size());
  }
  if        (jval->isNull()) {
    msgpack_pack_nil(pk);
  } else if (jval->isBool()) {
    bool b = jval->asBool();
    if (b) msgpack_pack_true(pk);
    else   msgpack_pack_false(pk);
  } else if (jval->isUInt()) {
    msgpack_pack_uint64(pk, jval->asUInt64());
  } else if (jval->isIntegral()) {
    msgpack_pack_int64(pk, jval->asInt64());
  } else if (jval->isString()) {
    string s = jval->asString();
    msgpack_pack_str     (pk, s.size());
    msgpack_pack_str_body(pk, s.c_str(), s.size());
  } else if (jval->isArray()) {
    mp_json_array_pack(pk, jval);
  } else if (jval->isObject()) {
    mp_json_object_pack(pk, jval);
  } else {
    LE("mp_pack_element: JVAL: " << *jval);
  }
}

static void mp_json_array_pack(msgpack_packer *pk, jv *jdata) {
  msgpack_pack_array(pk, jdata->size());
  for (uint i = 0; i < jdata->size(); i++) {
    jv *jval = &((*jdata)[i]);
    mp_pack_element(pk, NULL, jval);
  }
}

static void mp_json_object_pack(msgpack_packer *pk, jv *jdata) {
  vector<string> mbrs = zh_jv_get_member_names(jdata);
  msgpack_pack_map(pk, mbrs.size());
  for (uint i = 0; i < mbrs.size(); i++) {
    string  k    = mbrs[i];
    jv     *jval = &((*jdata)[k]);
    mp_pack_element(pk, &k, jval);
  }
}

msgpack_sbuffer zmp_pack(jv *jval) {
  msgpack_sbuffer sbuf;
  msgpack_sbuffer_init(&sbuf);
  msgpack_packer pk;
  msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
  mp_json_object_pack(&pk, jval);
  return sbuf;
}

static jv mp_unpack_obj_to_json(msgpack_object *mo) {
  jv  jdata; // NOTE: DEFINED BELOW
  int otype = mo->type;
  if        (otype == MSGPACK_OBJECT_MAP)   {
    jdata                         = JOBJECT;
    msgpack_object_kv*       p    = mo->via.map.ptr;
    msgpack_object_kv* const pend = mo->via.map.ptr + mo->via.map.size;
    for(; p < pend; ++p) {
      msgpack_object *k = &p->key;
      string          sk(k->via.str.ptr, k->via.str.size);
      msgpack_object *v = &p->val;
      jdata[sk]         = mp_unpack_obj_to_json(v);
    }
  } else if (otype == MSGPACK_OBJECT_ARRAY) {
    jdata                      = JARRAY;
    msgpack_object*       p    = mo->via.array.ptr;
    msgpack_object* const pend = mo->via.array.ptr + mo->via.array.size;
    for(; p < pend; ++p) {
      jv jval = mp_unpack_obj_to_json(p);
      zh_jv_append(&jdata, &jval);
    }
  } else if (otype == MSGPACK_OBJECT_STR)   {
    string s(mo->via.str.ptr, mo->via.str.size);
    jdata = s;
  } else if (otype == MSGPACK_OBJECT_POSITIVE_INTEGER)   {
    jdata = (UInt64)mo->via.u64;
  } else if (otype == MSGPACK_OBJECT_NEGATIVE_INTEGER)   {
    jdata = (Int64)mo->via.i64;
  } else if (otype == MSGPACK_OBJECT_FLOAT)              {
    jdata = mo->via.f64;
  } else if (otype == MSGPACK_OBJECT_BOOLEAN)            {
    jdata = mo->via.boolean;
  } else if (otype == MSGPACK_OBJECT_NIL)                {
    jdata = JNULL;
  } else {
    zh_fatal_error("LOGIC(mp_unpack_obj_to_json)");
  }
  return jdata;
}

jv zmp_unpack(const char *buf, size_t len) {
  jv                    jval;
  msgpack_unpacked      result;
  size_t                off = 0;
  msgpack_unpacked_init(&result);
  msgpack_unpack_return ret = msgpack_unpack_next(&result, buf, len, &off);
  if (ret == MSGPACK_UNPACK_SUCCESS) {
    msgpack_object obj = result.data;
    jval               = mp_unpack_obj_to_json(&obj);
    ret                = msgpack_unpack_next(&result, buf, len, &off);
  }
  msgpack_unpacked_destroy(&result);
  if (ret == MSGPACK_UNPACK_PARSE_ERROR) {
    LE("MSGPACK: Data in buffer had invalid format.\n");
  }
  return jval;
}

