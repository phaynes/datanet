
#include <iostream>
#include <string>

#include "json/json.h"
#include <msgpack.hpp>

typedef Json::Value jv;
typedef jv::UInt64  UInt64;
typedef jv::Int64   Int64;

#define JARRAY      Json::arrayValue
#define JOBJECT     Json::objectValue
#define JNULL       Json::nullValue
#define JNULLSTRING Json::Value("null")

typedef Json::Value jv;
typedef jv::UInt64  UInt64;
typedef jv::Int64   Int64;

#define JARRAY      Json::arrayValue
#define JOBJECT     Json::objectValue
#define JNULL       Json::nullValue
#define JNULLSTRING Json::Value("null")

using namespace std;

#define LD(text) cout << text << endl

#define mpacker msgpack::packer<msgpack::sbuffer>

static void mp_json_object_pack(jv *jdata, mpacker &pk);
static void mp_json_array_pack (jv *jdata, mpacker &pk);

static void mp_pack_element(mpacker &pk, string *k, jv *jval) {
  if (k) pk.pack(*k);
  if      (jval->isIntegral()) pk.pack(jval->asUInt64());
  else if (jval->isString())   pk.pack(jval->asString());
  else if (jval->isArray())    mp_json_array_pack(jval, pk);
  else         /* OBJECT */    mp_json_object_pack(jval, pk);
}

static void mp_json_array_pack(jv *jdata, mpacker &pk) {
  pk.pack_array(jdata->size());
  for (uint i = 0; i < jdata->size(); i++) {
    jv     *jval = &((*jdata)[i]);
    mp_pack_element(pk, NULL, jval);
  }
}

static void mp_json_object_pack(jv *jdata, mpacker &pk) {
  vector<string> mbrs = jdata->getMemberNames();
  pk.pack_map(mbrs.size());
  for (uint i = 0; i < mbrs.size(); i++) {
    string  k    = mbrs[i];
    jv     *jval = &((*jdata)[k]);
    mp_pack_element(pk, &k, jval);
  }
}


static void mp_packed_unpack_to_json(msgpack::object *mo, jv *jval) {
  int otype = mo->type;
LD(*mo);
  if        (otype == msgpack::type::MAP)   {
    int msize = mo->via.map.size;
    LD("MSGPACK_OBJECT_MAP: SIZE: " << msize);
    for (int i = 0; i < msize; i++) {
      msgpack::object mk   = mo->via.map.ptr->key;
      string k = string(mk.via.str.ptr, mk.via.str.size);
      LD("K: " << k);
      msgpack::object mval = mo->via.map.ptr->val;
      mp_packed_unpack_to_json(&mval, jval);
    }
  } else if (otype == msgpack::type::ARRAY) {
    int asize = mo->via.array.size;
    LD("MSGPACK_OBJECT_ARRAY");
  } else if (otype == msgpack::type::STR)   {
    LD("MSGPACK_OBJECT_STR");
  } else {
    LD("NUMBER");
  }
}

int main(void) {
  msgpack::sbuffer buffer;
  mpacker          pk(&buffer);

  jv jval = JOBJECT;
  jval["n"] = 2;
  jval["s"] = "YOYOYOYO";
  jval["o"] = JOBJECT;
  jval["o"]["x"] = 1;
  jval["o"]["y"] = "OBJECT";
  jval["a"] = JARRAY;
  jval["a"].append(22);
  jval["a"].append("ARRAY");
  jval["NEST"] = JOBJECT;
  jval["NEST"]["n"] = 1;
  jval["NEST"]["s"] = "BEST";
  jval["NEST"]["o"] = JOBJECT;
  jval["NEST"]["o"]["n"] = 1;
  jval["NEST"]["o"]["a"] = JARRAY;
  jval["NEST"]["o"]["a"].append(25);
  jv jo  = JOBJECT;
  jo["DEEP"] = "CHECK";
  jo["i"]    = 1;
  jval["NEST"]["o"]["a"].append(jo);

  mp_json_object_pack(&jval, pk);


  jv juval;
  msgpack::object_handle mh = msgpack::unpack(buffer.data(), buffer.size());
  msgpack::object mo        = mh.get();
  mp_packed_unpack_to_json(&mo, &juval);

  return 0;
}

