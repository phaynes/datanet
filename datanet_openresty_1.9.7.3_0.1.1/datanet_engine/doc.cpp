#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>

extern "C" {
  #include <time.h>
  #include <sys/time.h>
  #include <sys/types.h>
  #include <sys/un.h>
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

#include "json/json.h"
#include "easylogging++.h"

#include "helper.h"
#include "shared.h"
#include "convert.h"
#include "doc.h"
#include "oplog.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZDOC.JS -------------------------------------------------------------------

vector<string> zdoc_parse_op_key(string path) {
  return split(path, '.');
}

string zdoc_write_key_check(string path) {
  string err; //NOTE: empty string
  vector<string> res = split(path, '.');
  string kpath = res[0];
  transform(kpath.begin(), kpath.end(), kpath.begin(), ::toupper);
  if (!kpath.compare("_ID") || !kpath.compare("_META") ||
      !kpath.compare("_CHANNELS")) {
    return ZS_Errors["ReservedFieldMod"];
  }
  return err;
}

string zdoc_s_arg_check(bool iso, bool isa, string key, string vtype,
                        bool adding) {
  string err; //NOTE: empty string
  if (isa && !zh_is_json_element_int(key)) {
    return ZS_Errors["SetArrayInvalid"];
  }
  if (!vtype.size()) {
    return ZS_Errors["SetValueInvalid"];
  }
  if ((!iso && !isa) && adding) {
    return ZS_Errors["SetPrimitiveInvalid"];
  }
  return err;
}

string zdoc_insert_arg_pre_check(UInt64 index, char *endptr) {
  string err; //NOTE: empty string
  if ((endptr && *endptr) || (index < 0)) {
    return ZS_Errors["InsertArgsInvalid"];
  }
  return err;
}

string zdoc_insert_post_arg_check(string ptype) {
  string err; //NOTE: empty string
  if (ptype.compare("A")) {
    return ZS_Errors["InsertType"];
  }
  return err;
}

