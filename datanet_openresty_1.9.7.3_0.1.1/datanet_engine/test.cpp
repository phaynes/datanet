
#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>
#include <iostream>
#include <vector>

extern "C" {
  #include <sys/types.h>
  #include <unistd.h>
}

#include "json/json.h"

#include "easylogging++.h"
INITIALIZE_EASYLOGGINGPP

#include "lz4_api.h"
#include "helper.h"
#include "storage.h"

using namespace std;

string get_pid_tid() {
  pid_t     pid   = getpid();
  pthread_t tid   = pthread_self();
  string    trole = "C";
  string    prole = "M";
  return "(P:" + to_string(pid) + "-T:" + to_string(tid) + ")(" +
          prole + "-" + trole + ")";
}

int main() {
  string s("I AM Aaaaaaaaaaaaaaaaaaaa SUUuuuuuuuuuuuuuuuupppppppppPPPPPPPPPPPPPPPPPeeeeeeeeeeeeeeeeEEEEEEEEEEEEEEEEEEEEEEEEEEEEeeeerrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrRRRRRRRRRRRRRRRRRRRRRRRRRRR L000000000000000000000ooooooooooooooo000000000000000000000000000000ooooooooooooooooooo00000000000ONG STRiiiiiiiiiiiiiiiiii111111111111111111iiiiiiiiiiiiiiiiiiiiiiING");
  LD("BEG: SIZE: " << s.size() << " S: (" << s << ")");
  byte_buffer c = lz4_compress(s.c_str(), s.size());
  LD("COMPRESSED: SIZE: " << c.size());
  byte_buffer d = lz4_decompress(&c[0], c.size());
  LD("UNCOMPRESSED: SIZE: " << d.size());
  string f((const char *)&d[0]);
  LD("END: SIZE: " << f.size() << " S: (" << f << ")");
  return 0;
}

// g++ -std=c++11 -g -Wall -I./ -I./lua/src/ -o test test.cpp lz4_api.o ./lua-lz4/liblz4.a
