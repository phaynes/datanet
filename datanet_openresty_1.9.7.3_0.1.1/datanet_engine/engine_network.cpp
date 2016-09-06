
#include <errno.h>
extern int errno;
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "json/json.h"
#include "easylogging++.h"

#include "storage.h"
#include "helper.h"
#include "engine.h"
#include "deprecated_engine.h"
#include "engine_network.h"

extern string MyDataCenterUUID;
extern jv     CentralMaster;
extern Int64  MyWorkerUUID;

extern map<string, int> RemoteMethodCallIDtoFDMap;

extern jv     zh_nobody_auth;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEFINTIONS ----------------------------------------------------------------

typedef struct sockaddr_in sa_in;

typedef map<int, Int64> SocketWorkerMap;
typedef map<Int64, int> WorkerSocketMap;

#define MAX(a, b) (a > b) ? a : b
#define MIN(a, b) (a < b) ? a : b

#define LISTEN_QUEUE_DEPTH      10
#define BUFSIZE                 16384

#define MAX_SEND_SIZE 16384
//#define MAX_SEND_SIZE 64 /* for buffer debugging */

#define PRIMARY_UNAVAILABLE_SLEEP 1

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SHARED GLOBALS ------------------------------------------------------------

pthread_t PrimaryTid   = 0;
pthread_t ReconnectTid = 0;
pthread_t WorkerTid    = 0;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOCAL GLOBALS -------------------------------------------------------------

unsigned int WorkerPort        = 0;
unsigned int PrimaryServerPort = 0;
unsigned int PrimaryClientPort = 0;

fd_set PrimaryMasterReadFds;
fd_set PrimaryMasterWriteFds;
int    PrimaryMaxFD    = 0;
fd_set WorkerMasterReadFds;
fd_set WorkerMasterWriteFds;
int    WorkerMaxFD     = 0;

int    PrimaryServerFD       = -1;
int    WorkerServerFD        = -1;

int    SelfPrimaryClientFD   = -1;
int    SelfWorkerClientFD    = -1;
int    WorkerPrimaryClientFD = -1;
int    CentralClientFD       = -1;

map<string, int> CRmap;

SocketWorkerMap SWmap;
WorkerSocketMap WSmap;

bool DebugNetworkThreadSendBytes = true;

map<int, string> FDOutputBuffer;

#define ReconnBusySleepUsecs 1000 /* 1msec */
bool               ReconnLoopBusy           = true; // START BUSY
mutex              ReconnThreadMutex;
condition_variable ReconnThreadCV;
bool               DoSelfPrimaryReconnect   = false;
bool               DoSelfWorkerReconnect    = false;
bool               DoWorkerPrimaryReconnect = false;
bool               DoCentralReconnect       = false;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLASS DEFINITIONS ---------------------------------------------------------

class IOBuffer {
  public: 
    IOBuffer() {
      init();
    }

    IOBuffer(int fd) {
      init();
      _fd = fd;
    }

    IOBuffer(const IOBuffer &iobuf) {
      _fd       = iobuf._fd;
      _need     = iobuf._need;
      _size     = iobuf._size;
      _contents = iobuf._contents;
    }

    void concat(const char *c, int clen) {
      unsigned int nsize = _size + clen;
      if (nsize > _contents.capacity()) {
        _contents.reserve(nsize);
      }
      _copy_external_data(c, clen);
    }

    string process_socket_read() {
      string smt;
      if (_size == 0) return smt;
      if (_need == 0) {
        unsigned int clen;
        char *data = (char *)_contents.c_str();
        memcpy(&clen, data, sizeof(unsigned int));
        _need      = clen;
        _size     -= sizeof(unsigned int);
        char *src  = data + sizeof(unsigned int);
        _move_data(src, _size);
        //LD("process_socket_read: GOT NEED: " << _debug());
      }
      if (_size < _need) return smt;
      else {
        string subb(_contents.c_str(), _need);
        unsigned int rlen = _size - _need;
        char *data        = (char *)_contents.c_str();
        char *rest        = data + _need;
        reinit(rest, rlen);
        return subb;
      }
    }

    string get_buffer() {
      char   *data = (char *)_contents.c_str();
      string  buf(data, _size);
      return buf;
    }

    void clear() {
      _size = 0;
      _need = 0;
      _contents.clear();
    }

  private:
    void init() {
      _fd = 0;
      clear();
    }

    void reinit(const char *c, int clen) {
      clear();
      _move_data(c, clen);
    }

    void _copy_external_data(const char *c, unsigned int clen) {
      if (!clen) return;
      char *data  = (char *)_contents.c_str();
      data       += _size;
      memcpy(data, c, clen);
      _size      += clen;
    }

    void _move_data(const char *c, unsigned int clen) {
      char *data = (char *)_contents.c_str();
      string s(c, clen); // Make a copy
      memcpy(data, s.c_str(), s.size());
      _size      = clen;
    }

    string _debug() {
      if (!_need) {
        return "FD: " + to_string(_fd) + " NEED: 0 SIZE: " + to_string(_size);
      } else {
        string contents(_contents.c_str(), _size);
        return "FD: " + to_string(_fd) + " NEED: " + to_string(_need) +
               " SIZE: " + to_string(_size) + " CONTENTS: " + contents;
      }
    }

    unsigned int _fd;
    unsigned int _need;
    unsigned int _size;
    string       _contents;
};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WORKER UUID PARSER --------------------------------------------------------

//TODO DEPRECATED (EXCEPT CRmap population)
static bool parse_worker_info_method(int fd, string &wibody) {
  jv jreq = zh_parse_json_text(wibody, true);
  if (jreq == JNULL) {
    LE("ERROR: parse_worker_info_method: PARSE: " << wibody);
    return false;
  } else {
    string method = jreq["method"].asString();
    LT("parse_worker_info_method: method: " << method);
    if (!method.compare("WorkerInfo")) {
      Int64 wid = jreq["worker_id"].asInt64();
      LD("parse_worker_info_method: [FD: " << fd << ", WID: " << wid << "]");
      SWmap.insert(make_pair(fd, wid));
      WSmap.insert(make_pair(wid, fd));
      engine_send_worker_settings(fd);
      return true;
    } else { // AgentOnline from ReconnectThread
      string id = jreq["id"].asString();
      LT("INSERT DIRECT ID: " << id << " FD: " << fd);
      CRmap.insert(make_pair(id, fd)); // Used in process_central_response()
      return false;
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NETWORK HELPERS -----------------------------------------------------------

static int set_socket_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return false;
  flags     = (flags | O_NONBLOCK);
  return fcntl(fd, F_SETFL, flags);
}

static void read_fds_setter(fd_set *master, int nfd) {
  bool   isp = (master == &PrimaryMasterReadFds);
  string who = isp ? "PrimaryMasterReadFds" : "WorkerMasterReadFds";
  FD_SET(nfd, master);                     // add to master set
  if (isp) {
    PrimaryMaxFD = MAX(nfd, PrimaryMaxFD); // compute new MaxFD
    LD(who + " read_fds_setter: FD: " << nfd <<
       " PrimaryMaxFD: " << PrimaryMaxFD);
  } else {
    WorkerMaxFD  = MAX(nfd, WorkerMaxFD);  // compute new MaxFD
    LD(who + " read_fds_setter: FD: " << nfd <<
       " WorkerMaxFD: " << WorkerMaxFD);
  }
}

static void write_fds_setter(int nfd) {
  bool    isp    = zh_is_primary_thread();
  fd_set *master = isp ? &PrimaryMasterWriteFds  : &WorkerMasterWriteFds;
  string who     = isp ? "PrimaryMasterWriteFds" : "WorkerMasterWriteFds";
  LD(who + " write_fds_setter: FD: " << nfd);
  FD_SET(nfd, master);                     // add to master set
  if (isp) {
    PrimaryMaxFD = MAX(nfd, PrimaryMaxFD); // compute new MaxFD
  } else {
    WorkerMaxFD  = MAX(nfd, WorkerMaxFD);  // compute new MaxFD
  }
}

static void fd_set_remover(fd_set *master, int ofd) {
  bool   isp = (master == &PrimaryMasterReadFds);
  string who = isp ? "PrimaryMasterReadFds" : "WorkerMasterReadFds";
  LD(who + " fd_set_remover: FD: " << ofd);
  int ret = close(ofd); // close our half of socket
  if (ret == -1) LE("CLOSE() ERROR: " << strerror(errno));
  FD_CLR(ofd, master);  // remove from master set
}

static int accept_connection(int sockfd) {
  struct sockaddr_storage raddr;
  socklen_t               addrlen = sizeof(raddr);
  int nfd = accept(sockfd, (struct sockaddr *)&raddr, &addrlen);
  if (nfd != -1) return nfd;
  else {
    LE("accept failed: " << strerror(errno));
    return -1;
  }
}

static void remove_primary_accepted_fd(int sockfd) {
  SocketWorkerMap::iterator it = SWmap.find(sockfd);
  if (it != SWmap.end()) {
    Int64 wid = it->second;
    LD("remove_primary_accepted_fd: FD: " << sockfd << " WID: " << wid);
    SWmap.erase(sockfd);
    WSmap.erase(wid);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INTERNAL CLIENT & SERVER --------------------------------------------------

static int close_socket(int &fd) {
  if (fd == -1) return 0;
  else {
    LD("close_socket: CLOSE EXISTING FD");
    int cfd  = fd;
    fd       = -1;
    int ret  = close(cfd);
    if (ret == -1) LE("CLOSE() ERROR: " << strerror(errno));
    return ret;
  }
}

static bool initialize_network_client_socket(sa_in        *saddr,
                                             const char   *hostname,
                                             unsigned int  port) {
  struct hostent *srvr = gethostbyname(hostname);
  if (srvr == NULL) {
    LE("ERROR, no such host: " << hostname);
    return false;
  } else {
    bzero((char *)saddr, sizeof(saddr));
    saddr->sin_family = AF_INET;
    saddr->sin_port   = htons(port);
    bcopy((char *)srvr->h_addr,
          (char *)&saddr->sin_addr.s_addr,
          srvr->h_length);
    return true;
  }
}

static int initialize_network_client(string &chost, int cport) {
  sa_in saddr;
  if (!initialize_network_client_socket(&saddr, chost.c_str(), cport)) {
    LE("initialize_network_client: SOCKET INITIALIZATION ERROR");
    return -1;
  } else {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(fd, (struct sockaddr *)&saddr, sizeof((saddr))) < 0)  {
      LE("initialize_network_client: CONNECTION ERROR");
      return -1;
    } else {
      set_socket_nonblocking(fd);
      return fd;
    }
  }
}

static int initialize_server(int port) {
  LD("initialize_server: PORT: " << port);
  int yes = 1;
  int listener;
  int rv;
  struct addrinfo hints, *ai, *p; 

  memset(&hints, 0, sizeof(hints));
  hints.ai_family   = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags    = AI_PASSIVE;
  char buf[16];
  sprintf(buf, "%u", port);
  if ((rv = getaddrinfo(NULL, buf, &hints, &ai)) != 0) {
    LE("initialize_server: " << gai_strerror(rv));
    return -1;
  }

  for(p = ai; p != NULL; p = p->ai_next) {
    listener = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (listener < 0) continue;
    // lose the pesky "address already in use" error message
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
    if (bind(listener, p->ai_addr, p->ai_addrlen) < 0) {
      int ret = close(listener);
      if (ret == -1) LE("CLOSE() ERROR: " << strerror(errno));
      continue;
    }
    break;
  }
  freeaddrinfo(ai);
  if (p == NULL) { // if we got here, it means we didn't get bound
    LE("initialize_server: failed to bind: " << strerror(errno));
    return -1;
  }
  if (listen(listener, LISTEN_QUEUE_DEPTH) == -1) { // listen
    LE("initialize_server: listen FAILED: " << strerror(errno));
    return -1;
  }
  return listener;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG NETWORK BYTES -------------------------------------------------------

static void debug_send(int fd, const void *rp, int rlen) {
  char         *zbuf = (char *)rp + sizeof(unsigned int);
  unsigned int  slen = rlen - sizeof(unsigned int);
  zbuf[slen]         = '\0';
  LD("nb_send: SEND: FD: " << fd << " LEN: " << rlen <<
     " BODY: (" << zbuf << ")");
}

static void debug_partial_send(int fd, const char *rp, int rlen) {
  LD("nb_send: SEND: FD: " << fd << " LEN: " << rlen <<
     " BODY: (" << rp << ")");
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SEND NETWORK BYTES --------------------------------------------------------

//TODO TEST MORE
static int handle_send_error_egain(int fd, char *sbuf, int slen) {
  LD("WRITE: EWOULDBLOCK: FD: " << fd);
  string rest(sbuf, slen);
  FDOutputBuffer.insert(make_pair(fd, rest));
  write_fds_setter(fd);
  return -1; 
}

static bool is_fd_blocked(int fd) {
  map<int, string>::iterator it = FDOutputBuffer.find(fd);
  return (it != FDOutputBuffer.end());
}

static int nb_send(int fd, const void *rp, int rlen, bool full) {
  if (fd == -1) return -1;
  if (full && DebugNetworkThreadSendBytes) debug_send(fd, rp, rlen);
  if (is_fd_blocked(fd)) {
    LE("BLOCKED: FD: " << fd);
    return -1;
  }
  char *sbuf = (char *)rp;
  int   slen = rlen;
  while (slen) {
    int wlen = MIN(slen, MAX_SEND_SIZE);
    int ret  = send(fd, sbuf, wlen, 0);
    //LD("send: ret: " << ret);
    if (ret == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return handle_send_error_egain(fd, sbuf, slen);
      } else {
        LE("SEND() ERROR: " << strerror(errno));
        return -1;
      }
    }
    slen -= ret;
    sbuf += ret;
  }
  return rlen;
}

static int drain_unblocked_writer(int fd) {
  LD("drain_unblocked_writer: FD: " << fd);
  string rest = FDOutputBuffer[fd];
  FDOutputBuffer.erase(fd);
  return nb_send(fd, rest.c_str(), rest.size(), true);
}

int network_send_binary_fd(int fd, const char *rp, int rlen) {
  LD("network_send_binary_fd: FD: " << fd);
  unsigned int len = (unsigned int)rlen;
  if (DebugNetworkThreadSendBytes) debug_partial_send(fd, rp, rlen);
  if (nb_send(fd, &len, sizeof(unsigned int), false) == -1) return -1;
  //sleep(1); // NOTE: for debugging IOBuffer
  return nb_send(fd, rp, rlen, false);
}

int network_send_central(string &r) {
  if (CentralClientFD == -1) return network_reconnect_central_client(false);
  else {
    int fd = CentralClientFD;
    LD("network_send_central: FD: " << fd);
    return network_send_binary_fd(fd, r.c_str(), r.size());
  }
}

int network_send_primary(const char *rp, int rlen) {
  int fd = zh_is_worker_thread() ? WorkerPrimaryClientFD :
                                   SelfPrimaryClientFD;
  LD("network_send_primary: FD: " << fd);
  return network_send_binary_fd(fd, rp, rlen);
}

// NOTE: responding to LUA: DatanetNetwork:NetworkRequest
int network_respond_remote_call(int fd, const char *rp, int rlen) {
  LD("network_respond_remote_call: FD: " << fd);
  return network_send_binary_fd(fd, rp, rlen);
}

// TODO: called from process_central_response() plus
//       used for InternalPurgeGCVSummaries/DirtyDeltaDrain
//       Receiving worker expects fully formed JSON-RPC -> need a queue
int network_send_internal(Int64 wid, string &r) {
  WorkerSocketMap::iterator it = WSmap.find(wid);
  if (it == WSmap.end()) {
    LE("network_send_internal: WORKER-ID NOT FOUND: "<< wid);
    return -1;
  } else {
    int wfd = it->second;
    LD("network_send_internal: WID: " << wid << " WFD: " << wfd);
    return network_send_binary_fd(wfd, r.c_str(), r.size());
  }
}

int network_broadcast_method(const char *rp, int rlen) {
  LD("network_broadcast_method #W: " << WSmap.size());
  for(WorkerSocketMap::iterator it = WSmap.begin(); it != WSmap.end(); it++) {
    unsigned int wfd = it->second;
    int          ret = network_send_binary_fd(wfd, rp, rlen);
    if (ret == -1) {
      LD("ERROR: AgentSettings: SEND: WFD: " << wfd);
    }
  }
  return 1;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PROCESS RESPONSES ---------------------------------------------------------

static int check_worker_central_race(Int64 wid) {
  if (wid != MyWorkerUUID) return 0;
  else {
    LE("RACE-CONDITION (AgentOnline vs WorkerInfo) -> DoCentralReconnect");
    return network_reconnect_central_client(false);
  }
}

static int process_central_response(string &pdata) {
  LD("process_central_response: RECIEVED: " << pdata);
  jv jreq = zh_parse_json_text(pdata.c_str(), false);
  if (jreq == JNULL) {
    LE("CENTRAL-NETWORK-RESPONSE: PARSE ERROR");
    return -1;
  } else {
    if (zh_jv_is_member(&jreq, "method")) { // FROM CENTRAL METHOD
      // PROCESS ON PRIMARY
      if (!zh_jv_is_member(&(jreq["params"]["data"]), "ks")) {
        engine_process_server_method(&jreq);
        return 0;
      } else {                                      // FORWARD TO KQK-WORKER
        string  method = jreq["method"].asString();
        jv     *jks    = &(jreq["params"]["data"]["ks"]);
        string  kqk    = (*jks)["kqk"].asString();
        Int64   wid    = engine_get_worker_uuid_for_operation(kqk);
        LD("process_central_response: METHOD: " << method << " WID: " << wid);
        return network_send_internal(wid, pdata);
      }
    } else {                       // FROM CENTRAL ACK
      string                     id = jreq["id"].asString();
      map<string, int>::iterator it = RemoteMethodCallIDtoFDMap.find(id);
      if (it != RemoteMethodCallIDtoFDMap.end()) { // PROCESS ON PRIMARY
        engine_process_central_ack(&jreq);
        return 0;
      } else {                                     // FORWARD TO KQK-WORKER
        Int64 wid = jreq["worker_id"].asInt64();
        LD("process_central_response: ACK: WID: " << wid);
        if (wid) {
          int ret = network_send_internal(wid, pdata);
          if (ret != -1) return ret;
          else           return check_worker_central_race(wid);
        } else { // DIRECT FD REPONSE (OPENRESY-IO)
          map<string, int>::iterator it = CRmap.find(id);
          int                        fd = (it == CRmap.end()) ? -1 : it->second;
          LD("DIRECT FD RESPONSE: FD: " << fd);
          if (it == CRmap.end()) return -1;
          else {
            CRmap.erase(id);
            return network_send_binary_fd(fd, pdata.c_str(), pdata.size());
          }
        }
      }
    }
  }
}

static void process_primary_response(string &pdata) {
  LD("process_primary_response: RECIEVED: " << pdata);
  jv jreq = zh_parse_json_text(pdata.c_str(), false);
  if (jreq == JNULL) {
    LE("PRIMARY-NETWORK-RESPONSE: PARSE ERROR");
  } else {
    if (zh_jv_is_member(&jreq, "method")) { // FROM PRIMARY METHOD
      engine_process_primary_method(&jreq);
    } else {                       // FROM CENTRAL (VIA PRIMARY) ACK
      engine_process_primary_ack(&jreq);
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EVENT LOOP HELPERS --------------------------------------------------------

static bool initialize_worker_server() {
  LD("initialize_worker_server: WorkerPort: " << WorkerPort);
  if (WorkerServerFD != -1) return true;
  else {
    FD_ZERO(&WorkerMasterReadFds);
    int wsfd = initialize_server(WorkerPort);
    if (wsfd == -1) {
      LE("ERROR: Worker Server Initiliazation FAILED");
      return false;
    }
    read_fds_setter(&WorkerMasterReadFds, wsfd);
    WorkerServerFD = wsfd;
    return true;
  }
}

static bool initialize_primary_server() {
  LD("initialize_primary_server: PrimaryServerPort: " << PrimaryServerPort);
  if (PrimaryServerFD != -1) return true;
  else {
    FD_ZERO(&PrimaryMasterReadFds);
    int psfd = initialize_server(PrimaryServerPort);
    if (psfd == -1) {
      LE("ERROR: Primary Server Initiliazation FAILED");
      return false;
    }
    read_fds_setter(&PrimaryMasterReadFds, psfd);
    PrimaryServerFD = psfd;
    return true;
  }
}

static int initialize_worker_client() {
  LD("initialize_worker_client: WorkerPort: " << WorkerPort);
  string phost = "127.0.0.1";
  return initialize_network_client(phost, WorkerPort);
}

static int initialize_primary_client(bool isp, bool add) {
  LD("initialize_primary_client: PrimaryClientPort: " << PrimaryClientPort);
  string phost = "127.0.0.1";
  int    pfd   = initialize_network_client(phost, PrimaryClientPort);
  if (pfd == -1) {
    LE("ERROR: PRIMARY-UNAVAILABLE: SLEEP: " << PRIMARY_UNAVAILABLE_SLEEP);
    sleep(PRIMARY_UNAVAILABLE_SLEEP);
    return -1;
  } else {
    if (add) {
      fd_set *master = isp ? &PrimaryMasterReadFds : &WorkerMasterReadFds;
      read_fds_setter(master, pfd);
    }
    return pfd;
  }
}

static int initialize_central_client() {
  string chost = CentralMaster["socket"]["server"]["hostname"].asString();
  UInt64 cport = CentralMaster["socket"]["server"]["port"].asUInt64();
  LD("initialize_central_client: HOST: " << chost << " PORT: " << cport);
  int    cfd   = initialize_network_client(chost, cport);
  if (cfd == -1) {
    (void)engine_handle_broken_central_connection_event();
    return -1;
  } else {
    read_fds_setter(&PrimaryMasterReadFds, cfd);
    return cfd;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SIGNAL RECONNECT THREAD ---------------------------------------------------

static void signal_reconnect_thread(bool set_central, bool set_worker_primary) {
  while (ReconnLoopBusy) {
    LE("RECONNECT THREAD BUSY: USLEEP: " << ReconnBusySleepUsecs);
    usleep(ReconnBusySleepUsecs);
  }
  std::unique_lock<std::mutex> lck(ReconnThreadMutex);
  if (set_central) {
    LD("network_reconnect_central_client: NOTIFY CONDITION VARIABLE ->");
    DoCentralReconnect  = true;
  } else if (set_worker_primary) {
    LD("reconnect_worker_primary: NOTIFY CONDITION VARIABLE ->");
    DoWorkerPrimaryReconnect = true;
  } else {
    LD("restart_reconnect_thread: NOTIFY CONDITION VARIABLE ->");
  }
  ReconnThreadCV.notify_all();
}

int network_reconnect_central_client(bool start) {
  int cfd = CentralClientFD;
  if (cfd != -1) {
    CentralClientFD = -1;
    fd_set_remover(&PrimaryMasterReadFds, cfd);
  }
  if (!start) {
    (void)engine_handle_broken_central_connection_event();
  }
  signal_reconnect_thread(true, false);
  return 0;
}

static void primary_handle_broken_central_connection() {
  LT("primary_handle_broken_central_connection");
  network_reconnect_central_client(false);
}

static void reconnect_worker_primary() {
  int pfd               = WorkerPrimaryClientFD;
  WorkerPrimaryClientFD = -1;
  fd_set_remover(&WorkerMasterReadFds, pfd);
  signal_reconnect_thread(false, true);
}

static void worker_handle_broken_primary_connection() {
  LT("worker_handle_broken_primary_connection");
  reconnect_worker_primary();
}

static void restart_reconnect_thread() {
  signal_reconnect_thread(false, false);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RECONNECT THREAD ----------------------------------------------------------

static void do_central_reconnect() {
  int cfd = initialize_central_client();
  if (cfd != -1) {
    CentralClientFD    = cfd;
    LD("CentralClientFD: CONNECTED: FD: " << cfd);
    DoCentralReconnect = false;
    int ret = engine_handle_reconnect_to_central_event(SelfPrimaryClientFD);
    if (ret == -1) {
      LE("FAIL: engine_handle_reconnect_to_central_event");
      close_socket(SelfPrimaryClientFD);
      DoSelfPrimaryReconnect = true;
    }
  }
}

// WORKER-PRIMARY-CONNECT STEP TWO
jv network_finalize_worker_primary_connection() {
  LT("network_finalize_worker_primary_connection: WorkerPrimaryClientFD: " <<
     WorkerPrimaryClientFD);
  jv prequest = zh_create_json_rpc_body("WorkerInfo", &zh_nobody_auth);
  int wfd     = WorkerPrimaryClientFD;
  int ret     = engine_send_binary_fd(wfd, &prequest);
  if (ret == -1) {
    LE("network_finalize_worker_primary_connection FAIL");
    reconnect_worker_primary();
  }
  return prequest; // NOTE: returns a REQUEST-> WAIT ON CENTRAL
}

// WORKER-PRIMARY-CONNECT STEP ONE
static void do_worker_primary_reconnect() {
  int pfd = initialize_primary_client(false, true);// Connect to PrimaryServer
  if (pfd != -1) {
    // WakeupWorker -> WorkerPrimaryClientFD added to WorkerMasterReadFds
    LD("WorkerPrimaryClientFD: CONNECTED: FD: " << pfd);
    WorkerPrimaryClientFD = pfd; // used in WorkerThread (Step 2)
    int eret = engine_send_wakeup_worker(SelfWorkerClientFD);
    if (eret == -1) {
      WorkerPrimaryClientFD = -1;
      LE("ERROR: engine_send_wakeup_worker");
      fd_set_remover(&WorkerMasterReadFds, pfd);
    } else {
      LD("DoWorkerPrimaryReconnect -> FALSE");
      DoWorkerPrimaryReconnect = false;
    }
  }
}

static void do_worker_self_reconnect() {
  int wfd = initialize_worker_client();
  if (wfd != -1) {
    SelfWorkerClientFD = wfd;
    LD("SelfWorkerClientFD: CONNECTED: FD: " << wfd);
    DoSelfWorkerReconnect = false;
  }
}

static void do_self_primary_reconnect() {
  int pfd = initialize_primary_client(true, false);
  if (pfd != 1) {
    SelfPrimaryClientFD    = pfd;
    LD("SelfPrimaryClientFD: CONNECTED: FD: " << pfd);
    DoSelfPrimaryReconnect = false;
  }
}

static void reconn_loop() {
  ReconnLoopBusy = true;
  while (true) { // MAIN LOOP
    LD("reconn_loop: DoSelfPrimaryReconnect: "   << DoSelfPrimaryReconnect   <<
                   " DoSelfWorkerReconnect: "    << DoSelfWorkerReconnect    <<
                   " DoWorkerPrimaryReconnect: " << DoWorkerPrimaryReconnect <<
                   " DoCentralReconnect: "       << DoCentralReconnect);
    // NOTE: The Order of evaluation is IMPORTANT -> DONT CHANGE
    if (DoSelfPrimaryReconnect) {
      do_self_primary_reconnect();
    } else if (DoSelfWorkerReconnect) {
      do_worker_self_reconnect();
    } else if (DoWorkerPrimaryReconnect) {
      do_worker_primary_reconnect();
    } else if (DoCentralReconnect) {
      do_central_reconnect();
    } else {
      std::unique_lock<std::mutex> lck(ReconnThreadMutex);
      LD("RECONNECT-THREAD: CONDITION VARIABLE WAIT");
      ReconnLoopBusy = false;
      ReconnThreadCV.wait(lck);
      ReconnLoopBusy = true;
      LD("->RECONNECT-THREAD: CONDITION VARIABLE NOTIFIED -> GO");
    }
  }
}

static void run_reconnect_thread() {
  ReconnectTid = pthread_self();
  LT("run_reconnect_thread: ReconnectTid: " << ReconnectTid);
  reconn_loop();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PRIMARY EVENT LOOP --------------------------------------------------------

char PrimaryBuffer[BUFSIZE];

static void run_primary_network_thread() {
  PrimaryTid = pthread_self();
  LT("run_primary_network_thread: PrimaryTid: " << PrimaryTid);

  FD_ZERO(&PrimaryMasterWriteFds);

  IOBuffer           ciobuf(0);
  map<int, IOBuffer> widiobuf;
  while (true) { // MAIN LOOP
    fd_set rfds = PrimaryMasterReadFds; // re-init from Master
    LD("PRIMARY-EVENT-LOOP: SELECT(): PrimaryMaxFD: " << PrimaryMaxFD);
    int r = select(PrimaryMaxFD + 1, &rfds, &PrimaryMasterWriteFds, NULL, NULL);
    if (r == -1) {
      LE("SELECT() ERROR: " << strerror(errno));
      return;
    }

    for (int i = 0; i <= PrimaryMaxFD; i++) {
      if (FD_ISSET(i, &rfds)) {
        if (i == CentralClientFD) { // INCOMING PROXY (FROM CENTRAL) RESPONSE
          LD("PRIMARY-EVENT-LOOP: CENTRAL_CLIENT: FD: " << i);
          int nbytes = recv(i, PrimaryBuffer, (BUFSIZE - 1), 0);
          LD("nbytes: " << nbytes);
          if (nbytes <= 0) {
            primary_handle_broken_central_connection();
          } else {
            ciobuf.concat(PrimaryBuffer, nbytes);
            while (true) {
              string pdata = ciobuf.process_socket_read();
              if (!pdata.size()) break;
              else {
                process_central_response(pdata);
              }
            }
          }
        } else if (i == PrimaryServerFD) {
          LD("PRIMARY-EVENT-LOOP: PRIMARY-SERVER ACCEPT(): FD: " << i);
          int nfd = accept_connection(i);
          if (nfd != -1) {
            read_fds_setter(&PrimaryMasterReadFds, nfd);
            IOBuffer wiobuf(nfd);
            widiobuf.insert(make_pair(nfd, wiobuf));
          }
        } else { // INCOMING FROM WORKER
          LD("PRIMARY-EVENT-LOOP: PROXY-TO-CENTRAL: FD: " << i);
          int nbytes = recv(i, PrimaryBuffer, (BUFSIZE - 1), 0);
          LD("nbytes: " << nbytes);
          if (nbytes <= 0) {
            remove_primary_accepted_fd(i);
            widiobuf.erase(i);
            fd_set_remover(&PrimaryMasterReadFds, i);
          } else {
            widiobuf[i].concat(PrimaryBuffer, nbytes);
            SocketWorkerMap::iterator it     = SWmap.find(i);
            bool                      cherry = (it == SWmap.end());
            while (true) {
              string pdata = widiobuf[i].process_socket_read();
              if (!pdata.size()) break;
              else {
                bool send = true;
                if (cherry) { // PARSE WORKER UUID
                  send = !parse_worker_info_method(i, pdata);
                }
                if (send) network_send_central(pdata);
              }
            }
          }
        }
      } else if (FD_ISSET(i, &PrimaryMasterWriteFds)) {
        drain_unblocked_writer(i);
      }
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WORKER EVENT LOOP ---------------------------------------------------------

char WorkerBuffer[BUFSIZE];

static void run_worker_network_thread() {
  WorkerTid = pthread_self();
  LT("run_worker_network_thread: WorkerTid: " << WorkerTid);

  FD_ZERO(&WorkerMasterWriteFds);
  IOBuffer           niobuf(0);
  map<int, IOBuffer> rmc_iobuf_map;

  while (true) { // MAIN LOOP
    fd_set rfds = WorkerMasterReadFds; // re-init from Master
    LD("WORKER-EVENT-LOOP: SELECT: WorkerMaxFD: " << WorkerMaxFD);
    int r = select(WorkerMaxFD + 1, &rfds, &WorkerMasterWriteFds, NULL, NULL);
    if (r == -1) {
      LE("SELECT() ERROR: " << strerror(errno));
      return;
    }

    for (int i = 0; i <= WorkerMaxFD; i++) {
      if (FD_ISSET(i, &rfds)) {
        if (i == WorkerPrimaryClientFD) { // SENT FROM PRIMARY SERVER
          LD("WORKER-EVENT-LOOP: PRIMARY-CLIENT: FD: " << i);
          int nbytes = recv(i, WorkerBuffer, (BUFSIZE - 1), 0);
          LD("nbytes: " << nbytes);
          if (nbytes <= 0) {
            worker_handle_broken_primary_connection();
          } else {
            niobuf.concat(WorkerBuffer, nbytes);
            while (true) {
              string pdata = niobuf.process_socket_read();
              if (!pdata.size()) break;
              else {
                process_primary_response(pdata);
              }
            }
          }
        } else if (i == WorkerServerFD) { // INITIAL INCOMING ON WORKER SERVER
          LD("WORKER-EVENT-LOOP: WORKER-SERVER ACCEPT(): FD: " << i);
          int nfd = accept_connection(i);
          if (nfd != -1) {
            read_fds_setter(&WorkerMasterReadFds, nfd);
            IOBuffer riobuf(nfd);
            rmc_iobuf_map.insert(make_pair(nfd, riobuf));
          }
        } else {                     // INCOMING ON WORKER SERVER
          LD("WORKER-EVENT-LOOP: REMOTE-METHOD-CALL: FD: " << i);
          int nbytes = recv(i, WorkerBuffer, (BUFSIZE - 1), 0);
          LD("nbytes: " << nbytes);
          if (nbytes <= 0) {
            rmc_iobuf_map.erase(i);
            fd_set_remover(&WorkerMasterReadFds, i);
          } else {
            rmc_iobuf_map[i].concat(WorkerBuffer, nbytes);
            while (true) {
              string pdata = rmc_iobuf_map[i].process_socket_read();
              if (!pdata.size()) break;
              else {
                engine_process_remote_method_call(i, pdata);
              }
            }
          }
        }
      } else if (FD_ISSET(i, &WorkerMasterWriteFds)) {
        drain_unblocked_writer(i);
      }
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZATION ------------------------------------------------------------

static void assign_central_master(const char *chost, unsigned int cport) {
  CentralMaster["device_uuid"]                  = MyDataCenterUUID;
  CentralMaster["socket"]["server"]["hostname"] = chost;
  CentralMaster["socket"]["server"]["port"]     = cport;
  CentralMaster["socket"]["load_balancer"] = CentralMaster["socket"]["server"];
}

bool WorkerThreadStarted    = false;
bool ReconnectThreadStarted = false;

int network_worker_initialize(unsigned int iport, unsigned int wport) {
  PrimaryClientPort = iport;
  WorkerPort        = wport;
  LD("network_worker_initialize:" <<
     " PrimaryClientPort: " << PrimaryClientPort);

  if (!initialize_worker_server()) return -1;
  std::thread worker_network_thread(run_worker_network_thread);
  worker_network_thread.detach();
  WorkerThreadStarted = true;

  DoSelfPrimaryReconnect   = false;
  DoSelfWorkerReconnect    = true;
  DoWorkerPrimaryReconnect = true;
  DoCentralReconnect       = false;
  std::thread reconnect_thread(run_reconnect_thread);
  reconnect_thread.detach();
  ReconnectThreadStarted = true;
  return 0;
}

// NOTE: network_primary_initialize() called AFTER network_worker_initialize()
//       in case NewPrimaryElected
int network_primary_initialize(const char   *chost, unsigned int cport,
                               unsigned int  iport, unsigned int wport) {
  assign_central_master(chost, cport);
  PrimaryServerPort = iport;
  PrimaryClientPort = iport;
  WorkerPort        = wport;
  LD("network_primary_initialize:" <<
     " CentralHostname: "          << chost << " CentralPort: " << cport <<
     " PrimaryServerPort: "        << PrimaryServerPort);

  if (!initialize_primary_server()) return -1;
  std::thread primary_network_thread(run_primary_network_thread);
  primary_network_thread.detach();

  if (WorkerThreadStarted) { // CASE: NewPrimaryElected
    LD("WORKER-THREAD ALREADY STARTED -> NO-OP");
  } else {
    if (!initialize_worker_server()) return -1;
    std::thread worker_network_thread(run_worker_network_thread);
    worker_network_thread.detach();
  }

  DoSelfPrimaryReconnect   = true;
  DoSelfWorkerReconnect    = true;
  DoWorkerPrimaryReconnect = true;
  DoCentralReconnect       = true;
  if (ReconnectThreadStarted) { // CASE: NewPrimaryElected
    LD("RECONNECT-THREAD ALREADY STARTED");
    restart_reconnect_thread();
  } else {
    std::thread reconnect_thread(run_reconnect_thread);
    reconnect_thread.detach();
  }
  return 0;
}

int network_central_initialize() {
  string chost = CentralMaster["socket"]["server"]["hostname"].asString();
  UInt64 cport = CentralMaster["socket"]["server"]["port"].asUInt64();
  LD("network_central_initialize:" <<
     " CentralHostname: "          << chost << " CentralPort: " << cport);
  network_reconnect_central_client(true);
  return 0;
}

