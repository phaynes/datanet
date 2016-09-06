"use strict"

var fs   = null;

var ZBrowserClient, ZMCG, ZCmdClient, ZCUL, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  fs             = require('fs');
  ZBrowserClient = require('./zbrowser_client');
  ZMCG           = require('./zmemcache_get');
  ZCmdClient     = require('./zcmd_client');
  ZCUL           = require('./zcli_userland');
  ZS             = require('./zshared');
  ZH             = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

var Cli = null;

exports.Initialize = function() {
  Cli = ZH.AmBrowser ? ZBrowserClient.ConstructCmdClient("CLI") :
                       ZCmdClient.ConstructCmdClient    ("CLI");
}

exports.FromFile        = null;
exports.Quiet           = false;
exports.AlwaysPrintLine = false;

var Auto_commit  = false;
var Auto_display = false;
var Collection   = null;
var Zdoc         = null;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// OVERRIDABLE FUNCTIONS -----------------------------------------------------

exports.WriteStdout = function(txt) {
  if (exports.Quiet) return;
  console.log(txt);
}
exports.WriteResponse = exports.WriteStdout;
exports.WritePrompt   = exports.WriteStdout;

exports.WriteStderr = function(txt) {
  console.error(txt);
}

exports.Exit = function(num) { }

exports.Prompt = 'zync> ';

exports.PrintPrompt = function() {
  if (exports.WritePrompt) exports.WritePrompt(exports.Prompt);
}

exports.CursorPos = -1;
exports.ResetCursor    = function(pos)       { }
exports.RewriteLine    = function(post_text) { }
exports.ResetLine      = function()          { }
exports.RewriteNewline = function(post_text) { }
exports.RewriteMidLine = function(post_text) { }


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function validate_number(x) {
  var n = Number(x);
  if (isNaN(n)) {
    return 'INCR|DECR Usage: "INCR field NUMBER" -> use a number';
  }
}

// NOTE: this function is not safe, call validate_json_from_input() before it
function input_json_parse(text) {
  var json;
  try {
    json = JSON.parse(text);
  } catch(e) {
    return text;
  }
  return json;
}

function json_parse(text) {
  var json;
  try {
    json = JSON.parse(text);
  } catch(e) {
    throw(new Error('JSON.parse() Error: ' + e));
  }
  return json;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMAND DEFINITIONS -------------------------------------------------------

var Cmds = { NAMESPACE             : {argc : 1,  name : 'NAMESPACE'          },
             COLLECTION            : {argc : 1,  name : 'COLLECTION'         },

             'ADD-USER'            : {argc : 3,  name : 'ADD-USER'           },
             'REMOVE-USER'         : {argc : 1,  name : 'REMOVE-USER'        },
             GRANT                 : {argc : 3,  name : 'GRANT'              },
             'REMOVE-DATACENTER'   : {argc : 1,  name : 'REMOVE-DATACENTER'  },
             'AGENT-KEYS'          : {argc : 4,  name : 'AGENT-KEYS'         },

             ISOLATION             : {argc : 1,  name : 'ISOLATION'          },
             NOTIFY                : {argc : 2,  name : 'NOTIFY'             },

             STORE                 : {argc : 2,  name : 'STORE'              },
             FETCH                 : {argc : 1,  name : 'FETCH'              },
             FIND                  : {argc : 1,  name : 'FIND'               },
             REMOVE                : {argc : 1,  name : 'REMOVE'             },
             SCAN                  : {argc : 0,  name : 'SCAN'               },

             CACHE                 : {argc : 1,  name : 'CACHE'              },
             PIN                   : {argc : 1,  name : 'PIN'                },
             WATCH                 : {argc : 1,  name : 'WATCH'              },
             'STICKY-CACHE'        : {argc : 1,  name : 'STICKY-CACHE'       },
             EVICT                 : {argc : 1,  name : 'EVICT'              },
             'LOCAL-EVICT'         : {argc : 1,  name : 'LOCAL-EVICT'        },

             EXPIRE                : {argc : 2,  name : 'EXPIRE'             },

             USER                  : {argc : 2,  name : 'USER'               },
             STATION               : {argc : 2,  name : 'STATION'            },
             DESTATION             : {argc : 0,  name : 'DESTATION'          },
             'STATIONED-USERS'     : {argc : 0,  name : 'STATIONED-USERS'    },

             SUBSCRIBE             : {argc : 1,  name : 'SUBSCRIBE'          },
             UNSUBSCRIBE           : {argc : 1,  name : 'UNSUBSCRIBE'        },

             SET                   : {argc : 2,  name : 'SET'                },
             DELETE                : {argc : 1,  name : 'DELETE'             },
             INSERT                : {argc : 3,  name : 'INSERT'             },

             INCR                  : {argc : 2,  name : 'INCR'               },
             DECR                  : {argc : 2,  name : 'DECR'               },

             CHANNELS              : {argc : 1,  name : 'CHANNELS'           },

             COMMIT                : {argc : 0,  name : 'COMMIT'             },
             PULL                  : {argc : 0,  name : 'PULL'               },
             ROLLBACK              : {argc : 0,  name : 'ROLLBACK'           },

             'STATELESS-CREATE'    : {argc : 1,  name : 'STATELESS-CREATE'   },
             'MEMCACHE-CLUSTER'    : {argc : 2,  name : 'MEMCACHE-CLUSTER'   },
             'MEMCACHE-STORE'      : {argc : 4,  name : 'MEMCACHE-STORE'     },
             'MEMCACHE-COMMIT'     : {argc : -2, name : 'MEMCACHE-COMMIT'    },
             'MEMCACHE-GET'        : {argc : 1,  name : 'MEMCACHE-GET'       },
             'STATELESS-COMMIT'    : {argc : 0,  name : 'STATELESS-COMMIT'   },

             LPUSH                 : {argc : 2,  name : 'LPUSH'              },
             LPOP                  : {argc : 1,  name : 'LPOP'               },
             RPUSH                 : {argc : 2,  name : 'RPUSH'              },
             RPOP                  : {argc : 1,  name : 'RPOP'               },
             LINDEX                : {argc : 2,  name : 'LINDEX'             },
             LLEN                  : {argc : 1,  name : 'LLEN'               },
             LRANGE                : {argc : 3,  name : 'LRANGE'             },
             LTRIM                 : {argc : 3,  name : 'LTRIM'              },
             LREM                  : {argc : 3,  name : 'LREM'               },
             LINSERT               : {argc : 4,  name : 'LINSERT'            },

             MESSAGE               : {argc : 1,  name : 'MESSAGE'            },
             REQUEST               : {argc : 1,  name : 'REQUEST'            },

             'AGENT-SUBSCRIPTIONS' : {argc : 0,  name : 'AGENT-SUBSCRIPTIONS'},
             'USER-SUBSCRIPTIONS'  : {argc : 0,  name : 'USER-SUBSCRIPTIONS' },
             'USER-INFO'           : {argc : 2,  name : 'USER-INFO'          },
             'CLUSTER-INFO'        : {argc : 0,  name : 'CLUSTER-INFO'       },
             LATENCIES             : {argc : 0,  name : 'LATENCIES'          },
             'INCREMENT-HEARTBEAT' : {argc : 1,  name : 'INCREMENT-HEARTBEAT'},
             'TIMESTAMP-HEARTBEAT' : {argc : 1,  name : 'TIMESTAMP-HEARTBEAT'},
             'ARRAY-HEARTBEAT'     : {argc : 1,  name : 'ARRAY-HEARTBEAT'    },

             'AUTO-COMMIT'         : {argc : 1,  name : 'AUTO-COMMIT'        },
             'AUTO-DISPLAY'        : {argc : 1,  name : 'AUTO-DISPLAY'       },

             PRINT                 : {argc : -1, name : 'PRINT'              },

             SLEEP                 : {argc : 1,  name : 'SLEEP'              },
             DEBUG                 : {argc : -1, name : 'DEBUG'              },
             HISTORY               : {argc : 0,  name : 'HISTORY'            },

             SHUTDOWN              : {argc : 0,  name : 'SHUTDOWN'           },
             QUIT                  : {argc : 0,  name : 'QUIT'               },
           };

function validate_cmd_line(line) {
  var words = line.split(' ');
  var cmd   = Cmds[words[0].toUpperCase()];
  if (typeof(cmd) === 'undefined') {
    return new Error('Command: (' + words[0] + ') does not exist');
  }
  var wlen = 0;
  for (var i = 0; i < words.length; i++) {
    if (words[i].length > 0) wlen += 1;
  }
  if (cmd.argc > 0) {
    if (wlen < (cmd.argc + 1)) {
      return new Error('Command: (' + words[0] + ') has ' + cmd.argc + ' args');
    }
  } else { // NEGATIVE argc
    if (wlen > 2) {
      return new Error('Command: (' + words[0] + ') has [0,1] args');
    }
  }
}

// NOTE: unsafe function: call validate_cmd_line() before parse_cmd_line()
function parse_cmd_line(cmd, words) {
  var argv = [];
  if        (cmd.argc === 0) {
    argv.push(words[0]);
  } else if (cmd.argc < 0) {
    argv.push(words[0]);
    if (words.length > 1) argv.push(words[1]);
  } else { // argc > 0
    argv.push(words[0]);
    for (var i = 1; i < cmd.argc; i++) {
      argv.push(words[i]);
    }
    var cwords = ZH.clone(words);
    cwords.splice(0, cmd.argc);
    argv.push(cwords.join(" "));
  }
  //ZH.l('parse_cmd_line'); ZH.p(argv);
  return argv;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARGUMENT CHECKS -----------------------------------------------------------

function validate_json_from_input(text) {
  try {
    var json = JSON.parse(text);
  } catch(e) {
    if (text.charAt(0) === "{") {
      return new Error('JSON.parse() Error: ' + e);
    }
  }
  return null;
}

function validate_on_off_cmd(cname, mode) {
  var umode = mode.toUpperCase();
  if (umode !== "OFF" && umode !== "ON") {
    return new Error('Usage: ' + cname + ' [ON|OFF]');
  }
  return null;
}

function validate_add_remove_cmd(cname, mode) {
  var umode = mode.toUpperCase();
  if (umode !== "ADD" && umode !== "REMOVE") {
    return new Error('Usage: ' + cname + ' [ADD|REMOVE]');
  }
  return null;
}

function validate_arq_equality(a, b) {
  var ua = a.toUpperCase();
  var ub = b.toUpperCase();
  if (ua !== ub) {
    return new Error('Expected argument: ' + ua);
  }
  return null;
}

function check_zdoc_defined(zdoc) {
  if (zdoc === null) {
    return new Error('No ZDoc fetched: use FETCH command first');
  }
  return null;
}

function check_collection(collection) {
  if (collection === null) {
    return new Error('No collection: use the COLLECTION command first');
  }
  return null;
}

function check_args_store(collection, text) {
  if (collection === null) {
    return Error('No collection: use the COLLECTION command first');
  }
  var json;
  try {
    json = json_parse(text);
  } catch (e) {
    return e;
  }
  if (typeof(json._id) === 'undefined') {
    return Error('DocumentKey must be defined in "_id" field');
  }
  return null;
}

function check_args_find(collection, query) {
  var err = check_collection(collection);
  if (err) return err;
  err = validate_json_from_input(query);
  if (err) return err;
  return null;
}

function check_args_fetch(collection, key) {
  var err = check_collection(collection);
  if (err) return err;
  if (typeof(key) === 'undefined') {
    return new Error('Fetch Error: Key must be defined');
  }
  return null;
}

function check_args_expire(collection, key, expire) {
  var err = check_args_fetch(collection, key);
  if (err) return err;
  if (isNaN(expire)) {
    return new Error('USAGE: EXPIRE: key expire(SECONDS)');
  }
  return null;
}

function check_args_message(mbody) {
  var json;
  try {
    json = json_parse(mbody);
  } catch (e) {
    return e;
  }
  return null;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS COMMANDS ---------------------------------------------------------

exports.SetAgentAddress = function(ip, port) {
  Cli.SetAgentAddress(ip, port);
}

function do_print_cmd(zdoc, argv) {
  var p;
  if (argv.length == 2) p = ZH.LookupByDotNotation(zdoc._, argv[1]);
  else {
    if (zdoc === null) {
      exports.WriteStderr('ZDOC not defined');
      return;
    }
    p = ZH.clone(zdoc._);
    for (var i = 0; i < ZH.DocumentKeywords.length; i++) { // JUST SHOW DATA
      var kw = ZH.DocumentKeywords[i];
      delete(p[kw]);
    }
  }
  exports.WriteResponse(JSON.stringify(p, null, '    '));
}

exports.DoPrintCmd = function(zdoc, argv) {
  do_print_cmd(zdoc, argv);
}

function do_debug_cmd(zdoc, whom) {
  var p;
  if        (typeof(whom) === 'undefined') {
    exports.WriteStdout('    >> SETTINGS: >>>>>>>>>>>>>>>>>>>>>>>>');
    exports.WriteStdout('        Auto_commit: '  + Auto_commit);
    exports.WriteStdout('        Auto_display: ' + Auto_display);
    exports.WriteStdout('    >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>');
  } else {
    var w = whom.toUpperCase();
    if        (w === 'JSON')  {
      var p = zdoc._;
      exports.WriteStdout(JSON.stringify(p, null, '    '));
    } else if (w === 'CRDT')  {
      var p = zdoc.$;
      exports.WriteStdout(JSON.stringify(p, null, '    '));
    } else if (w === 'DELTA') {
      var p = zdoc.oplog;
      exports.WriteStdout(JSON.stringify(p, null, '    '));
    } else if (w === 'DATA')  {
      var p = ZH.DebugCrdtData(zdoc.$);
      exports.WriteStdout(JSON.stringify(p, null, '    '));
    } else return "CommandUsage: DEBUG [JSON|CRDT|DELTA]";
  }
  return null;
}

// NOTE: unprotected, call validate_on_off_cmd() beforehand
function do_auto_commit_cmd(zdoc, mode) {
  var umode = mode.toUpperCase();
  if        (umode === "OFF") {
    Auto_commit = false;
    exports.WriteResponse('+OK');
    return false;       // return value populates variable 'async_call'
  } else if (umode === "ON") {
    Auto_commit = true;
    if (zdoc === null) {
      exports.WriteResponse('+OK');
      return true;
    } else {
      return do_commit(zdoc); // return value -> 'async_call'
    }
  }
}

// NOTE: unprotected, call validate_on_off_cmd() beforehand
function do_auto_display_cmd(mode) {
  var umode = mode.toUpperCase();
  if        (umode === "OFF") {
    Auto_display = false;
  } else if (umode === "ON") {
    Auto_display = true;
  }
  exports.WriteResponse('+OK');
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMAND HISTORY -----------------------------------------------------------

var RealComHist       = []; // real history of commands
var RealComHistPos    = 0;  // index of current real history
var DisplayComHist    = []; // displayed history (i.e. shown in terminal)
var DisplayComHistPos = -1;
DisplayComHist[RealComHistPos] = ''; // init first line of input

//TODO write RealComHist[] to file and readin-file on startup

exports.ResetHistoryAndCursor = function(line) {
  if (line.length > 0) {
    RealComHist.push(line);
    RealComHistPos = RealComHistPos + 1;
    // on reset:display what happened
    DisplayComHist = ZH.clone(RealComHist);
    DisplayComHist[RealComHistPos] = '';
  }
  DisplayComHistPos = -1;
  exports.CursorPos = -1;
  return '';
}

exports.GetPreviousDisplayHistory = function() {
  if (DisplayComHistPos !== 0) { // NO-OP @ beginning of history
    if (DisplayComHistPos === -1) { // before current end
      DisplayComHistPos = DisplayComHist.length - 2;
    }
    else {
      DisplayComHistPos = DisplayComHistPos    - 1;
    }
    return DisplayComHist[DisplayComHistPos];
  } else return null;
}

exports.GetNextDisplayHistory = function() {
  if (DisplayComHistPos === -1) return null;
  // NO-OP end of history
  if (DisplayComHistPos === (DisplayComHist.length - 1)) return null;
  DisplayComHistPos = DisplayComHistPos + 1;
  return DisplayComHist[DisplayComHistPos];
}

exports.GetCurrentDisplayHistory = function() {
  var pos = (DisplayComHistPos === -1) ? RealComHistPos : DisplayComHistPos;
  return DisplayComHist[pos];
}

exports.StoreCurrentCommandInDisplayHistory = function(line) {
  // any change to the current line, goes into the end of DisplayCHistory[]
  var pos = (DisplayComHistPos === -1) ? RealComHistPos : DisplayComHistPos;
  DisplayComHist[pos] = line;
  return pos;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN CALLS ---------------------------------------------------------------

function do_namespace(ns) {
  Cli.SetNamespace(ns);
}

// NOTE: unprotected, call validate_on_off_cmd() beforehand
function do_isolation(mode, next) {
  var umode = mode.toUpperCase();
  var b;
  if      (umode === "OFF") b = false;
  else if (umode === "ON")  b = true;
  Cli.Isolation(b, next);
}

// NOTE: unprotected, call validate_add_remove_cmd() beforehand
function do_notify(cmd, url, next) {
  var ucmd = cmd.toUpperCase();
  Cli.SetNotify(ucmd, url, next);
}

function do_channels_cmd(zdoc, chans) {
  var nrchans = [];
  var cs      = chans.split(",");
  for (var i = 0; i < cs.length; i++) {
    var c = ZH.IfNumberConvert(cs[i]);
    nrchans.push(c);
  }
  zdoc.set_channels(nrchans);
  exports.WriteResponse('+OK: REPLICATION-CHANNELS: ' + 
                        JSON.stringify(nrchans));
}

function do_sleep(duration, next) {
  duration = Number(duration);
  if (isNaN(duration)) return 'USAGE: SLEEP milliseconds'
  else {
    setTimeout(next, duration);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ASYNC DATA CALLS ----------------------------------------------------------

// NOTE: unprotected, call check_args_store() first
function do_store(collection, text, next) {
  var json = json_parse(text);
  collection.store(json._id, json, next);
}

// NOTE: unprotected, call check_args_fetch() first
function do_fetch(collection, key, next) {
  collection.fetch(key, next);
}

// NOTE: unprotected, call check_args_find() first
function do_find(collection, query, next) {
  collection.find(query, next);
}

// NOTE: unprotected, call check_args_fetch() first
function do_cache(collection, key, next) {
  collection.cache(key, next);
}

// NOTE: unprotected, call check_args_fetch() first
function do_pin(collection, key, next) {
  collection.pin(key, next);
}

// NOTE: unprotected, call check_args_fetch() first
function do_watch(collection, key, next) {
  collection.watch(key, next);
}

// NOTE: unprotected, call check_args_fetch() first
function do_sticky_cache(collection, key, next) {
  collection.sticky_cache(key, next);
}
// NOTE: unprotected, call check_args_fetch() first
function do_evict(collection, key, next) {
  collection.evict(key, next);
}

// NOTE: unprotected, call check_args_fetch() first
function do_local_evict(collection, key, next) {
  collection.local_evict(key, next);
}

// NOTE: unprotected, call check_args_expire() first
function do_expire(collection, key, expire, next) {
  collection.expire(key, expire, next);
}

// NOTE: unprotected, call check_args_fetch() first
function do_remove(collection, key, next) {
  collection.remove(key, next);
}

// NOTE: unprotected, call check_collection() first
function do_scan(collection, next) {
  collection.scan(next);
}

// NOTE: unprotected, call check_zdoc_defined() first
function do_commit(zdoc) {
  busy = true;
  zdoc.commit(handle_updated_zdoc);
  return true; // return value populates variable 'async_call'
}

// NOTE: unprotected, call check_zdoc_defined() first
function do_rollback(collection, zdoc, next) {
  var key = zdoc._._id;
  collection.fetch(key, next);
}

function do_memcache_cluster(mcname, mpcfg) {
  var dnodes;
  try {
    var data = fs.readFileSync(mpcfg, 'utf8');
    dnodes   = JSON.parse(data);
  } catch (e) {
    return 'ERROR PARSING MEMCACHE-POOL CONFIG FILE: ' + e;
  }
  Cli.MemcacheClusterName = mcname;
  ZMCG.SetClientMemcacheClusterState(mcname, dnodes);
  exports.WriteResponse('+OK');
}

function do_stateless_create(collection, key, next) {
  collection.stateless_create(key, next);
}

function do_memcache_store(collection, sep, ex, text, next) {
  var json = json_parse(text);
  collection.memcache_store(json._id, sep, ex, json, next);
}

function do_memcache_get(collection, key, root, next) {
  collection.memcache_get(key, root, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ASYNC ADMIN CALLS ---------------------------------------------------------

function do_add_user(username, password, role, next) {
  Cli.AddUser(username, password, role, next);
}

function do_remove_user(username, next) {
  Cli.RemoveUser(username, next);
}

function do_grant_user(username, schanid, priv, next) {
  Cli.GrantUser(username, schanid, priv, next);
}

function do_remove_datacenter(dcuuid, next) {
  Cli.RemoveDataCenter(dcuuid, next);
}

function do_agent_keys(guuid, nkeys, minage, wonly, next) {
  Cli.GetAgentKeys(guuid, nkeys, minage, wonly, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ASYNC USER & SUBSCRIPTION CALLS -------------------------------------------

function do_station_user(username, password, next) {
  var auth = {username : username, password : password};
  Cli.StationUser(auth, next);
}

function do_destation_user(next) {
  Cli.DestationUser(next);
}

function do_subscribe(schanid, next) {
  Cli.Subscribe(schanid, next);
}

function do_unsubscribe(schanid, next) {
  Cli.Unsubscribe(schanid, next);
}

function do_switch_user(username, password, next) {
  var auth = {username : username,
              password : password};
  Cli.SwitchUser(auth, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ASYNC INFO CALLS ----------------------------------------------------------

function do_get_user_info(username, password, next) {
  var auth = {username : username, password : password};
  Cli.GetUserInfo(auth, next);
}

function do_get_user_subscriptions(next) {
  Cli.GetUserSubscriptions(next);
}

function do_get_agent_subscriptions(next) {
  Cli.GetAgentSubscriptions(next);
}

function do_stationed_users(next) {
  Cli.GetStationedUsers(next);
}

function do_cluster_info(next) {
  Cli.GetClusterInfo(next);
}

function do_latencies(next) {
  Cli.GetLatencies(next);
}

function do_increment_heartbeat(arg, next) {
  var res   = arg.split(' ');
  var cmd   = res[0];
  var field = null;
  if (res.length > 1) field = res[1];
  var c     = cmd.toUpperCase();
  if (c === 'START' && !field) {
    return next(new Error(ZS.Errors.IncrementHeartbeatUsage), null);
  }
  Cli.Heartbeat(cmd, field, null, null, null, true, false, next);
}

function do_dt_heartbeat(arg, isa, next) {
  var res   = arg.split(' ');
  var cmd   = res[0];
  var uuid  = null;
  var mlen  = null;
  var trim  = null;
  if (res.length > 1) {
    uuid = res[1];
    mlen = res[2];
    if (!uuid || !mlen) {
      if (isa) return next(new Error(ZS.Errors.ArrayHeartbeatUsage),     null);
      else     return next(new Error(ZS.Errors.TimestampHeartbeatUsage), null);
    }
  }
  if (res.length > 3) trim = res[3];
  Cli.Heartbeat(cmd, null, uuid, mlen, trim, false, isa, next);
}

function do_timestamp_heartbeat(arg, next) {
  do_dt_heartbeat(arg, false, next);
}

function do_array_heartbeat(arg, next) {
  do_dt_heartbeat(arg, true, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MESSAGE CALL --------------------------------------------------------------

function do_message(mbody, next) {
  Cli.Message(mbody, next);
}

function do_request(mbody, next) {
  Cli.Request(mbody, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SHUTDOWN CALL -------------------------------------------------------------

function do_shutdown(next) {
  Cli.Shutdown(next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ASYNC CALL HANDLERS -------------------------------------------------------

function post_handle_function() {
  if (cmd_q.length === 0) exports.PrintPrompt();
  drain_cmd_q();
}

function handle_sleep() {
  post_handle_function();
}

function handle_generic(err, res) {
  if (err) exports.WriteStderr(err.message);
  else {
    exports.WriteResponse('+OK');
    if (Auto_display) do_print_cmd(Zdoc, []);
  }
  post_handle_function();
}

function handle_fetch(err, zdoc) {
  if (err) {
    if (err.message === ZS.Errors.NoDataFound) exports.WriteResponse('-MISS');
    else                                       exports.WriteStderr(err.message);
  } else {
    Zdoc = zdoc;
    if (zdoc !== null) {
      // Browser CallBack: KEY-UPDATE
      if (typeof(ZS.EngineCallback.UpdateKeyFunc) !== 'undefined') {
        ZS.EngineCallback.UpdateKeyFunc(Zdoc._);
      }
    }
    exports.WriteResponse('+OK');
    if (Auto_display) do_print_cmd(Zdoc, []);
  }
  post_handle_function();
}

function handle_find(err, jsons) {
  if (err) exports.WriteStderr(err.message);
  else {
    for (var i = 0; i < jsons.length; i++) {
      exports.WriteResponse(JSON.stringify(jsons[i], null, '    '));
    }
  }
  post_handle_function();
}

function handle_json(err, json) {
  if (err) exports.WriteStderr(err.message);
  else     exports.WriteResponse(JSON.stringify(json, null, '    '));
  post_handle_function();
}

function handle_updated_zdoc(err, zdoc) {
  if (err) exports.WriteStderr(err.message);
  else {
    Zdoc = zdoc;
    if (zdoc !== null) {
      if (typeof(ZS.EngineCallback.UpdateKeyFunc) !== 'undefined') {
        ZS.EngineCallback.UpdateKeyFunc(Zdoc._);
      }
    }
    if (Auto_commit) exports.WriteResponse('+COMMITTED');
    else             exports.WriteResponse('+OK');
    if (Auto_display) do_print_cmd(Zdoc, []);
  }
  post_handle_function();
}

function handle_remove(err, rres) {
  if (err) exports.WriteStderr(err.message);
  else {
    Zdoc = null;
    exports.WriteResponse('+OK');
    if (Auto_display) do_print_cmd(Zdoc, []);
  }
  post_handle_function();
}

function handle_scan(err, zdocs) {
  if (err) exports.WriteStderr(err.message);
  else {
    var skip_print_scan = false;
    if (typeof(ZS.EngineCallback.ScanFunc) !== 'undefined') {
      skip_print_scan = ZS.EngineCallback.ScanFunc(zdocs);
    }
    if (!skip_print_scan) {
      for (var key in zdocs) {
        exports.WriteResponse("KEY: " + key);
        do_print_cmd(zdocs[key], []);
      }
    }
  }
  post_handle_function();
}

function generic_ecb_handler(lerr, hres, rline, res, ecb_func) {
  if (lerr) exports.WriteStderr(lerr);
  else {
    if (typeof(res) === 'undefined') {
      exports.WriteResponse(null);
    } else {
      exports.WriteResponse(rline);
    }
    if (typeof(ecb_func) !== 'undefined') ecb_func(res);
  }
  post_handle_function();
}

function handle_get_user_subscriptions(lerr, hres) {
  if (hres === null) hres = {};
  var rline    = '+OK: CHANNELS: ' + hres.subscriptions;
  var ecb_func = ZS.EngineCallback.GetUserSubscriptionsFunc;
  generic_ecb_handler(lerr, hres, rline, hres.subscriptions, ecb_func);
}

function handle_get_agent_subscriptions(lerr, hres) {
  if (hres === null) hres = {};
  var rline    = '+OK: CHANNELS: ' + hres.subscriptions;
  var ecb_func = ZS.EngineCallback.GetAgentSubscriptionsFunc;
  generic_ecb_handler(lerr, hres, rline, hres.subscriptions, ecb_func);
}

function handle_get_stationed_users(lerr, hres) {
  if (hres === null) hres = {};
  var rline    = '+OK: USERS: ' + hres.users;
  var ecb_func = ZS.EngineCallback.GetStationedUsersFunc;
  generic_ecb_handler(lerr, hres, rline, hres.users, ecb_func);
}

function handle_switch_user(lerr, hres) {
  if (hres === null) hres = {};
  var rline    = '+OK';
  var ecb_func = ZS.EngineCallback.ChangeUserFunc;
  generic_ecb_handler(lerr, hres, rline, hres.user, ecb_func);
}

function handle_get_info(err, ures) {
  if (err) exports.WriteStderr(err.message);
  else {
    var pres = JSON.stringify(ures, null, "    ")
    exports.WriteResponse('+OK: INFO: ' + pres);
  }
  post_handle_function();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMAND EXECUTION ---------------------------------------------------------

function run_auto_commit_display(zdoc, async_call) {
  exports.WriteResponse('+OK');
  if      (Auto_commit)  async_call = do_commit(zdoc);
  else if (Auto_display) do_print_cmd(zdoc, []);
  return async_call;
}

var cmd_q = [];
var busy  = false;

function drain_cmd_q() {
  while (cmd_q.length > 0) {
    var next_line = cmd_q.shift();
    exports.PrintPrompt();
    if (do_line(next_line)) return; // called command's callback will drain_q
  }
  busy             = false;
  // when first cmd_q[] gets drained, switch to stdin(put)
  exports.FromFile = false;
}

exports.ProcessLine = function(line) {
  if (busy) cmd_q.push(line);
  else      do_line  (line);
}

// OVERRIDE THIS FUNC
exports.CollectionChanged = function(cname) { }

function do_line(line) {
  var zdoc       = Zdoc;
  var collection = Collection;
  var async_call = false;
  if (line.length !== 0) {
    if (exports.FromFile || exports.AlwaysPrintLine) {
      exports.WriteStdout(line + "\n"); // can come from cmd_q[]
    }
    // Browser CallBack: PREPROCESS-COMMAND
    if (typeof(ZS.EngineCallback.PreprocessCommandFunc) !== 'undefined') {
      ZS.EngineCallback.PreprocessCommandFunc(line);
    }
    var err = null;
    {
      err = validate_cmd_line(line);
      if (!err) {
        var words = line.split(' ');
        var cmd   = Cmds[words[0].toUpperCase()];
        var argv  = parse_cmd_line(cmd, words);
        var cname = cmd.name;

        // NAMESPACE & COLLECTION
        if        (cname === "NAMESPACE") {
          Zdoc       = null;
          Collection = null;
          var ns     = argv[1];
          do_namespace(ns);
          exports.WriteResponse('+OK');
          // Browser CallBack: UPDATE-NAMESPACE
          if (typeof(ZS.EngineCallback.UpdateNamespaceFunc) !== 'undefined') {
            ZS.EngineCallback.UpdateNamespaceFunc(ns);
          }
        } else if (cname === "COLLECTION") {
          Zdoc       = null;
          var cname  = argv[1];
          Collection = Cli.Collection(cname);
          exports.CollectionChanged(cname); // Calls into Z(zbrowser.js)
          // Browser CallBack: UPDATE-COLLECTION
          if (typeof(ZS.EngineCallback.UpdateCollectionFunc) !== 'undefined') {
            ZS.EngineCallback.UpdateCollectionFunc(cname);
          }
          exports.WriteResponse('+OK');

        // ADMIN COMMANDS
        } else if (cname === "ADD-USER") {
          busy       = true;
          async_call = true;
          do_add_user(argv[1], argv[2], argv[3], handle_generic);
        } else if (cname === "REMOVE-USER") {
          busy       = true;
          async_call = true;
          do_remove_user(argv[1], handle_generic);
        } else if (cname === "GRANT") {
          busy       = true;
          async_call = true;
          do_grant_user(argv[1], argv[2], argv[3], handle_generic);
        } else if (cname === "REMOVE-DATACENTER") {
          busy       = true;
          async_call = true;
          do_remove_datacenter(argv[1], handle_generic);
        } else if (cname === "AGENT-KEYS") {
          busy       = true;
          async_call = true;
          do_agent_keys(argv[1], argv[2], argv[3], argv[4], handle_get_info);

        // DEVICE COMMANDS
        } else if (cname === "STATION") { //TODO validate [uname,pw]
          busy       = true;
          async_call = true;
          do_station_user(argv[1], argv[2], handle_generic);
        } else if (cname === "DESTATION") {
          busy       = true;
          async_call = true;
          do_destation_user(handle_generic);
        } else if (cname === "USER") { //TODO validate legality of [uname,pw]
          Zdoc       = null;
          busy       = true;
          async_call = true;
          do_switch_user(argv[1], argv[2], handle_switch_user);

        // DOCUMENT COMMANDS
        } else if (cname === "STORE") {
          var text = argv[1];
          if (argv.length === 3) text = text + argv[2];
          err = check_args_store(collection, text);
          if (!err) {
            busy       = true;
            async_call = true;
            do_store(collection, text, handle_updated_zdoc);
          }
        } else if (cname === "REMOVE") {
          err = check_args_fetch(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_remove(collection, argv[1], handle_remove);
          }
        } else if (cname === "FETCH") {
          err = check_args_fetch(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_fetch(collection, argv[1], handle_fetch);
          }
        } else if (cname === "FIND") {
          err = check_args_find(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_find(collection, argv[1], handle_find);
          }
        } else if (cname === "SCAN") {
          err = check_collection(collection);
          if (!err) {
            busy       = true;
            async_call = true;
            do_scan(collection, handle_scan);
          }

        // CACHE & EVICT
        } else if (cname === "CACHE") {
          err = check_args_fetch(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_cache(collection, argv[1], handle_fetch);
          }
        } else if (cname === "PIN") {
          err = check_args_fetch(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_pin(collection, argv[1], handle_fetch);
          }
        } else if (cname === "WATCH") {
          err = check_args_fetch(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_watch(collection, argv[1], handle_fetch);
          }
        } else if (cname === "STICKY-CACHE") {
          err = check_args_fetch(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_sticky_cache(collection, argv[1], handle_fetch);
          }
        } else if (cname === "EVICT") {
          err = check_args_fetch(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_evict(collection, argv[1], handle_updated_zdoc);
          }
        } else if (cname === "LOCAL-EVICT") {
          err = check_args_fetch(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_local_evict(collection, argv[1], handle_updated_zdoc);
          }

        // EXPIRE
        } else if (cname === "EXPIRE") {
          err = check_args_expire(collection, argv[1], argv[2]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_expire(collection, argv[1], argv[2], handle_generic);
          }

        // CHANNEL COMMANDS
        } else if (cname === "SUBSCRIBE") {
          busy       = true;
          async_call = true;
          do_subscribe(argv[1], handle_generic);
        } else if (cname === "UNSUBSCRIBE") {
          busy       = true;
          async_call = true;
          do_unsubscribe(argv[1], handle_generic);

        // WRITE OPS
        } else if (cname === "SET") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            err = validate_json_from_input(argv[2]);
            if (!err) {
              err = zdoc.s(argv[1], input_json_parse(argv[2]));
              if (!err) async_call = run_auto_commit_display(zdoc, async_call);
            }
          }
        } else if (cname === "DELETE") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            err = zdoc.d(argv[1]);
            if (!err) async_call = run_auto_commit_display(zdoc, async_call);
          }
        } else if (cname === "INSERT") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            err = validate_json_from_input(argv[3]);
            if (!err) {
              err = zdoc.insert(argv[1], argv[2],
                                input_json_parse(argv[3]));
              if (!err) async_call = run_auto_commit_display(zdoc, async_call);
            }
          }
        // NUMBER WRITE OPERATIONS
        } else if (cname === "INCR" || cname === "DECR") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            err = validate_number(argv[2]);
            if (!err) {
              if (cname === "INCR") err = zdoc.incr(argv[1], argv[2]);
              else                  err = zdoc.decr(argv[1], argv[2]);
              if (!err) async_call = run_auto_commit_display(zdoc, async_call);
            }
          }

        // MODIFY DOCUMENT COMMANDS
        } else if (cname === "CHANNELS") {
          err = check_zdoc_defined(zdoc);
          if (!err) err = do_channels_cmd(zdoc, argv[1]);

        // TRANSACTION COMMIT
        } else if (cname === "COMMIT") {
          err = check_zdoc_defined(zdoc);
          if (!err) async_call = do_commit(zdoc);
        } else if (cname === "ROLLBACK") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            busy       = true;
            async_call = true;
            do_rollback(collection, zdoc, handle_updated_zdoc);
          }
        } else if (cname === "PULL") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            busy       = true;
            async_call = true;
            zdoc.pull(handle_updated_zdoc);
          }

        // STATELESS/MEMCACHE
        } else if (cname === "MEMCACHE-CLUSTER") {
          err = do_memcache_cluster(argv[1], argv[2]);
        } else if (cname === "MEMCACHE-STORE") {
          var text = argv[3];
          if (argv.length === 5) text = text + argv[4];
          err = check_args_store(collection, text);
          if (!err) {
            var sep    = argv[1];
            if (sep !== "true" && sep !== "false") {
              err = "SECOND ARG MUST BE BOOLEAN";
            }
            sep = Boolean(sep);
            if (!err) {
              var ex     = argv[2];
              if (isNaN(ex)) {
                err = "THIRD ARG MUST BE NUMBER";
              }
              ex = Number(ex);
              if (!err) {
                busy       = true;
                async_call = true;
                do_memcache_store(collection, sep, ex, text,
                                  handle_updated_zdoc);
              }
            }
          }
        } else if (cname === "STATELESS-CREATE") {
          err = check_args_fetch(collection, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_stateless_create(collection, argv[1], handle_updated_zdoc);
          }
        } else if (cname === "MEMCACHE-COMMIT") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            if (argv.length > 2) err = validate_number(argv[2]);
            if (!err) {
              var sep    = (argv.length > 1) ? (argv[1] ? true : false) : true;
              var ex     = (argv.length > 2) ? argv[2] : 0;
              busy       = true;
              async_call = true;
              zdoc.memcache_commit(sep, ex, handle_updated_zdoc);
            }
          }
        } else if (cname === "MEMCACHE-GET") {
          var res = argv[1].split(" ");
          if (res.length > 2) err = "Command: (MEMCACHE-GET) has 1-2 args";
          else {
            var key  = res[0];
            var root = res[1];
            err      = check_args_fetch(collection, key);
            if (!err) {
              busy       = true;
              async_call = true;
              do_memcache_get(collection, key, root, handle_json);
            }
          }
        } else if (cname === "STATELESS-COMMIT") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            busy       = true;
            async_call = true;
            zdoc.stateless_commit(handle_updated_zdoc);
          }

        // LIST WRITE OPERATIONS
        } else if (cname === "LPUSH"  || cname === "RPUSH"     ||
                   cname === "LPUSHX" || cname === "RPUSHX") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            err = validate_json_from_input(argv[2]);
            if (!err) {
              if        (cname === "LPUSH") {
                err = ZCUL.do_lpush (zdoc, argv[1], input_json_parse(argv[2]));
              } else if (cname === "RPUSH") {
                err = ZCUL.do_rpush (zdoc, argv[1], input_json_parse(argv[2]));
              } else if (cname === "LPUSHX") {
                err = ZCUL.do_lpushx(zdoc, argv[1], input_json_parse(argv[2]));
              } else if (cname === "RPUSHX") {
                err = ZCUL.do_rpushx(zdoc, argv[1], input_json_parse(argv[2]));
              }
              if (!err) async_call = run_auto_commit_display(zdoc, async_call);
            }
          }
        } else if (cname === "LPOP" || cname === "RPOP") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            if        (cname === "LPOP") {
              err = ZCUL.do_lpop(zdoc, argv[1]);
            } else if (cname === "RPOP") {
              err = ZCUL.do_rpop(zdoc, argv[1]);
            }
            if (!err) async_call = run_auto_commit_display(zdoc, async_call);
          }
        } else if (cname === "LINSERT") {
          err = check_zdoc_defined(zdoc);
          if (!err) {
            err = ZCUL.do_linsert(zdoc, argv[1], argv[2], argv[3], argv[4]);
            if (!err) async_call = run_auto_commit_display(zdoc, async_call);
          }
        } else if (cname === "LTRIM") {
          err = check_zdoc_defined(zdoc);
          if (!err) err = ZCUL.do_ltrim(zdoc, argv[1], argv[2], argv[3]);
          if (!err) async_call = run_auto_commit_display(zdoc, async_call);
        } else if (cname === "LREM") {
          err = check_zdoc_defined(zdoc);
          if (!err) err = ZCUL.do_lrem(zdoc, argv[1], argv[2], argv[3]);
          if (!err) async_call = run_auto_commit_display(zdoc, async_call);

        // LIST READ OPERATIONS
        } else if (cname === "LINDEX") {
          err = check_zdoc_defined(zdoc);
          if (!err) err = ZCUL.do_lindex(zdoc, argv[1], argv[2]);
        } else if (cname === "LLEN") {
          err = check_zdoc_defined(zdoc);
          if (!err) err = ZCUL.do_llen(zdoc, argv[1]);
        } else if (cname === "LRANGE") {
          err = check_zdoc_defined(zdoc);
          if (!err) err = ZCUL.do_lrange(zdoc, argv[1], argv[2], argv[3]);

        // MESSAGE OPERATIONS
        } else if (cname === "MESSAGE") {
          err = check_args_message(argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_message(argv[1], handle_generic);
          }
        } else if (cname === "REQUEST") {
          err = check_args_message(argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_request(argv[1], handle_generic);
          }

        // READ OPERATIONS
        } else if (cname === "PRINT") {
          err = check_zdoc_defined(zdoc);
          if (!err) do_print_cmd(zdoc, argv);

        // ISOLATION
        } else if (cname === "ISOLATION") {
          err = validate_on_off_cmd(cname, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_isolation(argv[1], handle_generic);
          }

        // NOTIFY
        } else if (cname === "NOTIFY") {
          err = validate_add_remove_cmd(cname, argv[1]);
          if (!err) {
            busy       = true;
            async_call = true;
            do_notify(argv[1], argv[2], handle_generic);
          }

        // INFO COMMANDS
        } else if (cname === "STATIONED-USERS") {
          busy       = true;
          async_call = true;
          do_stationed_users(handle_get_stationed_users);
        } else if (cname === "USER-INFO") {
          busy       = true;
          async_call = true;
          do_get_user_info(argv[1], argv[2], handle_get_info);
        } else if (cname === "USER-SUBSCRIPTIONS") {
          busy       = true;
          async_call = true;
          do_get_user_subscriptions(handle_get_user_subscriptions);
        } else if (cname === "AGENT-SUBSCRIPTIONS") {
          busy       = true;
          async_call = true;
          do_get_agent_subscriptions(handle_get_agent_subscriptions);
        } else if (cname === "CLUSTER-INFO") {
          busy       = true;
          async_call = true;
          err = do_cluster_info(handle_get_info);
        } else if (cname === "LATENCIES") {
          busy       = true;
          async_call = true;
          err = do_latencies(handle_get_info);
        } else if (cname === "INCREMENT-HEARTBEAT") {
          busy       = true;
          async_call = true;
          err = do_increment_heartbeat(argv[1], handle_get_info);
        } else if (cname === "TIMESTAMP-HEARTBEAT") {
          busy       = true;
          async_call = true;
          err = do_timestamp_heartbeat(argv[1], handle_get_info);
        } else if (cname === "ARRAY-HEARTBEAT") {
          busy       = true;
          async_call = true;
          err = do_array_heartbeat(argv[1], handle_get_info);

        // DISPLAY/HELPER/DEBUG COMMANDS
        } else if (cname === "AUTO-COMMIT") {
          err = validate_on_off_cmd(cname, argv[1]);
          if (!err) async_call = do_auto_commit_cmd(zdoc, argv[1]);
        } else if (cname === "AUTO-DISPLAY") {
          err = validate_on_off_cmd(cname, argv[1]);
          if (!err) do_auto_display_cmd(argv[1]);
        } else if (cname === "SLEEP") {
          busy       = true;
          async_call = true;
          err = do_sleep(argv[1], handle_sleep);
        } else if (cname === "DEBUG") {
          err = check_zdoc_defined(zdoc);
          if (!err) err = do_debug_cmd(zdoc, argv[1]);
        } else if (cname === "HISTORY") {
          exports.WriteStderr(RealComHist);

        // SHUTDOWN
        } else if (cname === "SHUTDOWN") {
          busy       = true;
          async_call = true;
          do_shutdown(handle_generic);

        // QUIT
        } else if (cname === "QUIT") {
          Cli.Close();
          exports.WriteStdout('+BYE\n');
          exports.Exit(0);
        }

      }
    }
    if (ZH.IsDefined(err) && err !== null) exports.WriteStderr(err);
    if (!busy) exports.PrintPrompt();
  }
  return async_call;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PROCESS SINGLE KEY STROKE -------------------------------------------------

// NOTE: key :{cstr, ccode, is_up, is_down, is_left, is_right}
exports.ProcessKeyStroke = function(key) {
  var pos  = (DisplayComHistPos === -1) ? RealComHistPos : DisplayComHistPos;
  var line = DisplayComHist[pos];
  if        (key.ccode === 13) { // CARRAIGE-RETURN
    if (!line || line.length === 0) { // no line, print newline and prompt
      exports.WriteStdout("\r\n");
      exports.PrintPrompt();
    } else {
      exports.WriteStdout("\r\n");
      exports.ProcessLine(line);
      exports.ResetHistoryAndCursor(line);
    }
    return;
  } else if (key.ccode === 127) { // BACKSPACE
    if (line.length == 0) return; // NO-OP at start
    if (exports.CursorPos === -1) line = line.slice(0, -1);
    else {
      line = line.substr(0, exports.CursorPos - exports.Prompt.length - 1) +
             line.substr(   exports.CursorPos - exports.Prompt.length);
      exports.CursorPos = exports.CursorPos -1;
    }
    exports.RewriteMidLine(line);
  } else if (key.is_up) {
    line = exports.GetPreviousDisplayHistory();
    if (line) exports.RewriteNewline(line);
    else      return;
  } else if (key.is_down) {
    line = exports.GetNextDisplayHistory();
    if (line) exports.RewriteLine(line);
    else {
      exports.ResetLine();
      return;
    }
  } else if (key.is_left) {
    // NO-OP @ beginning of line
    if (exports.CursorPos === exports.Prompt.length) return;
    if (exports.CursorPos === -1) {
      exports.CursorPos = exports.Prompt.length + line.length - 1;
    } else {
      exports.CursorPos = exports.CursorPos - 1;
    }
    exports.ResetCursor(exports.CursorPos);
  } else if (key.is_right) {
    // OP only if previous left-arrow this line
    if (exports.CursorPos === -1) return;
    if (exports.CursorPos !== (line.length + exports.Prompt.length)) {
      exports.CursorPos = exports.CursorPos + 1;
    }
    exports.ResetCursor(exports.CursorPos);

  } else { // all other characters
    if (exports.CursorPos === -1) {
      line = line + key.cstr;
      exports.WriteStdout(key.cstr);
    } else {
      line = line.insert((exports.CursorPos - exports.Prompt.length), key.cstr);
      exports.CursorPos = exports.CursorPos + 1;
      exports.RewriteMidLine(line);
    }
  }

  pos = exports.StoreCurrentCommandInDisplayHistory(line);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZCC']={} : exports);

