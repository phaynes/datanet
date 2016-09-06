"use strict";

var fs         = require('fs');

var ZNotify    = require('./znotify');
var ZCmdClient = require('./zcmd_client');
var ZQ         = require('./zqueue');
var ZS         = require('./zshared');
var ZH         = require('./zhelper');

ZH.NetworkDebug = false;
ZH.Debug        = false;
ZH.LogToConsole = true;
ZH.ZyncRole     = 'TESTER';
ZH.MyUUID       = 'TESTER';

var FullDebug = false;

//FullDebug = true;
if (FullDebug) {
  ZH.NetworkDebug = true;
  ZH.Debug        = true;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ARGV[] --------------------------------------------------------------------

var Config_file = process.argv[2];
if (typeof(Config_file)  === 'undefined') Usage();

var Json;
try {
  var data = fs.readFileSync(Config_file, 'utf8');
  Json     = JSON.parse(data);
} catch (e) {
  console.error('CONFIG-FILE: ' + Config_file);
  console.error('CONFIG-FILE-ERROR: ' + e);
  process.exit(-1);
}

if (!Json.agents)  ConfigFileError();
if (!Json.actions) ConfigFileError();

function Usage(err) {
  if (typeof(err) !== 'undefined') ZH.e('Error: ' + err);
  ZH.e('Usage: ' + process.argv[0] + ' ' + process.argv[1] + ' config_file');
  process.exit(-1);
}

function ConfigFileError(err) {
  if (typeof(err) !== 'undefined') console.error('Error: ' + err);
  console.error('CONFIG-FILE must contain: agents : [], actions : []');
  process.exit(-1);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

var NextCondSignalNumber = 0;
function get_next_cond_signal_number() {
  NextCondSignalNumber += 1;
  return NextCondSignalNumber;
}

function are_equal(a, b) {
  var atype = typeof(a);
  var btype = typeof(b);
  if (atype !== btype) return false;
  if (atype === "number" || atype === "string") return (a === b);
  if (Array.isArray(atype)) { // ARRAY
    if (a.length !== b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (!are_equal(a[i], b[i])) return false;
    }
    return true;
  } else {                    // OBJECT
    if (Object.keys(a).length !== Object.keys(b).length) return false;
    for (var key in a) {
      if (!are_equal(a[key], b[key])) return false;
    }
    return true;
  }
}

var NAME_VARIABLE = "${name}";
var NEXT_VARIABLE = "${next}";

var NextVariable = {};
function get_next_variable(name) {
  if (!NextVariable[name]) NextVariable[name] = 0;
  NextVariable[name] += 1;
  return NextVariable[name];
}

function check_variables_keywords(vars) {
  if (!vars) return null;
  for (var vname in vars) {
    var regex = "${" + vname + "}";
    if (regex === NAME_VARIABLE ||
        regex === NEXT_VARIABLE) {
      return '-ERROR: VARIABLE NAME: (' + vname + ') IS A KEYWORD';
    }
  }
  return null;
}

function evaluate_arg_val(arg, name) {
  if (typeof(arg) !== 'string') return arg;
  var ret = arg;
  if (ret.search(NAME_VARIABLE)) {
    ret = ret.replace(NAME_VARIABLE, name);
  }
  if (ret.search(NEXT_VARIABLE)) {
    var nval = get_next_variable(name);
    ret      = ret.replace(NEXT_VARIABLE, nval);
  }
  return ret;
}

function validate_capped_list(cl) {
  var verr = null;
  var cnts = {};
  ZH.l('validate_capped_list: CL'); ZH.l(cl);
  for (var i = 0; i < cl.length; i++) {
    var c    = cl[i];
    var els  = c.split('.');
    var name = els[0];
    var cnt  = Number(els[1]);
    if (cnts[name]) {
      var lcnt = cnts[name];
      if (lcnt) {
        var ecnt = lcnt - 1;
        if (ecnt !== cnt) {
          var verr = '-ERROR: Agent: ' + name + ' LastCount: ' + lcnt +
                     ' not predecessor of CurrentCount: ' + cnt;
          return Error(verr);
        }
      }
    }
    cnts[name] = cnt;
  }
  return null;
}

var ValidateFunctions = {};
ValidateFunctions["validate_capped_list"] = validate_capped_list;

function get_agent_pid(auuid, next) {
  try {
    var pidfile = '/tmp/ZYNC_AGENT_PID_' + auuid;
    var fdata   = fs.readFileSync(pidfile, 'utf8');
    if (!fdata) {
      var err = 'ERROR: EMPTY: pidfile: ' + pidfile;
      next(err, null);
      return;
    } else {
      var pid = Number(fdata);
      next(null, pid);
      return;
    }
  } catch(e) {
    var err = 'ERROR: get_agent_pid: ' + e;
    next(err, null);
    return;
  }
}

function set_agent_to_sync_keys(auuid, b, next) {
  get_agent_pid(auuid, function(gerr, pid) {
    if (gerr) next(gerr, null);
    else {
      var sig = b ? 'SIGXCPU' : 'SIGXFSZ';
      ZH.t('COMMAND: kill -s ' + sig + ' ' + pid);
      process.kill(pid, sig);
      next(null, null);
    }
  });
}  

function set_agent_delta_reaper(auuid, b, next) {
  get_agent_pid(auuid, function(gerr, pid) {
    if (gerr) next(gerr, null);
    else {
      var sig = b ? 'SIGVTALRM' : 'SIGTRAP';
      ZH.t('COMMAND: kill -s ' + sig + ' ' + pid);
      process.kill(pid, sig);
      next(null, null);
    }
  });
}  

function set_agent_second_timer(auuid, b, next) {
  get_agent_pid(auuid, function(gerr, pid) {
    if (gerr) next(gerr, null);
    else {
      var sig = b ? 'SIGSYS' : 'SIGPWR';
      ZH.t('COMMAND: kill -s ' + sig + ' ' + pid);
      process.kill(pid, sig);
      next(null, null);
    }
  });
}  

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------
// INITIALIZATION ----------------------------------------------------------

var Finished       = false;

var Collection     = {};
var Kqk            = {};

var Clients        = {};

var NotifyServer   = {};

var CondSignalData = {};
var CondWaitKeys   = {};

var WaitThreadIds  = {};
var WaitNext       = null;

function parse_agent_file(afile) {
  var cdata;
  try {
    var txt = fs.readFileSync(afile, 'utf8');
    cdata   = JSON.parse(txt);
  } catch (e) {
    console.error('ERROR: PARSE: AGENT-FILE(' + afile + '): ' + e);
    process.exit(-1);
  }
  return cdata;
}

function initialize(next) {
  for (var i = 0; i < Json.agents.length; i++) {
    var cdata = Json.agents[i];
    if (cdata.file) {
      cdata = parse_agent_file(cdata.file);
    }
    var cli   = ZCmdClient.ConstructCmdClient(cdata.name);
    Clients[cdata.name]      = cli;
    var uuid                 = cdata.uuid ? cdata.uuid : 
                                            i; // NOTE: UNIQUE ENOUGH
    Clients[cdata.name].uuid = uuid;
    Clients[cdata.name].ish  = cdata.https;
    cli.SetAgentAddress(cdata.ip, cdata.port);
  }
  next(null, null);
}

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------
// THREAD HANDLER ----------------------------------------------------------

var ThreadsRunning = {};

function should_exit() {
  if (!Finished) return false;
  var nk = Object.keys(CondWaitKeys).length;
  if (nk !== 0) return false;
  var nt = Object.keys(ThreadsRunning).length;
  if (nt !== 0) return false;
  return true;
}

function thread_exit(xerr, tid) {
  ZH.e('THREAD_EXIT: tid: ' + tid);
  delete(ThreadsRunning[tid]);
  delete(WaitThreadIds [tid]);
  var nw = Object.keys(WaitThreadIds).length;
  if (WaitNext && nw === 0) {
    ZH.l('WAIT is OVER');
    var next = WaitNext;
    WaitNext = null;
    next(null, null);
    return;
  }
  do_exit(xerr, tid);
}

function start_thread(tid, actions, adata, next) {
  if (ThreadsRunning[tid]) {
    next(new Error('Thread: ' + tid + ' already declared'), null);
  } else {
    delete(adata.thread);
    var cmds = [adata];
    while(actions.length !== 0) {
      var act  = actions.shift();
      var ctid = act.thread;
      if (ctid !== tid) {
        actions.unshift(act);
        break;
      }
      delete(act.thread);
      cmds.push(act);
    }
    ZH.l('start_thread: tid: ' + tid + ' #cmds: ' + cmds.length);
    ThreadsRunning[tid] = true;
    process_actions(cmds, function(perr, pres) {
      thread_exit(perr, tid);
    });
    next(null, null);
  }
}


// -------------------------------------------------------------------------
// -------------------------------------------------------------------------
// NOTIFY HANDLER ----------------------------------------------------------

var CondSignaled   = [];

function send_cond_signal(name, kqk, desc) {
  ZH.t('SEND: COND_SIGNAL: name: ' + name + ' K: ' + kqk);
  CondSignaled.push(desc);

  var next;
  if (CondWaitKeys[name] && CondWaitKeys[name][kqk]) {
    next = CondWaitKeys[name][kqk];
    delete(CondWaitKeys[name][kqk]);
    var nw = Object.keys(CondWaitKeys[name]).length;
    if (nw === 0) delete(CondWaitKeys[name]);
  }

  delete(CondSignalData[name][kqk]);
  var ns = Object.keys(CondSignalData[name]).length;
  if (ns === 0) delete(CondSignalData[name]);

  if (should_exit()) {
    clean_exit('COND_WAIT');
    return;
  }
  if (next) {
    next(null, null);
  }
}

function debug_evaluate_notify_plus(name, ediff, diff, orig, curr) {
  ZH.t('EN: (+) name: ' + name + ' ediff: ' + ediff + ' diff: ' + diff +
       ' orig: ' + orig  + ' curr: ' + curr);
}

function debug_evaluate_notify_equals(name, fval, curr) {
  ZH.l('EN: (=) name: ' + name);
  ZH.l('fval'); ZH.l(fval);
  ZH.l('curr'); ZH.l(curr);
}

function debug_evaluate_notify_shift(name, ival, icurr) {
  ZH.l('name: ' + name);
  ZH.l('ival');  ZH.l(ival);
  ZH.l('icurr'); ZH.l(icurr);
}

function debug_evaluate_notify_merge(val, dbg) {
  ZH.l('EN: MERGE_CASE');
  ZH.l('val'); ZH.l(val);
  ZH.l('dbg'); ZH.l(dbg);
}

function evaluate_notify(name, kqk, remove, orig, curr,
                         valop, val, dbg, ism, desc) {
  if (valop === "merge_case") {
    if (!dbg) return;
    //debug_evaluate_notify_merge(val, dbg)
    if (dbg.subscriber_merge_case === val) send_cond_signal(name, kqk, desc);
  } else {
    if (ism) return; // NOTE: NO DATA in SubscriberMerge Notifications
    if (valop === "removed") {
      ZH.t('EN: remove: ' + remove);
      if (remove) send_cond_signal(name, kqk, desc);
    } else if (!remove) { // NOTHING TO EVALUATE ON REMOVE
      if        (valop === "+") {
        var ediff = val;
        var diff  = curr - orig;
        debug_evaluate_notify_plus(name, ediff, diff, orig, curr);
        if (ediff === diff) send_cond_signal(name, kqk, desc);
      } else if (valop === "=") {
        var fval = val;
        debug_evaluate_notify_equals(name, fval, curr)
        if (are_equal(fval, curr)) send_cond_signal(name, kqk, desc);
      } else if (valop === "shift") {
        var ival  = val;
        var icurr = curr[0];
        //debug_evaluate_notify_shift(name, ival, icurr)
        if (are_equal(ival, icurr)) send_cond_signal(name, kqk, desc);
      } else if (valop === "deleted") {
        ZH.t('EN: (deleted) name: '+name);ZH.t('curr'); ZH.t(curr);
        if (ZH.IsUndefined(curr)) send_cond_signal(name, kqk, desc);
      }
    }
  }
}

function debug_notify_handler(kqk, name, data) {
  ZH.l('notify_handler: name: ' + name + ' K: ' + kqk);
  ZH.l('DATA');           ZH.l(data);
  ZH.l('CondSignalData'); ZH.l(CondSignalData);
}

function notify_handler(url, data) {
  var flags = data.flags;
  var kqk   = data.kqk;
  var name  = NotifyServer[url];
  debug_notify_handler(kqk, name, data)
  if (CondSignalData[name] && CondSignalData[name][kqk]) {
    var wk     = CondSignalData[name][kqk];
    var field  = wk.field;
    var valop  = wk.valop;
    var val    = wk.value;
    var snum   = wk.signum; 
    var json   = data.json;
    var remove = data.flags ? data.flags.remove : undefined;
    var dbg    = data.debug;
    var ism    = (dbg && dbg.subscriber_merge_case);
    if (!json && !remove && !dbg) return; 
    var curr;
    if (!remove && !ism) { // NO CHECK ON REMOVE or SubscrberMerge
      curr = field.length ? ZH.LookupByDotNotation(json, field) : json;
      var cfname = wk.checker;
      if (cfname) {
        var cfunc = ValidateFunctions[cfname];
        var cerr  = cfunc(curr);
        if (cerr) throw(cerr);
      }
    }
    var desc  = {name : name, kqk : kqk, remove : remove, field : field,
                 value_op : valop, value : val, num : snum, debug : dbg};
    evaluate_notify(name, kqk, remove, wk.original, curr,
                    valop, val, dbg, ism, desc)
  }
}

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------
// COMMANDS ----------------------------------------------------------------

function persist_zdoc_in_memory(name, key, nzdoc, next) {
  Kqk[name][key] = nzdoc;
  next(null, nzdoc);
}

function do_fetch(name, key, next) {
  var collection = Collection[name];
  collection.fetch(key, function(ferr, nzdoc) {
    if (ferr) next(ferr, null);
    else      persist_zdoc_in_memory(name, key, nzdoc, next)
  });
}

function flag_fetch_zdoc(name, key, fetch, next) {
  if (fetch) do_fetch(name, key, next);
  else {
    var zdoc = Kqk[name][key];
    next(null, zdoc);
  }
}

function do_commit(zdoc, name, key, next) {
  zdoc.commit(function(ferr, nzdoc) {
    if (ferr) next(ferr, null);
    else      persist_zdoc_in_memory(name, key, nzdoc, next)
  });
}

function flag_commit_zdoc(zdoc, name, key, commit, next) {
  if (commit) do_commit(zdoc, name, key, next);
  else        next(null, zdoc);
}

var Cmds = { NAMESPACE    : {argc :  1, fname : cmd_namespace},
             COLLECTION   : {argc :  1, fname : cmd_collection},
             USER         : {argc :  2, fname : cmd_user},
             GRANT        : {argc :  3, fname : cmd_grant},
             STATION      : {argc :  2, fname : cmd_station},
             SUBSCRIBE    : {argc :  1, fname : cmd_subscribe},
             STORE        : {argc :  2, fname : cmd_store},
             FETCH        : {argc :  1, fname : cmd_fetch},
             COMMIT       : {argc :  1, fname : cmd_commit},
             REMOVE       : {argc :  1, fname : cmd_remove},
             INCR         : {argc :  3, fname : cmd_incr},
             SET          : {argc :  3, fname : cmd_set},
             DELETE       : {argc :  2, fname : cmd_delete},
             INSERT       : {argc :  4, fname : cmd_insert},
             NOTIFY       : {argc :  2, fname : cmd_notify},
             UNNOTIFY     : {argc :  2, fname : cmd_unnotify},
             COND_SIGNAL  : {argc : -4, fname : cmd_cond_signal},
             COND_WAIT    : {argc :  1, fname : cmd_cond_wait},
             WAIT         : {argc :  1, fname : cmd_wait, local : true},
             ISOLATION    : {argc :  1, fname : cmd_isolation},
             TO_SYNC_KEYS : {argc :  1, fname : cmd_to_sync_keys},
             DELTA_REAPER : {argc :  1, fname : cmd_delta_reaper},
             SECOND_TIMER : {argc :  1, fname : cmd_second_timer},
             SLEEP        : {argc :  1, fname : cmd_sleep},
           };

function cmd_sleep(cli, args, flags, next) {
  var name = cli.name;
  var ts   = args[0];
  ZH.t('cmd_sleep: (' + name + ') TS: ' + ts);
  setTimeout(function() { next(null, null); }, ts);
}

function cmd_to_sync_keys(cli, args, flags, next) {
  var name  = cli.name;
  var auuid = cli.uuid;
  var b     = args[0];
  ZH.t('cmd_to_sync_keys: (' + name + ') auuid: ' + auuid + ' b: ' + b);
  set_agent_to_sync_keys(auuid, b, next);
}

function cmd_delta_reaper(cli, args, flags, next) {
  var name  = cli.name;
  var auuid = cli.uuid;
  var b     = args[0];
  ZH.t('cmd_delta_reaper: (' + name + ') auuid: ' + auuid + ' b: ' + b);
  set_agent_delta_reaper(auuid, b, next);
}

function cmd_second_timer(cli, args, flags, next) {
  var name  = cli.name;
  var auuid = cli.uuid;
  var b     = args[0];
  ZH.t('cmd_second_timer: (' + name + ') auuid: ' + auuid + ' b: ' + b);
  set_agent_second_timer(auuid, b, next);
}

function cmd_namespace(cli, args, flags, next) {
  var name = cli.name;
  var ns   = args[0];
  ZH.t('cmd_namespace: (' + name + ') NS: ' + ns);
  cli.SetNamespace(ns);
  next(null, null);
}

function cmd_collection(cli, args, flags, next) {
  var name  = cli.name;
  var cname = args[0];
  ZH.t('cmd_collection: (' + name + ') CNAME: ' + cname);
  Collection[name] = cli.Collection(cname);
  Kqk[name]        = {};
  next(null, null);
}

function cmd_user(cli, args, flags, next) {
  var name     = cli.name;
  var username = args[0];
  var password = args[1];
  var auth     = {username : username, password : password};
  ZH.t('cmd_user: (' + name + ') USERNAME: ' + username);
  cli.SetClientAuthentication(username, password);
  cli.SwitchUser(auth, next);
}

function cmd_grant(cli, args, flags, next) {
  var name     = cli.name;
  var username = args[0];
  var schanid  = args[1];
  var priv     = args[2];
  ZH.t('cmd_grant: USER: ' + username + ' CHANNEL: ' + schanid +
       ' PRIV: ' + priv);
  cli.GrantUser(username, schanid, priv, next);
}

function cmd_station(cli, args, flags, next) {
  var name     = cli.name;
  var username = args[0];
  var password = args[1];
  var auth     = {username : username, password : password};
  ZH.t('cmd_station: (' + name + ') USERNAME: ' + username);
  cli.IsUserStationed(username, function(uerr, stationed) {
    var ok = (uerr && uerr.message === ZS.Errors.NotStationed);
    if      (uerr && !ok) next(uerr, null);
    else if (stationed)   next(null, null);
    else                  cli.StationUser(auth, next);
  });
}

function cmd_subscribe(cli, args, flags, next) {
  var name  = cli.name;
  var rchan = args[0];
  ZH.t('cmd_subscribe: (' + name + ') CHANNEL: ' + rchan);
  cli.IsSubscribed(rchan, function(ierr, hit) {
    if (ierr) next(ierr, null);
    else {
      if (hit) next(null, null);
      else     cli.Subscribe(rchan, next);
    }
  });
}

function cmd_store(cli, args, flags, next) {
  var name = cli.name;
  var key  = args[0];
  var json = args[1];
  ZH.t('cmd_store: (' + name + ') KEY: ' + key);
  var collection = Collection[name];
  collection.store(key, json, function(serr, nzdoc) {
    if (serr) next(serr, null);
    else      persist_zdoc_in_memory(name, key, nzdoc, next)
  });

}

function cmd_fetch(cli, args, flags, next) {
  var name = cli.name;
  var key  = args[0];
  ZH.t('cmd_fetch: (' + name + ') KEY: ' + key);
  do_fetch(name, key, next);
}

function cmd_commit(cli, args, flags, next) {
  var name       = cli.name;
  var key        = args[0];
  ZH.t('cmd_commit: (' + name + ') KEY: ' + key);
  var collection = Collection[name];
  var zdoc       = Kqk[name][key];
  if (!zdoc) next(new Error('Key not found -> FETCH or CACHE key first'), null);
  else       do_commit(zdoc, name, key, next);
}

function cmd_remove(cli, args, flags, next) {
  var name       = cli.name;
  var key        = args[0];
  ZH.t('cmd_remove: (' + name + ') key: ' + key);
  var collection = Collection[name];
  collection.remove(key, function(rerr, rres) {
    if (rerr) {
      if ((rerr.message == ZS.Errors.NoDataFound) ||
          (rerr.message == ZS.Errors.NoKeyToRemove)) next(null, rres); // OK
      else                                           next(rerr, null);
    } else                                           next(null, rres);
  });
}

function cmd_delete(cli, args, flags, next) {
  var name   = cli.name;
  var key    = args[0];
  var field  = args[1];
  var fetch  = flags ? flags.fetch  : false;
  var commit = flags ? flags.commit : false;
  ZH.t('cmd_delete: (' + name + ') key: ' + key + ' field: ' + field +
       ' fetch: ' + fetch + ' commit: ' + commit);
  flag_fetch_zdoc(name, key, fetch, function(ferr, zdoc) {
    if (ferr) next(ferr, null);
    else {
      if (!zdoc) next(new Error(ZS.Errors.TestKeyNotLocal), null);
      else {
        var ierr = zdoc.d(field);
        if (ierr) next(ierr, null);
        else      flag_commit_zdoc(zdoc, name, key, commit, next);
      }
    }
  });
}

function cmd_incr(cli, args, flags, next) {
  var name   = cli.name;
  var key    = args[0];
  var field  = args[1];
  var val    = evaluate_arg_val(args[2], name);
  var fetch  = flags ? flags.fetch  : false;
  var commit = flags ? flags.commit : false;
  ZH.t('cmd_incr: (' + name + ') key: ' + key + ' field: ' + field +
       ' val: ' + val + ' fetch: ' + fetch + ' commit: ' + commit);
  flag_fetch_zdoc(name, key, fetch, function(ferr, zdoc) {
    if (ferr) next(ferr, null);
    else {
      if (!zdoc) next(new Error(ZS.Errors.TestKeyNotLocal), null);
      else {
        var ierr = zdoc.incr(field, val);
        if (ierr) next(ierr, null);
        else      flag_commit_zdoc(zdoc, name, key, commit, next);
      }
    }
  });
}

function cmd_set(cli, args, flags, next) {
  var name   = cli.name;
  var key    = args[0];
  var field  = args[1];
  var val    = evaluate_arg_val(args[2], name);
  var fetch  = flags ? flags.fetch  : false;
  var commit = flags ? flags.commit : false;
  ZH.t('cmd_set: (' + name + ') key: ' + key + ' field: ' + field +
       ' val: ' + val + ' fetch: ' + fetch + ' commit: ' + commit);
  flag_fetch_zdoc(name, key, fetch, function(ferr, zdoc) {
    if (ferr) next(ferr, null);
    else {
      if (!zdoc) next(new Error(ZS.Errors.TestKeyNotLocal), null);
      else {
        var ierr = zdoc.s(field, val);
        if (ierr) next(ierr, null);
        else      flag_commit_zdoc(zdoc, name, key, commit, next);
      }
    }
  });
}

function cmd_insert(cli, args, flags, next) {
  var name   = cli.name;
  var key    = args[0];
  var field  = args[1];
  var pos    = args[2];
  var val    = evaluate_arg_val(args[3], name);
  var fetch  = flags ? flags.fetch  : false;
  var commit = flags ? flags.commit : false;
  ZH.t('cmd_insert: (' + name + ') key: ' + key + ' pos: ' + pos +
       ' val: ' + val + ' field: ' + field +
       ' fetch: ' + fetch + ' commit: ' + commit);
  flag_fetch_zdoc(name, key, fetch, function(ferr, zdoc) {
    if (ferr) next(ferr, null);
    else {
      if (!zdoc) next(new Error(ZS.Errors.TestKeyNotLocal), null);
      else {
        var ierr = zdoc.insert(field, pos, val);
        if (ierr) next(ierr, null);
        else      flag_commit_zdoc(zdoc, name, key, commit, next);
      }
    }
  });
}

function cmd_cond_signal(cli, args, flags, next) {
  var name   = cli.name;
  var key    = args[0];
  var field  = args[1];
  var valop  = args[2];
  var val    = args[3];
  var cfname = args[4];
  if (cfname && !ValidateFunctions[cfname]) {
    next(new Error('ValidationFunction: ' + cfname +
                   ' not defined in Array: ValidateFunctions[]'), null);
    return;
  }
  var fetch = flags ? flags.fetch  : false;
  flag_fetch_zdoc(name, key, fetch, function(ferr, zdoc) {
    if (ferr) next(ferr, null);
    else {
      if (!zdoc && valop === "+") {
        next(new Error(ZS.Errors.TestKeyNotLocalValOpEq), null);
      } else {
        var ks = ZH.CompositeQueueKey(cli.ns, cli.cn, key);
        ZH.t('cmd_cond_signal: (' + name + ') kqk: ' + ks.kqk +
             ' field: ' + field + ' valop: ' + valop + ' val: ' + val);
        var orig;
        if (zdoc) {
          var json = zdoc._;
          orig     = field.length ? ZH.LookupByDotNotation(json, field) : json;
        }
        var snum = get_next_cond_signal_number();
        CondSignalData[name]         = {};
        CondSignalData[name][ks.kqk] = {field    : field,
                                        valop    : valop,
                                        value    : val,
                                        original : orig,
                                        signum   : snum,
                                        checker  : cfname};
        next(null, null);
      }
    }
  });
}

function cmd_cond_wait(cli, args, flags, next) {
  var name  = cli.name;
  var key   = args[0];
  var ks    = ZH.CompositeQueueKey(cli.ns, cli.cn, key);
  var kqk   = ks.kqk;
  ZH.t('cmd_cond_wait: (' + name + ') K: ' + kqk);
  if (!CondSignalData[name] || !CondSignalData[name][kqk]) {
    ZH.t('NO SIGNALER -> NO COND_WAIT');
    next(null, null);
  } else {
    if (!CondWaitKeys[name]) CondWaitKeys[name] = {};
    CondWaitKeys[name][kqk] = next;
    ZH.t('COND_WAIT ON SIGNAL: name: ' + name + ' K: ' + kqk);
  }
}

function cmd_notify(cli, args, flags, next) {
  var name        = cli.name;
  var ish         = cli.ish;
  var notify_ip   = args[0];
  var notify_port = args[1];
  ZNotify.CreateNotifyServer(ish, notify_ip, notify_port, notify_handler,
  function(cerr, url) {
    if (cerr) next(cerr, null);
    else {
      NotifyServer[url] = name;
      ZH.t('NotifyServer: NAME: ' + name + ' URL: ' + url);
      cli.SetNotify('ADD', url, next);
    }
  });
}

function cmd_unnotify(cli, args, flags, next) {
  var name        = cli.name;
  var ish         = cli.ish;
  var notify_ip   = args[0];
  var notify_port = args[1];
  var url         = ZNotify.GetNotifyServerURL(ish, notify_ip, notify_port);
  ZH.t('cmd_unnotify: NAME: ' + name + ' URL: ' + url);
  cli.SetNotify('REMOVE', url, next);
}

function cmd_isolation(cli, args, flags, next) {
  var name = cli.name;
  var b     = args[0];
  ZH.t('cmd_isolation: (' + name + ') b: ' + b);
  cli.Isolation(b, next);
}

function cmd_wait(cli, args, flags, next) {
  var nt = Object.keys(ThreadsRunning).length;
  if (nt === 0) {
    ZH.t('cmd_wait: NO THREADS -> NO WAIT');
    next(null, null);
  } else {
    if (args.length === 0) { // NO ARGS -> ALL TIDS
      for (var tid in ThreadsRunning) {
        WaitThreadIds[tid] = true;
      }
    } else {
      for (var i = 0; i < args.length; i++) {
        var tid = args[i];
        if (ThreadsRunning[tid]) {
          WaitThreadIds[tid] = true;
        } else {
          ZH.t('WARNING: cmd_wait: tid: ' + tid + ' NOT RUNNING');
        }
      }
    }
    ZH.t('cmd_wait: on: ' + Object.keys(WaitThreadIds) + ' Running: ' + nt);
    WaitNext = next;
  }
}

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------
// COMMANDS ----------------------------------------------------------------

function summarize_action(name, xname, args) {
  return ' ERROR: CLIENT: ' + name + ' CMD: ' + xname +
         ' ARGS: [' + args.join() + ']';
}

function do_variable_replace(data, vars) {
  if (!vars) return data;
  for (var vname in vars) {
    var vval  = vars[vname];
    var regex = "${" + vname + "}";
    data      = data.replaceAll(regex, vval);
  }
  return data;
}

function append_file_to_actions(fname, vars, actions, next) {
  var cmds;
  try {
    var data = fs.readFileSync(fname, 'utf8');
    data     = do_variable_replace(data, vars);
    cmds     = JSON.parse(data);
  } catch (e) {
    next(e);
    return;
  }
  if (!Array.isArray(cmds)) {
    next(new Error("CommandFile must contain ARRAY of commands"), null);
  } else {
    ZH.l('FILE: ' + fname + ' #cmds: ' + cmds.length);
    var cmnt_cmd = {"comment" : "RUNNING FILE: " + fname};
    actions.unshift(cmnt_cmd);
    for (var i = cmds.length - 1; i >= 0; i--) {
      actions.unshift(cmds[i]);
    }
    next(null, null);
  }
}

function handle_error_response(cerr, eprfx, ferr, next) {
  if (!ferr) {
    var herr = '-ERROR: Expected Error did not happen: ';
    if (cerr) herr += cerr;
    else      herr += eprfx;
    next(new Error(herr), null);
  } else {
    var ok = cerr ? (ferr.message === cerr) :
                    (ferr.message.search(eprfx) === 0);
    if (!ok) next(ferr, null);
    else {
      if (cerr) ZH.t('OK: MATCH: Expected Error: ' + cerr);
      else      ZH.t('OK: MATCH: Expected Error Prefix: ' + eprfx);
      next(null, null);
    }
  }
}

function next_command(cmd, cli, args, loops, actions, adata, next) {
  var sts = adata.sleep;
  if (!sts || !loops) {
    run_command(cmd, cli, args, loops, actions, adata, next);
  } else {
    ZH.t('run_command: SLEEP: ' + sts);
    setTimeout(function() {
      run_command(cmd, cli, args, loops, actions, adata, next);
    }, sts);
  }
}

function run_command(cmd, cli, args, loops, actions, adata, next) {
  if (loops === 0) next(null, null);
  else {
    loops -= 1;
    var flags = adata.flags;
    var cerr  = adata.error;
    var eprfx = adata.error_prefix;
    cmd.fname(cli, args, flags, function(ferr, fres) {
      if (cerr || eprfx) handle_error_response(cerr, eprfx, ferr, next);
      else {
        if (ferr) next(ferr, null);
        else      next_command(cmd, cli, args, loops, actions, adata, next);
      }
    });
  }
}

function process_actions(actions, next) {
  if (actions.length === 0) next(null, null);
  else {
    var adata = actions.shift();
    var cmnt  = adata.comment;
    var fname = adata.file;
    if (cmnt) {
      ZH.t('COMMENT: ' + cmnt);
      process_actions(actions, next);
    } else if (fname) {
      var vars = adata.variables;
      var verr = check_variables_keywords(vars);
      if (verr) throw(verr);
      append_file_to_actions(fname, vars, actions, function(aerr, ares) {
        if (aerr) next(aerr, null);
        else      process_actions(actions, next);
      });
    } else {
      if (!adata.command) {
        next(new Error(ZS.Errors.InvalidTestCommand), null);
      }
      var xname = adata.command.toUpperCase();
      var cmd   = Cmds[xname];
      if (!cmd) {
        next(new Error('ERROR: COMMAND: ' + xname + ' NOT FOUND'), null);
        return;
      }

      var name  = adata.client;
      var cli   = Clients[name];
      var local = cmd ? cmd.local : null;
      if (!local && !cli) {
        next(new Error('ERROR: CLIENT: ' + name + ' NOT FOUND'), null);
        return;
      } 

      var args  = adata.args;
      if (!Array.isArray(args)) {
        next(new Error('ERROR: ARGS must be type: ARRAY '), null);
        return;
      } else if (cmd.args > 0 && args.length !== cmd.argc) {
        var err = 'ERROR: COMMAND: ' + xname + ' expects: ' + cmd.argc +
                  ' arguments';
        next(new Error(err), null);
        return;
      } else if (cmd.args < 0 && args.length < cmd.argc) {
        var err = 'ERROR: COMMAND: ' + xname + ' expects minimum: ' +
                   cmd.argc + ' arguments';
        next(new Error(err), null);
        return;
      }

      var tid = adata.thread;
      if (tid) {
        start_thread(tid, actions, adata, function(serr, sres) {
          if (serr) {
            var csumm = summarize_action(name, xname, args) 
            var esumm = csumm + ' ' + JSON.stringify(serr.message);
            next(new Error(esumm), null);
          } else {
            process_actions(actions, next);
          }
        });
      } else {
        var loops = adata.loops ? adata.loops : 1;
        run_command(cmd, cli, args, loops, actions, adata,
        function(serr, sres) {
          if (serr) {
            var csumm = summarize_action(name, xname, args);
            var esumm = csumm + ' ' + JSON.stringify(serr.message);
            next(new Error(esumm), null);
          } else {
            process_actions(actions, next);
          }
        });
      }
    }
  }
}

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------
// MAIN --------------------------------------------------------------------

function clean_exit(tid) {
  ZH.l('clean_exit: tid: ' + tid);
  ZH.e('TEST-RUN-RESULTS'); ZH.e(CondSignaled);
  process.exit(0);
}

function do_exit(serr, tid) {
  if (serr) {
    var etxt  = tid ? 'TID: ' + tid : 'MAIN: ';
    etxt     += serr.message;
    ZH.e(etxt);
    process.exit(-1);
  } else {
    Finished = true;
    if (should_exit()) clean_exit(tid);
  }
}

var Actions = Json.actions;

var next = do_exit;
var tid  = 0;

initialize(function(ierr, ires) {
  if (ierr) next(new Error('INITIALIZE: ERROR: ' + ierr.message), tid);
  else {
    process_actions(Actions, function(perr, pres) {
      if (perr) {
        var etxt = perr.message ? perr.message : perr;
        next(new Error('PROCESS_ACTIONS: ERROR: ' + etxt), tid);
      } else {
        next(null, tid);
      }
    });
  }
});

