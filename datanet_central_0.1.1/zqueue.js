"use strict";

var ZCSub, ZAF, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZCSub = require('./zcluster_subscriber');
  ZAF   = require('./zaflow');
  ZS    = require('./zshared');
  ZH    = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MUTEX PRIMITIVE -----------------------------------------------------------

// FUNCTION: Mutex
// PURPOSE:  A primitive Mutex to Queue events that should NOT overlap
// NOTE:      Technically this is an associated array of Mutexes
//            it is used for per-key or per-subscriber mutexes
//
function Mutex(mutex_name) {
  this.Pending = {}; // Per key Counter of last GetMutex call
  this.Started = {}; // Per key Counter of last Mutex acquired
  this.Lock    = {}; // Per key mutex lock
  this.Name    = mutex_name;

  this.Release = function(key, err, hres, next) {
    this.Lock[key] = false; // Mutex Release
    if      (next) next(err, hres);
    else if (err)  throw(err);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FLOW ----------------------------------------------------------------------

// NOTE: FIXME: all FLOWs require an OP argument
//              and all flow.f() functions must use OP to call OP's func()

var DebugFlow         = false;
DebugFlow             = true;
var ExtendedDebugFlow = false;

function get_flow_summary(flow, qe) {
  var fid;
  if        (flow.m.Name === 'CentralKey') {
    fid = ZH.Central.GetCentralKeySerializationId(qe);
  } else if (flow.m.Name === 'CentralKeyRead') {
    fid = ZCSub.GetFlowCentralKeyReadId(qe);
  } else if (flow.m.Name === 'CentralUserRead') {
    fid = ZCSub.GetFlowCentralUserReadId(qe);
  } else if (flow.m.Name === 'AgentKey') {
    fid = ZAF.GetFlowAgentKeyId(qe);
  } else if (flow.m.Name === 'PubSub') {
    fid = ZCSub.GetFlowSendSubscriberId(qe);
  }
  var summ  = 'FID: ' + fid;
  summ     += qe.ks ? (' K: ' + qe.ks.kqk) : (' U: ' + qe.duuid);
  if (ExtendedDebugFlow) {
    var sdata  = JSON.stringify(qe.data);
    summ      += ' DATA: ' + sdata;
  }
  return summ;
}

function debug_flow(flow, qe, prfx, kqlen) {
  if (!DebugFlow) return;
  var fname  = flow.m.Name;
  var summ   = get_flow_summary(flow, qe);
  var now    = ZH.GetMsTime();
  ZH.l('FLOW: ' + prfx + ': (' + fname + ') OP: ' + qe.op +
       ' #KQ: ' + kqlen + ' @: ' + now + ' ' +  summ);
}

function flow_incr(x) {
  if (x) return x + 1;
  else   return 1;
}

function flow_cmp_gt(a, b) {
  if (a === Number.MAX_VALUE) return (b > a);
  else                        return (a > b);
}

function empty_flow(plugin, collections, flow, next) {
  if (flow.q[flow.k].length === 0) next(null, null);
  else {
    var qe = flow.q[flow.k].shift(); // Get FIRST Queue element
    debug_flow(flow, qe, 'START', flow.q[flow.k].length);
    flow.f(plugin, collections, qe, function(derr, dres) {
      debug_flow(flow, qe, 'END', flow.q[flow.k].length);
      setImmediate(empty_flow, plugin, collections, flow, next);
    });
  }
}

function flow_next(plugin, collections, flow, next) {
  flow.m.Started[flow.k] = flow.m.Pending[flow.k]; // Set inside Mutex
  empty_flow(plugin, collections, flow, function(qerr, qres) {
    if (flow_cmp_gt(flow.m.Pending[flow.k], flow.m.Started[flow.k])) {
      flow_next(plugin, collections, flow, next);
    } else next(null, null);
  });
}

exports.StartFlow = function(plugin, collections, flow) {
  flow.m.Pending[flow.k] = flow_incr(flow.m.Pending[flow.k]);// Per request
  if (flow.m.Lock[flow.k] !== true) {                        // Mutex Check
    flow.m.Lock[flow.k]  = true;                             // Mutex Lock
    flow_next(plugin, collections, flow, function(perr, pres) {
      flow.m.Release(flow.k, null);                          // Unlock Mutex
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL KEY QUEUE ---------------------------------------------------------

// NOTE: BOTH MUTEX AND QUEUE are that same for ROUTER & STORAGE
//       in ROLE: BOTH this insures serialization at BOTH levels
//       in ROLE: [ROUTER,STORAGE] this has no effect

// PURPOSE: Mutex so Central processes per-key methods serially
var CentralKeySerialization     = new Mutex('CentralKey');
exports.RouterKeySerialization  = CentralKeySerialization;
exports.StorageKeySerialization = CentralKeySerialization;

// PURPOSE: Per Central KEY Queue for key serialization purposes
var CentralKeySerializationQueue     = {};
exports.RouterKeySerializationQueue  = CentralKeySerializationQueue;
exports.StorageKeySerializationQueue = CentralKeySerializationQueue;

// PURPOSE: Add for Centrally received AgentDeltas/NeedMerges
exports.AddToKeySerializationFlow = function(q, ks, op, net, data, auth,
                                             hres, next) {
  var qe = {ks : ks, op : op, net : net, data : data, auth : auth,
            hres : hres, next : next};
  if (ZH.IsUndefined(q[ks.kqk])) q[ks.kqk] = []
  q[ks.kqk].push(qe);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL READ QUEUE --------------------------------------------------------

exports.CentralKeyRead      = new Mutex('CentralKeyRead');
exports.CentralKeyReadQueue = {};
exports.AddToCentralKeyReadFlow = function(q, ks, op, net, data, hres) {
  var qe = {ks : ks, op : op, net : net, data : data, hres : hres};
  if (ZH.IsUndefined(q[ks.kqk])) q[ks.kqk] = []
  q[ks.kqk].push(qe);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL USERNAME QUEUE ----------------------------------------------------

exports.CentralUserRead      = new Mutex('CentralUserRead');
exports.CentralUserReadQueue = {};
exports.AddToCentralUserReadFlow = function(q, username, op, net, data) {
  var qe = {username : username, op : op, net : net, data : data};
  if (ZH.IsUndefined(q[username])) q[username] = []
  q[username].push(qe);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT KEY QUEUE -----------------------------------------------------------

// EXPORT:  AgentKeySerialization
// PURPOSE: Mutex to COMMIT or MERGE a KEY (on the subscriber/author)
//
exports.AgentKeySerialization = new Mutex('AgentKey');

// EXPORT:  AgentKeySerializationQueue
// PURPOSE: Per Key Queue of COMMITs of MERGEs for a KEY (on subscriber/author)
//
exports.AgentKeySerializationQueue = {};

exports.AddToAgentKeyFlow = function(q, ks, op, net, data, auth, hres, next) {
  var qe = {ks : ks, op : op, net : net, data : data, auth : auth,
            hres : hres, next : next};
  if (ZH.IsUndefined(q[ks.kqk])) q[ks.kqk] = []
  q[ks.kqk].push(qe);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER-MASTER PUBLISH TO SUBSCRIBER QUEUE -----------------------------

// EXPORT:  PubSub     
// PURPOSE: Mutex publish deltas (from Central to Agent) to a given subscriber
//
exports.PubSub       = new Mutex('PubSub');

// EXPORT:  PubSubQueue
// PURPOSE: Per Subscriber Queue of deltas to publish (from Central to AgentS)
//
exports.PubSubQueue  = {};

// EXPORT:  AddToSubscriberQueue()
// PURPOSE: Standard Add() for Per Subscriber Queues
// NOTE:    no (hres,next) these are async per subscriber calls -> no callbacks
//
exports.AddToSubscriberFlow = function(q, duuid, op, data) {
  if (ZH.IsUndefined(q[duuid])) q[duuid] = []
  var qe = {duuid : duuid, op : op, data : data};
  q[duuid].push(qe);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZQ']={} : exports);

