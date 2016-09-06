"use strict";

var ZSM, ZSD, ZAS, ZDack, ZDelt, ZCR, ZCache, ZHB, ZADaem, ZGCReap, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZSM       = require('./zsubscriber_merge');
  ZSD       = require('./zsubscriber_delta');
  ZAS       = require('./zactivesync');
  ZDack     = require('./zdack');
  ZDelt     = require('./zdeltas');
  ZCR       = require('./zcreap');
  ZCache    = require('./zcache');
  ZHB       = require('./zheartbeat');
  ZADaem    = require('./zagent_daemon');
  ZGCReap   = require('./zgc_reaper');
  ZS        = require('./zshared');
  ZH        = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-COMMIT-QUEUE FLOW ---------------------------------------------------

exports.FlowAgentKey = function(plugin, collections, qe, next){
  var op = qe.op;
  var func;
  if        (op === 'SUBSCRIBER_DELTA') {
    func = ZSD.FlowSubscriberDelta;
  } else if (op === 'SUBSCRIBER_MERGE') {
    func = ZSM.FlowSubscriberMerge;
  } else if (op === 'CLIENT_COMMIT') {
    func = ZDelt.FlowCommitDelta;
  } else if (op === 'CLIENT_STATELESS_COMMIT') {
    func = ZDelt.FlowStatelessCommitDelta;
  } else if (op === 'CLIENT_MEMCACHE_COMMIT') {
    func = ZDelt.FlowMemcacheCommitDelta;
  } else if (op === 'COMMIT_DELTA') {
    func = ZAS.FlowSubscriberCommitDelta;
  } else if (op === 'CLIENT_FETCH') {
    func = ZDelt.FlowAgentFetch;
  } else if (op === 'AGENT_CACHE') {
    func = ZCache.FlowAgentCache;
  } else if (op === 'AGENT_EVICT') {
    func = ZCache.FlowAgentEvict;
  } else if (op === 'AGENT_LOCAL_EVICT') {
    func = ZCache.FlowAgentLocalEvict;
  } else if (op === 'DRAIN_KEY_VERSIONS') {
    func = ZADaem.FlowDrainKeyVersions;
  } else if (op === 'ACK_AD_ERROR') {
    func = ZDack.FlowAckAgentDeltaError;
  } else if (op === 'HEARTBEAT') {
    func = ZHB.FlowHeartbeat;
  } else if (op === 'CACHE_REAP') {
    func = ZCR.FlowDoReapEvict;
  } else if (op === 'AGENT_GCV_REAP') {
    func = ZGCReap.FlowDoAgentGCVReap;
  } else throw(new Error('PROGRAM ERROR(FlowAgentKey)'));
  func(plugin, collections, qe, next);
}

exports.GetFlowAgentKeyId = function(qe) {
  return qe.ks.kqk;
}

// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZAF']={} : exports);

