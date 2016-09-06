"use strict";

var ZXact, ZAS, ZSD, ZADaem, ZDelt, ZOplog, ZDS, ZFix, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZXact  = require('./zxaction');
  ZAS    = require('./zactivesync');
  ZSD    = require('./zsubscriber_delta');
  ZADaem = require('./zagent_daemon');
  ZDelt  = require('./zdeltas');
  ZOplog = require('./zoplog');
  ZDS    = require('./zdatastore');
  ZFix   = require('./zfixlog');
  ZAF    = require('./zaflow');
  ZQ     = require('./zqueue');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function do_key_to_sync(net, ks, next) {
  ZH.l('ZDack.do_key_to_sync: K: ' + ks.kqk);
  ZAS.SetAgentSyncKeySignalAgentToSyncKeys(net, ks, next);
}

function set_agent_key_out_of_sync(net, ks, next) {
  ZH.l('ZDack.set_agent_key_out_of_sync: K: ' + ks.kqk);
  var skey = ZS.AgentKeysOutOfSync;
  net.plugin.do_set_field(net.collections.global_coll, skey, ks.kqk, true,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      do_key_to_sync(net, ks, function(aerr, ares) {
        if (aerr) next(aerr, null);
        else      next(new Error(ZS.Errors.DeltaNotSync), null);
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EMPTY DELTA ---------------------------------------------------------------

function save_empty_agent_delta(net, auth, ks, avnum, dentry, next) {
  var avrsn = ZH.CreateAvrsn(ZH.MyUUID, avnum);
  var dkey  = ZS.GetAgentKeyDeltaVersions(ks.kqk);
  net.plugin.do_push(net.collections.delta_coll, dkey, "aversions", avrsn,
  function(perr, pres) {
    if (perr) next(perr, null);
    else {
      ZDS.StoreAgentDelta(net.plugin, net.collections, ks, dentry, auth,
      function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          var now  = ZH.GetMsTime();
          var dkey = ZS.AgentDirtyDeltas;
          var dval = ZS.GetDirtyAgentDeltaKey(ks.kqk, ZH.MyUUID, avrsn);
          net.plugin.do_set_field(net.collections.global_coll,
                                  dkey, dval, now, next);
        }
      });
    }
  });
}

function cleanup_empty_delta(dentry) {
  delete(dentry.delta._meta.author);
  delete(dentry.delta._meta.is_geo);
}

function build_create_empty_delta(net, ks, auuid, cavrsn,
                                  fcrdt, delta, dvrsn, next) {
  var ts             = ZH.GetMsTime();
  var ncnt           = 1;
  dvrsn              = ZXact.UpdateCrdtMetaAndAdded(fcrdt, auuid,
                                                    ncnt, dvrsn, ts)
  var perr           = ZXact.FinalizeDeltaForCommit(fcrdt, delta, dvrsn, ts);
  var dentry         = ZH.CreateDentry(delta, ks);
  cleanup_empty_delta(dentry)
  dentry._           = auuid;
  var meta           = dentry.delta._meta;
  meta.initial_delta = false;
  meta.author        = {agent_uuid    : auuid,
                        agent_version : cavrsn};
  //ZH.e('empty dentry'); ZH.e(dentry);
  next(null, dentry);
}

function create_empty_delta(net, ks, auuid, cavnum, next) {
  var cavrsn = ZH.CreateAvrsn(auuid, cavnum);
  ZH.e('empty_delta: K: ' + ks.kqk + ' AV: ' + cavrsn);
  var oplog  = [];
  ZDS.ForceRetrieveCrdt(net.plugin, net.collections, ks, function(ferr, fcrdt) {
    if (ferr) next(ferr, null);
    else {
      ZOplog.CreateDelta(fcrdt, oplog, function(verr, delta) {
        if (verr) next(verr, null);
        else {
          var ncnt             = 1;
          delta._meta.op_count = ncnt;
          ZXact.ReserveOpIdRange(net.plugin, net.collections, auuid, ncnt,
          function(uerr, dvrsn) {
            if (uerr) next(uerr, null);
            else {
              build_create_empty_delta(net, ks, auuid, cavrsn,
                                       fcrdt, delta, dvrsn, next);
            }
          });
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ACK AGENT DELTA -----------------------------------------------------------

exports.GetRemoveAgentDeltaID = function(pc) {
  return pc.ks.kqk + '-' + pc.extra_data.author.agent_version;
}

function remove_dirty_key(net, pc, next) {
  var ks    = pc.ks;
  var avrsn = pc.extra_data.author.agent_version;
  var adkey = ZS.AgentDirtyKeys;
  var adval = pc.metadata.dirty_key - 1;
  ZH.DecrementSet(net.plugin, net.collections.global_coll, adkey, ks.kqk, adval,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var dkey = ZS.AgentDirtyDeltas;
      var dval = ZS.GetDirtyAgentDeltaKey(ks.kqk, ZH.MyUUID, avrsn);
      net.plugin.do_unset_field(net.collections.global_coll, dkey, dval, next);
    }
  });
}

function __remove_agent_delta(net, pc, next) {
  var ks    = pc.ks;
  var avrsn = pc.extra_data.author.agent_version;
  ZH.l('ZDack.__remove_agent_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  var dkey  = ZS.GetAgentKeyDeltaVersions(ks.kqk);
  net.plugin.do_pull(net.collections.delta_coll, dkey, "aversions", avrsn,
  function (xerr, xres) {
    if (xerr && xerr.message !== ZS.Errors.ElementNotFound &&
                xerr.message !== ZS.Errors.KeyNotFound) {
      next(xerr, null);
    } else {
      ZDS.RemoveAgentDelta(net.plugin, net.collections, ks, avrsn,
      function(rerr, rres) {
        if (rerr) next(rerr, null);
        else {
          remove_dirty_key(net, pc, next)
        }
      });
    }
  });
}

function do_remove_agent_delta(net, pc, next) {
  var ks    = pc.ks;
  var avrsn = pc.extra_data.author.agent_version;
  ZH.l('ZDack.do_remove_agent_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  __remove_agent_delta(net, pc, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      var dkey  = ZS.AgentSentDeltas;
      var dval  = ZS.GetSentAgentDeltaKey(ks.kqk, avrsn);
      ZH.l('AgentSentDeltas: UNSET: dval: ' + dval);
      net.plugin.do_unset_field(net.collections.global_coll, dkey, dval, next);
    }
  });
}

exports.RetryRemoveAgentDelta = function(plugin, collections, pc, rerr, next) {
  var ks  = pc.ks;
  var net = ZH.CreateNetPerRequest(ZH.Agent);
  ZH.l('ZDack.RetryRemoveAgentDelta: K: ' + ks.kqk);
  var internal = rerr ? false : true;
  do_remove_agent_delta(net, pc, function(serr, sres) {
    if (serr) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(null, null);
    }
  });
}

function add_fixlog_ack_agent_delta(net, pc, next) {
  var ks   = pc.ks;
  var udid = exports.GetRemoveAgentDeltaID(pc);
  ZH.l('add_fixlog_ack_agent_delta: K: ' + ks.kqk + ' UD: ' + udid);
  var ukey = ZS.FixLog;
  net.plugin.do_set_field(net.collections.global_coll, ukey, udid, pc, next);
}

function get_remove_agent_delta_metadata(net, ks, next) {
  var md    = {};
  var adkey = ZS.AgentDirtyKeys;
  net.plugin.do_get_field(net.collections.global_coll, adkey, ks.kqk,
  function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      md.dirty_key = gres ? gres : 1;
      next(null, md);
    }
  });
}

function do_flow_remove_agent_delta(net, ks, author, next) {
  get_remove_agent_delta_metadata(net, ks, function(gerr, md) {
    if (gerr) next(gerr, null);
    else {
      var op    = 'RemoveAgentDelta';
      var edata = {author : author};
      var pc    = ZH.InitPreCommitInfo(op, ks, null, null, edata);
      pc.metadata.dirty_key = md.dirty_key;
      add_fixlog_ack_agent_delta(net, pc, function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          var rfunc = exports.RetryRemoveAgentDelta;
          do_remove_agent_delta(net, pc, function(aerr, ares) {
            if (aerr) rfunc(net.plugin, net.collections, pc, aerr, next);
            else {
              ZFix.RemoveFromFixLog(net.plugin, net.collections, pc, next);
            }
          });
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DELTA ERROR ACKS ----------------------------------------------------

function drain_key_delta(net, ks, next) {
  ZH.l('drain_key_delta: K: ' + ks.kqk);
  ZADaem.GetAgentKeyDrain(net, ks, true, next);
}

function handle_server_rejected_delta(net, ks, cavrsn, auth, next) {
  var cavnum = ZH.GetAvnum(cavrsn);
  ZH.e('handle_server_rejected_delta: AV: ' + cavnum);
  create_empty_delta(net, ks, ZH.MyUUID, cavnum, function(cerr, dentry) {
    if (cerr) next(cerr, null);
    else {
      save_empty_agent_delta(net, auth, ks, cavnum, dentry,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          do_key_to_sync(net, ks, function(aerr, ares) {
            if (aerr) next(aerr, null);
            else      drain_key_delta(net, ks, next);
          });
        }
      });
    }
  });
}

function handle_ooo_agent_delta_ack(net, ks, next) {
  ZH.l('AckAgentDelta: OOO-DELTA: K: ' + ks.kqk);
  drain_key_delta(net, ks, next);
}

function do_flow_ack_agent_delta_error(net, data, ri, next) {
  if (ZH.IsUndefined(data.error)) {
    next(new Error(ZS.Errors.MalformedAckAgentDelta), null);
  } else if (data.error.code !== -32006) {
      ZH.e('-ERROR: AgentDelta-ACK is MALFORMED'); ZH.e(data.error);
      next(null, null);
  } else {
    if (data.error.message === ZS.Errors.RepeatDelta) {
      var details = data.error.details;
      var ks      = details.ks;
      var author  = details.author;
      do_flow_remove_agent_delta(net, ks, author, next);
    } else if (data.error.message === ZS.Errors.DeltaNotSync) {
      var details = data.error.details;
      var ks      = details.ks;
      var author  = details.author;
      do_flow_remove_agent_delta(net, ks, author, function(rerr, rres) {
        if (rerr) next(rerr, null);
        else      set_agent_key_out_of_sync(net, ks, next);
      });
    } else if (data.error.message === ZS.Errors.OutOfOrderDelta) {
      var details = data.error.details;
      var ks      = details.ks;
      handle_ooo_agent_delta_ack(net, ks, next)
    } else if (data.error.message === ZS.Errors.ServerFilterFailed ||
               data.error.message === ZS.Errors.WritePermsFail) {
      var details = data.error.details;
      var ks      = details.ks;
      var cavrsn  = details.central_agent_version;
      var auth    = details.authorization;
      handle_server_rejected_delta(net, ks, cavrsn, auth, next);
      // NOTE: ZS.EngineCallback.DeltaFailure() is ASYNC
      if (ZS.EngineCallback.DeltaFailure) {
        var reason = details.reason ? details.reason : data.error.message;
        ZS.EngineCallback.DeltaFailure(ks, details.reason);
      }
    } else {
      ZH.e('Unhandled AckAgentDeltaError: ecode(-32006)'); ZH.e(data.error);
      next(null, null);
    }
  }
}

//TODO FIXLOG
exports.FlowAckAgentDeltaError = function(plugin, collections, qe, next) {
  var net    = qe.net;
  var qnext  = qe.next; // NOTE: not used
  var ks     = qe.ks;
  ZH.l('FlowAckAgentDeltaError: K: ' + ks.kqk);
  var data   = qe.data;
  do_flow_ack_agent_delta_error(net, data.edata, data.ri,
  function(serr, sres) {
    if (serr) ZH.e(serr.message); // NOTE: no throws in FLOWs
    next(null, null);
  });
}

exports.HandleAckAgentDeltaError = function(net, data, ri) {
  if (!data.error.details) {
    ZH.e('HandleAckAgentDeltaError: NO DETAILS'); ZH.e(data);
    return;
  }
  var details = data.error.details;
  var ks      = details.ks;
  ZH.l('ZDack.HandleAckAgentDeltaError: K: ' + ks.kqk);
  var data = {edata : data, ri : ri};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'ACK_AD_ERROR',
                       net, data, null, null, ZH.OnErrLog);
  var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL-VERSION GET/SET ---------------------------------------------------

exports.GetAgentAllCreated = function(plugin, collections, next) {
  var akey = ZS.AgentLastCreated;
  plugin.do_get(collections.global_coll, akey, next);
}

exports.GetAgentLastCreated = function(plugin, collections, next) {
  var akey  = ZS.AgentLastCreated;
  var fname = ZH.MyDataCenter;
  plugin.do_get_field(collections.global_coll, akey, fname, next);
}

function set_agent_createds(plugin, collections, createds, next) {
  ZH.AgentLastCentralCreated = createds;
  var akey = ZS.AgentLastCreated;
  plugin.do_set(collections.global_coll, akey, createds, next); 
}

exports.SetAgentCreateds = function(plugin, collections, createds, next) {
  var nc = createds ? Object.keys(createds).length : 0;
  if (nc === 0) next(null, null);
  else {
    var akey = ZS.AgentLastCreated;
    plugin.do_get(collections.global_coll, akey, function(gerr, gres) {
      if (gerr) next(gerr, null);
      else {
        if (gres.length === 0) {
          ZH.l('INIT: ZDack.SetAgentCreateds'); ZH.p(createds);
          set_agent_createds(plugin, collections, createds, next);
        } else {
          var screateds = gres[0];
          for (var guuid in screateds) {
            var screated = screateds[guuid];
            var created  = createds [guuid];
            if (created && created > screated) screateds[guuid] = created;
          }
          for (var guuid in createds) {
            var created  = createds [guuid];
            var screated = screateds[guuid];
            if (!screated) screateds[guuid] = created;
          }
          delete(screateds._id);
          ZH.l('ZDack.SetAgentCreateds'); ZH.p(screateds);
          set_agent_createds(plugin, collections, screateds, next);
        }
      }
    });
  }
}
          

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZAS.check_flow_subscriber_commit_delta()
exports.DoFlowRemoveAgentDelta = function(net, ks, author, next) {
  do_flow_remove_agent_delta(net, ks, author, next);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZDack']={} : exports);

