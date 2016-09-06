"use strict";

var ZADaem, ZSD, ZDack, ZCache, ZOOR, ZFix, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZADaem = require('./zagent_daemon');
  ZSD    = require('./zsubscriber_delta');
  ZDack  = require('./zdack');
  ZCache = require('./zcache');
  ZOOR   = require('./zooo_replay');
  ZFix   = require('./zfixlog');
  ZAF    = require('./zaflow');
  ZQ     = require('./zqueue');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER COMMIT DELTA ---------------------------------------------------

exports.GetSubscriberCommitID = function(pc) {
  var ks     = pc.ks;
  var author = pc.extra_data.author;
  var avrsn  = author.agent_version;
  return ks.kqk + '-' + avrsn;
}

exports.CmpSubscriberCommitID = function(pca, pcb) {
  var aarr   = pca.id.split('-');
  var akqk   = aarr[0];
  var aguuid = aarr[1];
  var aavrsn = aarr[2];
  var barr   = pcb.id.split('-');
  var bkqk   = barr[0];
  var bguuid = barr[1];
  var bavrsn = barr[2];
  if      (akqk   !== bkqk)   return (akqk   > bkqk)   ? 1 : -1;
  else if (aguuid !== bguuid) return (aguuid > bguuid) ? 1 : -1;
  else {
    return (aavrsn === bavrsn) ? 0 : (aavrsn > bavrsn) ? 1 : -1;
  }
}

function do_subscriber_commit_delta(net, pc, next) {
  var ks     = pc.ks;
  var author = pc.extra_data.author;
  var avrsn  = author.agent_version;
  var pkey   = ZS.GetPersistedSubscriberDelta(ks, author);
  net.plugin.do_remove(net.collections.delta_coll, pkey, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var dkey = ZS.GetAgentKeyDeltaVersions(ks.kqk);
      net.plugin.do_pull(net.collections.delta_coll, dkey, "aversions", avrsn,
      function (xerr, xres) {
        if (xerr && xerr.message !== ZS.Errors.ElementNotFound &&
                    xerr.message !== ZS.Errors.KeyNotFound) {
          next(xerr, null);
        } else {
          ZOOR.RemoveSubscriberDelta(net.plugin, net.collections, ks, author,
          function(rerr, rres) {
            if (rerr) next(rerr, null);
            else {
              var dkey = ZS.AgentDirtyDeltas;
              var dval = ZS.GetDirtySubscriberDeltaKey(ks.kqk, author);
              net.plugin.do_unset_field(net.collections.global_coll,
                                        dkey, dval, next);
            }
          });
        }
      });
    }
  });
}

exports.RetrySubscriberCommitDelta = function(plugin, collections,
                                              pc, rerr, next) {
  var ks  = pc.ks;
  var net = ZH.CreateNetPerRequest(ZH.Agent);
  ZH.l('ZAS.RetrySubscriberCommitDelta: K: ' + ks.kqk);
  var internal = rerr ? false : true;
  do_subscriber_commit_delta(net, pc, function(serr, sres) {
    if (serr) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(null, null);
    }
  });
}

function add_fixlog_subscriber_commit_delta(net, pc, next) {
  var ks   = pc.ks;
  var udid = exports.GetSubscriberCommitID(pc);
  ZH.l('add_fixlog_subcommit_delta: K: ' + ks.kqk + ' UD: ' + udid);
  var ukey = ZS.FixLog;
  net.plugin.do_set_field(net.collections.global_coll, ukey, udid, pc, next);
}

function do_flow_subscriber_commit_delta(net, ks, author, next) {
  if (ZH.ChaosMode === 35) {
    ZH.e('CommitDelta -> NO-OP: CHAOS-MODE: ' + ZH.ChaosDescriptions[35]);
    return next(null, null);
  }
  ZH.l('do_flow_subscriber_commit_delta: K: ' + ks.kqk);
  var op    = 'SubscriberCommitDelta';
  var edata = {author : author};
  var pc    = ZH.InitPreCommitInfo(op, ks, null, null, edata);
  add_fixlog_subscriber_commit_delta(net, pc, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var rfunc = exports.RetrySubscriberCommitDelta;
      do_subscriber_commit_delta(net, pc, function(aerr, ares) {
        if (aerr) rfunc(net.plugin, net.collections, pc, aerr, next);
        else      ZFix.RemoveFromFixLog(net.plugin, net.collections, pc, next);
      });
    }
  });
}

exports.DoCommitDelta = function(net, ks, author, next) {
  ZH.l('ZAS.DoCommitDelta: K: ' + ks.kqk + ' AV: ' + author.agent_version);
  if (author.agent_uuid === ZH.MyUUID) {
    ZDack.DoFlowRemoveAgentDelta(net, ks, author, next);
  } else {
    do_flow_subscriber_commit_delta(net, ks, author, next);
  }
}

exports.PostApplyDeltaConditionalCommit= function(net, ks, dentry, next) {
  var author = dentry.delta._meta.author;
  var ckey   = ZS.GetDeltaCommitted(ks, author);
  net.plugin.do_get_field(net.collections.delta_coll, ckey, "value",
  function(gerr, committed) {
    if (gerr) next(gerr, null);
    else {
      if (!committed) next(null, null);
      else            exports.DoCommitDelta(net, ks, author, next);
    }
  });
}

function set_delta_committed(net, ks, author, next) {
  var ckey = ZS.GetDeltaCommitted(ks, author);
  net.plugin.do_set_field(net.collections.delta_coll,
                          ckey, "value", true, next);
}

function subscriber_commit_later(net, ks, author, reason, next) {
  var avrsn = author.agent_version;
  ZH.e('SD-COMMIT-DELTA ' + reason + ': K: ' + ks.kqk + ' AV: ' + avrsn);
  set_delta_committed(net, ks, author, next);
}

function check_flow_subscriber_commit_delta(net, ks, author, next) {
  ZH.l('check_flow_subscriber_commit_delta: K: ' + ks.kqk);
  var skey = ZS.AgentKeysToSync;
  net.plugin.do_get_field(net.collections.global_coll, skey, ks.kqk,
  function(gerr, tosync) {
    if (gerr) next(gerr, null);
    else {
      if (tosync) {
        subscriber_commit_later(net, ks, author, 'ON TO-SYNC-KEY', next);
      } else {
        var fkey = ZS.GetDeltaNeedReference(ks, author);
        net.plugin.do_get_field(net.collections.oood_coll,
                                fkey, "reference_author",
        function(gerr, rfauthor) {
          if (gerr) next(gerr, null);
          else {
            if (rfauthor) {
              subscriber_commit_later(net, ks, author,
                                      'DURING AGENT-GC-WAIT', next);
            } else {
              var okey = ZS.GetOOODelta(ks, author);
              net.plugin.do_get_field(net.collections.delta_coll, okey, "value",
              function(oerr, oooav) {
                if (oerr) next(oerr, null);
                else {
                  if (oooav) {
                    subscriber_commit_later(net, ks, author, 'ON OOO-SD', next);
                  } else {
                    ZSD.FetchSubscriberDelta(net.plugin, net.collections,
                                             ks, author,
                    function(gerr, dentry) {
                      if (gerr) next(gerr, null);
                      else {
                        if (!dentry) {
                          subscriber_commit_later(net, ks, author,
                                                  'DELTA NOT ARRIVED', next);
                        } else {
                          var avrsn = author.agent_version;
                          var dkey  = ZS.GetAgentDeltaGCVNeedsReorder(ks);
                          net.plugin.do_get_field(net.collections.global_coll,
                                                  dkey, avrsn,
                          function(nerr, needs) {
                            if (nerr) next(nerr, null);
                            else {
                              if (needs) {
                                subscriber_commit_later(net, ks, author,
                                                        'KEY-GCV-NREO', next);
                              } else {
                                exports.DoCommitDelta(net, ks, author, next)
                              }
                            }
                          });
                        }
                      }
                    });
                  }
                }
              });
            }
          }
        });
      }
    }
  });
}

exports.FlowSubscriberCommitDelta = function(plugin, collections, qe, next) {
  var net     = qe.net;
  var qnext   = qe.next;
  var ks      = qe.ks;
  var data    = qe.data;
  var author  = data.author;
  check_flow_subscriber_commit_delta(net, ks, author, function(serr, sres) {
    qnext(serr, null);
    next(null, null);
  });
}

function start_commit_delta(net, ks, author, next) {
  var data = {author : author};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'COMMIT_DELTA',
                       net, data, null, null, next);
  var flow = {k : ks.kqk,         q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

exports.HandleSubscriberCommitDelta = function(net, ks, author, next) {
  if (ZH.ChaosMode === 22) {
    return next(new Error(ZH.ChaosDescriptions[22]), null);
  }
  start_commit_delta(net, ks, author, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SYNC CHANNEL --------------------------------------------------------

exports.SetAgentSyncKey = function(plugin, collections, ks, next) {
  ZH.l('SetAgentSyncKey: K: ' + ks.kqk + ' TOK: ' + ks.security_token);
  var skey = ZS.AgentKeysToSync;
  plugin.do_set_field(collections.global_coll,
                      skey, ks.kqk, ks.security_token, next);
}

exports.SetAgentSyncKeySignalAgentToSyncKeys = function(net, ks, next) {
  exports.SetAgentSyncKey(net.plugin, net.collections, ks,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      ZADaem.SignalAgentToSyncKeys();
      next(null, null);
    }
  });
}

function do_sync_missing_key(net, ks, next) {
  net.plugin.do_get_field(net.collections.lru_coll, ks.kqk, "when",
  function(gerr, when) {
    if (gerr) next(gerr, null);
    else {
      if (when) { // ALREADY PRESENT
        ZH.l('already synced: K: ' + ks.kqk + ' -> NO-OP');
        next(null, null);
      } else {    // MISSING -> SYNC it from Central
        exports.SetAgentSyncKey(net.plugin, net.collections, ks, next);
      }
    }
  });
}

function sync_missing_key(net, ks, issub, next) {
  if (!issub) do_sync_missing_key(net, ks, next); // STATION PKSS[]
  else {
    var dk_key = ZS.GetDeviceToKeys(ZH.MyUUID);
    net.plugin.do_get_field(net.collections.dtok_coll, dk_key, ks.kqk,
    function (uerr, cached) {
      if (uerr) next(uerr, null);
      else {
        if (!cached) do_sync_missing_key(net, ks, next);
        else {
          ZH.l('sync_missing_key: K: ' + ks.kqk + ' -> EVICT');
          ZCache.AgentEvict(net, ks, true, {}, function(serr, sres) {
            if (serr) next(serr, null);
            else      do_sync_missing_key(net, ks, next);
          });
        }
      }
    });
  }
}

function sync_missing_keys(net, kss, issub, next) {
  if (kss.length === 0) {
    next(null, null);
    // NOTE: BELOW is ASYNC
    ZADaem.SignalAgentToSyncKeys();
  } else {
    var ks = kss.shift();
    sync_missing_key(net, ks, issub, function(aerr, ares) {
      if (aerr) next(aerr, null);
      else {
        setImmediate(sync_missing_keys, net, kss, issub, next);
      }
    });
  }
}

exports.AgentSyncChannelKss = function(net, kss, issub, hres, next) {
  ZH.l('ZAS.AgentSyncChannelKss: #Ks: ' + kss.length);
  if (kss.length === 0) next(null, hres);
  else {
    sync_missing_keys(net, kss, issub, function(serr, sres) {
      next(serr, hres);
    });
  }
}

// NOTE: ZAS.AgentHandleUnsyncChannel:
//         for now, NO-OP, figuring out which keys to stop_syncing
//         taking into account multiple rchans per doc and
//                             multiple subscriptions per user and
//                             multiple stationed-users per device
//         plus keys that are both subscribed to and cached
//         is a headache for a later date
//
exports.AgentHandleUnsyncChannel = function(plugin, collections, schanid,
                                            hres, next) {
  ZH.l('ZAS.AgentHandleUnsyncChannel: R: ' + schanid);
  next(null, hres);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZAS']={} : exports);


