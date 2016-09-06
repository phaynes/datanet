"use strict";

var ZDelt, ZNM, ZPio, ZMerge, ZSM, ZADaem, ZDack, ZConv, ZCR, ZChannel, ZAio;
var ZAuth, ZFix, ZMDC, ZDQ, ZDS, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZDelt    = require('./zdeltas');
  ZNM      = require('./zneedmerge');
  ZPio     = require('./zpio');
  ZMerge   = require('./zmerge');
  ZSM      = require('./zsubscriber_merge');
  ZADaem   = require('./zagent_daemon');
  ZDack    = require('./zdack');
  ZConv    = require('./zconvert');
  ZCR      = require('./zcreap');
  ZChannel = require('./zchannel');
  ZAio     = require('./zaio');
  ZAuth    = require('./zauth');
  ZFix     = require('./zfixlog');
  ZMDC     = require('./zmemory_data_cache');
  ZDQ      = require('./zdata_queue');
  ZDS      = require('./zdatastore');
  ZAF      = require('./zaflow');
  ZQ       = require('./zqueue');
  ZS       = require('./zshared');
  ZH       = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

var DebugEvictMergeRace      = false;
var DebugEvictMergeRaceSleep = 2000;

function send_delayed_agent_merge(net, ks) {
  var to = DebugEvictMergeRaceSleep;
  ZH.e('DEEP-DEBUG: send_delayed_agent_merge: K: ' + ks.kqk + ' SLEEP: ' + to);
  setTimeout(function() {
    ZH.e('DEEP-DEBUG: SEND: send_delayed_agent_merge: K: ' + ks.kqk);
    ZADaem.NeedMerge(net.plugin, net.collections, ks, ZH.OnErrLog);
  }, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function set_key_watch(net, ks, duuid, is_agent, hres, next) {
  ZH.l('set_key_watch: K: ' + ks.kqk + ' U: ' + duuid);
  ZMDC.SetAgentWatchKeys(net.plugin, net.collections, ks, duuid,
  function(werr, wres) {
    if (werr) next(werr, hres);
    else {
      if (!is_agent) next(null, hres);
      else {
        net.plugin.do_set_field(net.collections.lru_coll, ks.kqk, "watch", true,
        function(serr, sres) {
          next(serr, hres);
        });
      }
    }
  });
}

function store_cache_key_metadata(net, ks, watch, duuid, hres, next) {
  ZH.l('store_cache_key_metadata: K: ' + ks.kqk + ' U: ' + duuid);
  ZMDC.SetKeyToDevices(net.plugin, net.collections, ks, duuid,
  function (kerr, kres) {
    if (kerr) next(kerr, hres);
    else {
      var dk_key = ZS.GetDeviceToKeys(duuid);
      net.plugin.do_set_field(net.collections.dtok_coll, dk_key, ks.kqk, true,
      function (derr, dres) {
        if (derr) next(derr, hres);
        else {
          if (watch) set_key_watch(net, ks, duuid, true, hres, next);
          else {
            ZMDC.RemoveAgentWatchKeys(net.plugin, net.collections, ks, duuid,
            function(werr, wres) {
              next(werr, hres);
            });
          }
        }
      });
    }
  });
}

function remove_cache_key_metadata(net, ks, duuid, hres, next) {
  ZH.l('remove_cache_key_metadata: ' + ks.kqk + ' duuid: ' + duuid);
  ZMDC.UnsetKeyToDevices(net.plugin, net.collections, ks, duuid,
  function (kerr, kres) {
    if (kerr) next(kerr, hres);
    else {
      var dk_key = ZS.GetDeviceToKeys(duuid);
      net.plugin.do_unset_field(net.collections.dtok_coll, dk_key, ks.kqk,
      function (derr, dres) {
        next(derr, hres);
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HELPERS -------------------------------------------------------------

function agent_store_cache_key_metadata(net, ks, watch, auth, next) {
  var username = auth.username;
  ZH.l('agent_store_cache_key_metadata: K: ' + ks.kqk + ' UN: ' + username);
  store_cache_key_metadata(net, ks, watch, ZH.MyUUID, {}, function(kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      var auk_key = ZS.GetAgentUserToCachedKeys(username);
      net.plugin.do_set_field(net.collections.key_coll, auk_key, ks.kqk, true,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          var aku_key = ZS.GetAgentCachedKeyToUsers(ks);
          net.plugin.do_set_field(net.collections.key_coll,
                                  aku_key, username, true,
          function(aerr, ares) {
            if (aerr) next(aerr, null);
            else {
              var ekey = ZS.GetEvicted(ks);
              net.plugin.do_remove(net.collections.global_coll, ekey,
              function(rerr, rres) {
                if (rerr) next(rerr, null);
                else      ZCR.SetLruLocalRead(net, ks, next);
              });
            }
          });
        }
      });
    }
  });
}

function agent_remove_cache_key_users(net, unames, ks, next) {
  if (unames.length === 0) next(null, null);
  else {
    var username = unames.shift();
    ZH.l('agent_remove_cache_key_users: K: ' + ks.kqk + ' UN: ' + username);
    var auk_key = ZS.GetAgentUserToCachedKeys(username);
    net.plugin.do_unset_field(net.collections.key_coll, auk_key, ks.kqk,
    function(uerr, ures) {
      if (uerr) next(uerr, null);
      else {
        setImmediate(agent_remove_cache_key_users, net, unames, ks, next);
      }
    });
  }
}

function agent_remove_cache_key_user_to_key(net, ks, next) {
  var aku_key = ZS.GetAgentCachedKeyToUsers(ks);
  net.plugin.do_get(net.collections.key_coll, aku_key, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var usernames = gres[0];
      delete(usernames._id);
      var unames    = [];
      for (var n in usernames) unames.push(n);
      agent_remove_cache_key_users(net, unames, ks, function(rerr, rres) {
        if (rerr) next(rerr, null);
        else {
          net.plugin.do_remove(net.collections.key_coll, aku_key, next);
        }
      });
    }
  });
}

function agent_remove_cache_key_metadata(net, ks, next) {
  remove_cache_key_metadata(net, ks, ZH.MyUUID, {}, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      ZMDC.RemoveAgentWatchKeys(net.plugin, net.collections, ks, ZH.MyUUID,
      function(kerr, kres) {
        if (kerr) next(kerr, null);
        else      agent_remove_cache_key_user_to_key(net, ks, next);
      });
    }
  });
}

function do_agent_remove_subscriber_version(net, ks, auuid, avrsn, next) {
  var skey = ZS.GetDeviceSubscriberVersion(auuid);
  net.plugin.do_unset_field(net.collections.key_coll, skey, ks.kqk,
  function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      var kkey = ZS.GetPerKeySubscriberVersion(ks.kqk);
      net.plugin.do_unset_field(net.collections.key_coll, kkey, auuid, next);
      //TODO ZDS.RemoveSubscriberDelta???
    }
  });
}

function agent_remove_subscriber_version(net, ks, avrsns, next) {
  if (avrsns.length === 0) next(null, null);
  else {
    var avrsn = avrsns.shift();
    var auuid = ZH.GetAvUUID(avrsn);
    do_agent_remove_subscriber_version(net, ks, auuid, avrsn,
    function(rerr, rres) {
      if (rerr) next(rerr, null);
      else {
        setImmediate(agent_remove_subscriber_version, net, ks, avrsns, next);
      }
    });
  }
}

// TODO SubscriberVersions gotten from AckAgentNeedMerge need removal
function agent_remove_subscriber_versions(net, ks, next) {
  var dkey = ZS.GetAgentKeyDeltaVersions(ks.kqk);
  net.plugin.do_get_array_values(net.collections.delta_coll, dkey, "aversions",
  function(verr, avrsns) {
    if (verr) next(verr, null);
    else {
      if (!avrsns) avrsns = [];
      agent_remove_subscriber_version(net, ks, avrsns, function(rerr, rres) {
        if (rerr) next(rerr, null);
        else      net.plugin.do_remove(net.collections.delta_coll, dkey, next);
      });
    }
  });
}

function agent_remove_key_metadata(net, ks, next) {
  ZMDC.RemoveKeyToDevices(net.plugin, net.collections, ks,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      var mkey = ZS.GetAgentKeyMaxGCVersion(ks.kqk);
      net.plugin.do_remove(net.collections.key_coll, mkey,
      function(rerr, rres) {
        if (rerr) next(rerr, null);
        else {
          ZMDC.RemoveGCVersion(net.plugin, net.collections, ks,
          function(serr, sres) {
            if (serr) next(serr, null);
            else {
              ZMDC.RemoveKeyRepChans(net.plugin, net.collections, ks,
              function(kerr, kres) {
                if (kerr) next(kerr, null);
                else {
                  agent_remove_subscriber_versions(net, ks,
                  function(verr, cres) {
                    if(verr) next(verr, null);
                    else {
                      net.plugin.do_remove(net.collections.lru_coll,
                                           ks.kqk, next);
                    }
                  });
                }
              });
            }
          });
        }
      });
    }
  });
}

function check_already_subscribed(net, auth, rchans, next) {
  if (!rchans) next(null, false);
  else {
    ZChannel.GetUserSubscriptions(net.plugin, net.collections, auth,
    function(gerr, csubs) {
      if (gerr) next(gerr, hres);
      else {
        var hit = false;
        for (var schanid in csubs) {
          for (var i = 0; i < rchans.length; i++) {
            var rchan = rchans[i];
            if (typeof(rchan) == "number") rchan = String(rchan);
            if (rchan === schanid) {
              hit = true;
              break;
            }
          }
          if (hit == true) break;
        }
        ZH.l('check_already_subscribed: HIT: ' + hit);
        next(null, hit);
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INFO ----------------------------------------------------------------------

exports.AgentIsCached = function(plugin, collections, ks, auth, hres, next) {
  ZMDC.GetKeyToDevices(plugin, collections, ks, ZH.MyUUID,
  function (kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      hres.cached = kres ? true : false;
      next(null, hres);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT CACHE ---------------------------------------------------------------

function set_wildcard_perms(net, rchans, perms, next) {
  if (rchans.length === 0) next(null, null);
  else {
    var rchan = rchans.shift();
    var wkey  = ZS.GetUserChannelPermissions(ZH.WildCardUser);
    net.plugin.do_set_field(net.collections.uperms_coll, wkey, rchan, perms,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        setImmediate(set_wildcard_perms, net, rchans, perms, next);
      }
    });
  }
}

function check_wildcard_perms(net, crdt, cperms, auth, next) {
  if (cperms !== "R*" && cperms !== "W*") next(null, null);
  else {
    var crchans = ZH.clone(crdt._meta.replication_channels);
    var perms   = (cperms === "R*") ? "R" : "W";
    set_wildcard_perms(net, crchans, perms, next);
  }
}

function cache_process_subscriber_merge(net, md, hres, next) {
  ZSM.DoProcessSubscriberMerge(net, md.ks, md.crdt, md.central_agent_versions,
                               md.gc_summary, md.ether, md.remove,
                               true, hres, next);
}

function do_agent_send_central_cache(net, ks, pin, watch, auth, hres, next) {
  ZAio.SendCentralCache(net.plugin, net.collections, ks, watch, auth,
  function(cerr, dres) {
    if (cerr) next(cerr, hres);
    else {
      var md = dres.merge_data;
      if (!md.crdt) next(new Error(ZS.Errors.NoDataFound), hres);
      else {
        ZH.l('<<<<(C): HandleCentralAckCache: K: ' + ks.kqk);
        agent_store_cache_key_metadata(net, ks, watch, auth,
        function(kerr, kres) {
          if (kerr) next(kerr, hres);
          else {
            var crdt    = md.crdt;
            var created = crdt._meta.created
            ZDack.SetAgentCreateds(net.plugin, net.collections, created,
            function(aerr, ares) {
              if (aerr) next(aerr, ares);
              else {
                var perms = md.permissions;
                check_wildcard_perms(net, crdt, perms, auth,
                function(cerr, cres) {
                  if (cerr) next(cerr, hres);
                  else {
                    var json     = ZH.CreatePrettyJson(crdt);
                    hres.applied = {crdt : crdt, json : json};
                    if (!dres.watch) {
                      cache_process_subscriber_merge(net, md, hres, next);
                    } else {
                      var rchans = crdt._meta.replication_channels;
                      ZMDC.CasKeyRepChans(net.plugin, net.collections,
                                          ks, rchans,
                       function(berr, bres) {
                         if (berr) next(berr, hres);
                         else {
                          hres.watch = true; // Used in response
                          next(null, hres);
                        }
                      });
                    }
                  }
                });
              }
            });
          }
        });
      }
    }
  });
}

function add_crdt_to_applied(crdt, hres, next) {
  var json     = ZH.CreatePrettyJson(crdt);
  hres.applied = {crdt : crdt, json : json};
  next(null, hres);
}

function do_handle_already_cached(net, ks, watch, fcrdt, internal,
                                  auth, hres, next) {
  agent_store_cache_key_metadata(net, ks, watch, auth, function(kerr, kres) {
    if (kerr) next(kerr, hres);
    else {
      if (internal) add_crdt_to_applied(fcrdt, hres, next);
      else {
        if (fcrdt._data) add_crdt_to_applied(fcrdt, hres, next);
        else {
          get_memcache_crdt_data(net, ks, fcrdt, function(gerr, mcrdt) {
            if (gerr) next(gerr, hres);
            else      add_crdt_to_applied(mcrdt, hres, next);
          });
        }
      }
    }
  });
}

function handle_already_cached(net, ks, pin, watch, internal, fcrdt,
                               auth, hres, next) {
  if (!fcrdt) { // MEMCACHE-EVICT
    do_agent_send_central_cache(net, ks, pin, watch, auth, hres, next);
  } else {
    do_handle_already_cached(net, ks, watch, fcrdt, internal, auth, hres, next);
  }
}

function is_key_locally_present(net, ks, next) {
  ZMDC.GetAgentWatchKeys(net.plugin, net.collections, ks, ZH.MyUUID,
  function(gerr, watched) {
    if (gerr) next(gerr, null);
    else {
      if (watched) next(null, false);
      else         next(null, true);
    }
  });
}

function do_handle_cacheable(net, ks, pin, watch, internal, fcrdt,
                             auth, hres, next) {
  var dk_key = ZS.GetDeviceToKeys(ZH.MyUUID);
  net.plugin.do_get_field(net.collections.dtok_coll, dk_key, ks.kqk,
  function (uerr, cached) {
    if (uerr) next(uerr, hres);
    else {
      if (!cached) { // NOT-CACHED
        do_agent_send_central_cache(net, ks, pin, watch, auth, hres, next);
      } else {       // ALREADY-CACHED
        ZH.l('ALREADY-CACHED: K: ' + ks.kqk);
        if (fcrdt) {
          handle_already_cached(net, ks, pin, watch, internal, fcrdt,
                                auth, hres, next);
        } else {
          ZH.l('NOT LOCAL -> SEND_CENTRAL: K: ' + ks.kqk);
          do_agent_send_central_cache(net, ks, pin, watch, auth, hres, next);
        }
      }
    }
  });
}

function do_agent_cache(net, ks, pin, watch, force, internal,
                        auth, hres, next) {
  ZH.l('ZCache.AgentCache: K: ' + ks.kqk + ' P: ' + pin + ' W: ' + watch);
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function(ferr, fcrdt) {
    if (ferr) next(ferr, hres);
    else {
      var rchans = fcrdt ? fcrdt._meta.replication_channels : null;
      check_already_subscribed(net, auth, rchans, function(serr, issubbed) {
        if (serr) next(serr, null);
        else {
          if (issubbed) next(new Error(ZS.Errors.CacheOnSubscribe), null);
          else {
            if (force || watch) {
              do_agent_send_central_cache(net, ks, pin, watch,
                                          auth, hres, next);
            } else {
              do_handle_cacheable(net, ks, pin, watch, internal, fcrdt,
                                  auth, hres, next);
            }
          }
        }
      });
    }
  });
}

function agent_pin_cache_key(net, ks, pin, next) {
  if (!pin) {
    net.plugin.do_unset_field(net.collections.lru_coll, ks.kqk, "pin", next);
  } else {
    net.plugin.do_set_field(net.collections.lru_coll,
                            ks.kqk, "pin", true, next);
  }
}

function agent_sticky_cache_key(net, ks, sticky, next) {
  var kkey = ZS.GetKeyInfo(ks);
  if (!sticky) {
    net.plugin.do_unset_field(net.collections.kinfo_coll, kkey, "sticky", next);
  } else {
    net.plugin.do_set_field(net.collections.kinfo_coll,
                            kkey, "sticky", true, next);
  }
}

function do_flow_agent_cache(net, ks, pin, watch, sticky, force, internal,
                             auth, hres, next) {
  do_agent_cache(net, ks, pin, watch, force, internal, auth, hres,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      agent_pin_cache_key(net, ks, pin, function(perr, pres) {
        if (perr) next(perr, hres);
        else {
          agent_sticky_cache_key(net, ks, sticky, function(aerr, ares) {
            next(aerr, hres);
          });
        }
      });
    }
  });
}

exports.FlowAgentCache = function(plugin, collections, qe, next) {
  var net    = qe.net;
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var ks     = qe.ks;
  var auth   = qe.auth;
  var data   = qe.data;
  var pin    = data.pin;
  var watch  = data.watch;
  var sticky = data.sticky;
  var force  = data.force;
  do_flow_agent_cache(net, ks, pin, watch, sticky, force, false, auth, qhres,
  function(serr, sres) {
    qnext(serr, qhres);
    next (null, null);
  });
}

exports.AgentCache = function(net, ks, pin, watch, sticky, force, internal,
                             auth, hres, next) {
  if (internal) {
    do_flow_agent_cache(net, ks, pin, watch, sticky, force, true,
                        auth, hres, next);
  } else {
    var data = {pin : pin, watch: watch, sticky : sticky, force : force};
    ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'AGENT_CACHE',
                         net, data, auth, hres, next);
    var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
                m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT EVICT ---------------------------------------------------------------

function do_evict_remove_key(net, ks, next) {
  ZSM.SetKeyInSync(net, ks, function(kerr, kres) { // EVICT-MERGE-RACE
    if (kerr) next(kerr, null);
    else {
      var kkey = ZS.GetKeyInfo(ks);
      net.plugin.do_get_field(net.collections.kinfo_coll, kkey, "separate",
      function(gerr, sep) { 
        if (gerr) next(gerr, null);
        else {
          ZDS.RemoveKey(net, ks, sep, function(rerr, rres) {
            if(rerr) next(gerr, null);
            else {
              var ekey = ZS.GetEvicted(ks);
              net.plugin.do_set_field(net.collections.global_coll,
                                      ekey, "value", true, next);
            }
          });
        }
      });
    }
  });
}

function do_agent_evict(net, ks, hres, next) {
  agent_remove_key_metadata(net, ks, function(aerr, ares) {
    if (aerr) next(aerr, hres);
    else {
      agent_remove_cache_key_metadata(net, ks, function(serr, sres) {
        if (serr) next(serr, hres);
        else {
          do_evict_remove_key(net, ks, function(rerr, rres) {
            if (DebugEvictMergeRace) send_delayed_agent_merge(net, ks);
            next(rerr, hres);
          });
        }
      });
    }
  });
}

function do_flow_agent_evict(net, ks, hres, next) {
  var dk_key = ZS.GetDeviceToKeys(ZH.MyUUID);
  net.plugin.do_get_field(net.collections.dtok_coll, dk_key, ks.kqk,
  function (uerr, cached) {
    if (uerr) next(uerr, hres);
    else {
      if (!cached) next(new Error(ZS.Errors.EvictUncached), hres);
      else {
        ZAio.SendCentralEvict(net.plugin, net.collections, ks,
        function(derr, dres) {
          if (derr) next(derr, hres);
          else      do_agent_evict(net, ks, hres, next);
        });
      }
    }
  });
}

exports.FlowAgentEvict = function(plugin, collections, qe, next) {
  var net    = qe.net;
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var ks     = qe.ks;
  do_flow_agent_evict(net, ks, qhres, function(serr, sres) {
    qnext(serr, qhres);
    next (null, null);
  });
}

exports.AgentEvict = function(net, ks, internal, hres, next) {
  if (internal) {
    do_flow_agent_evict(net, ks, hres, next);
  } else {
    var data = {};
    ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'AGENT_EVICT',
                         net, data, ZH.NobodyAuth, hres, next);
    var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
                m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}

function do_flow_agent_local_evict(net, ks, hres, next) {
  ZH.l('ZCache.AgentLocalEvict: K: ' + ks.kqk);
  var dk_key = ZS.GetDeviceToKeys(ZH.MyUUID);
  net.plugin.do_get_field(net.collections.dtok_coll, dk_key, ks.kqk,
  function (uerr, cached) {
    if (uerr) next(uerr, hres);
    else {
      if (!cached) next(new Error(ZS.Errors.EvictUncached), hres);
      else {
        ZAio.SendCentralLocalEvict(net.plugin, net.collections, ks,
        function(derr, dres) {
          if (derr) next(derr, hres);
          else {
            do_evict_remove_key(net, ks, function(rerr, rres) {
              if (rerr) next(rerr, hres);
              else      set_key_watch(net, ks, ZH.MyUUID, true, hres, next);
            });
          }
        });
      }
    }
  });
}

exports.FlowAgentLocalEvict = function(plugin, collections, qe, next) {
  var net    = qe.net;
  var qnext  = qe.next;
  var qhres  = qe.hres;
  var ks     = qe.ks;
  do_flow_agent_local_evict(net, ks, qhres, function(serr, sres) {
    qnext(serr, qhres);
    next (null, null);
  });
}

exports.AgentLocalEvict = function(net, ks, hres, next) {
  if (internal) {
    do_flow_agent_local_evict(net, ks, hres, next);
  } else {
    var data = {};
    ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'AGENT_LOCAL_EVICT',
                         net, data, ZH.NobodyAuth, hres, next);
    var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
                m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MEMCACHE GET CRDT FROM OPLOG & DELTA --------------------------------------

function get_data_from_oplog(net, ks, crdt, oplog, next) {
  if (oplog.length === 0) next(null, crdt);
  else {
    var op   = oplog.shift();
    var path = op.path;
    var res  = path.split(".");
    var root = res[0];
    net.plugin.do_get_crdt_root(net.collections.crdt_coll, ks.kqk, root,
    function (gerr, rval) {
      if (gerr) next(gerr,  null);
      else {
        if (!rval) next(null, null); // MISS - FIELD NOT CACHED
        else {
          crdt._data.V[root] = rval;
          setImmediate(get_data_from_oplog, net, ks, crdt, oplog, next);
        }
      }
    });
  }
}

exports.GetMemcacheCrdtDataFromOplog = function(net, ks, crdt, oplog, next) {
  ZH.l('ZCache.GetMemcacheCrdtDataFromOplog: K: ' + ks.kqk);
  var mcrdt   = ZH.clone(crdt);
  var coplog  = ZH.clone(oplog);
  mcrdt._data = ZConv.AddORSetMember({}, "O", crdt._meta);
  get_data_from_oplog(net, ks, mcrdt, coplog, next);
}

function get_data_from_members(net, ks, crdt, mbrs, next) {
  if (mbrs.length === 0) next(null, crdt);
  else {
    var root = mbrs.shift();
    net.plugin.do_get_crdt_root(net.collections.crdt_coll, ks.kqk, root,
    function (gerr, rval) {
      if (gerr) next(gerr,  null);
      else {
        if (rval) crdt._data.V[root] = rval;
        setImmediate(get_data_from_members, net, ks, crdt, mbrs, next);
      }
    });
  }
}

exports.GetMemcacheCrdtDataFromDelta = function(net, ks, crdt, dentry, next) {
  ZH.l('ZCache.GetMemcacheCrdtDataFromDelta: K: ' + ks.kqk);
  net.plugin.do_get_directory_members(net.collections.crdt_coll, ks.kqk,
  function(gerr, mbrs) {
    if (gerr) next(gerr, null);
    else {
      var mcrdt   = ZH.clone(crdt);
      mcrdt._data = ZConv.AddORSetMember({}, "O", crdt._meta);
      var pmbrs   = ZMerge.GetMembersPresentInDelta(mbrs, dentry.delta);
      get_data_from_members(net, ks, mcrdt, pmbrs, next);
    }
  });
}

function get_memcache_crdt_data(net, ks, crdt, next) {
  ZH.l('get_memcache_crdt_data: K: ' + ks.kqk);
  net.plugin.do_get_directory_members(net.collections.crdt_coll, ks.kqk,
  function(gerr, mbrs) {
    if (gerr) next(gerr, null);
    else {
      var mcrdt   = ZH.clone(crdt);
      mcrdt._data = ZConv.AddORSetMember({}, "O", crdt._meta);
      get_data_from_members(net, ks, mcrdt, mbrs, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL CACHE -------------------------------------------------------------

function get_dominant_permission(rchans, perms) {
  if (!perms) return null;
  var rperms = null;
  for (var i = 0; i < rchans.length; i++) {
    var rchan = rchans[i];
    var p     = perms[rchan];
    if (p) {
      if      (p === "R") return "R"; // R is DOMINANT
      else if (p === "W") rperms = "W";
    }
  }
  return rperms;
}

function get_user_to_key_permissions(net, crdt, auth, hres, next) {
  ZAuth.GetAllUserChannelPermissions(net.plugin, net.collections, auth, {},
  function(perr, pres) {
    if (perr) next(perr, null);
    else {
      var perms  = pres.permissions;
      var rchans = crdt._meta.replication_channels;
      var rperms = get_dominant_permission(rchans, perms);
      if (rperms) {
        hres.merge_data.permissions = rperms;
        next(null, null);
      } else { // NO RCHANS HAVE PERMISSIONS -> WILDCARD
        var wkey = ZS.GetUserChannelPermissions(ZH.WildCardUser);
        net.plugin.do_get(net.collections.uperms_coll, wkey,
        function(gerr, gres) {
          if (gerr) next(gerr, null);
          else {
            var operms = gres[0];
            var rperms = get_dominant_permission(rchans, operms);
            hres.merge_data.permissions = rperms + "*"; // WILDCARD
            next(null, null);
          }
        });
      }
    }
  });
}

exports.HandleStorageClusterCache = function(net, ks, watch, agent, need_body,
                                             auth, hres, next) {
  var suuid = agent.uuid;
  ZH.l('ZCache.HandleStorageClusterCache: K: ' + ks.kqk + ' U: ' + suuid);
  store_cache_key_metadata(net, ks, watch, suuid, hres, function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      ZMDC.InvalidateKeyToDevices(ks);
      if (!need_body) next(null, hres);
      else {
        ZMDC.GetGCVersion(net.plugin, net.collections, ks,
        function(verr, gcv) {
          if (verr) next(verr, null);
          else {
            ZNM.FlowHandleStorageNeedMerge(net, ks, gcv, null, agent, hres,
            function(aerr, ares) {
              if (aerr) next(aerr, hres);
              else {
                var crdt = hres.merge_data.crdt;
                if (!crdt) next(null, null);
                else {
                  get_user_to_key_permissions(net, crdt, auth, hres, next);
                }
              }
            });
          }
        });
      }
    }
  });
}

exports.CentralHandleGeoClusterCache = function(net, ks, watch, agent,
                                                hres, next) {
  hres.ks   = ks; // Used in response
  var suuid = agent.uuid;
  ZH.l('ZCache.CentralHandleGeoClusterCache: K: ' + ks.kqk + ' U: ' + suuid);
  // NOTE: no check for existence, that happend at Origin-DataCenter
  store_cache_key_metadata(net, ks, watch, suuid, hres, next);
  // NOTE: ZDQ.StorageQueueRequestClusterCache() is ASYNC
  ZDQ.StorageQueueRequestClusterCache(net.plugin, net.collections,
                                      ks, watch, agent, false,
                                      ZH.NobodyAuth, hres, ZH.OnErrLog);
  if (!ZH.DisableBroadcastUpdateSubscriberMap) {
    // NOTE: ZPio.BroadcastUpdateSubscriberMap() is ASYNC
    ZPio.BroadcastUpdateSubscriberMap(net, ks, null, null,
                                      agent, watch, true);
  }

}

function get_geo_send(net, ks, watch, suuid, next) {
  ZMDC.GetKeyToDevices(net.plugin, net.collections, ks, suuid,
  function (kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      var cached = kres ? true : false;
      if (!cached) next(null, true);
      else {
        ZMDC.GetAgentWatchKeys(net.plugin, net.collections, ks, suuid,
        function(gerr, watched) {
          if (gerr) next(gerr, null);
          else {
            if (watched) next(null, !watch);
            else         next(null, watch);
          }
        });
      }
    }
  });
}

function handle_central_cacheable(net, ks, watch, agent, auth, hres, next) {
  var suuid = agent.uuid;
  ZH.l('handle_central_cacheable: K: ' + ks.kqk + ' U: ' + suuid);
  get_geo_send(net, ks, watch, suuid, function(gerr, geo_send) {
    if (gerr) next(gerr, null);
    else {
      if (geo_send) { // NOTE: ZPio.GeoBroadcastCache() is ASYNC
        ZPio.GeoBroadcastCache(net.plugin, net.collections, ks, watch, agent);
      }
      next(null, hres);
      var cb = ZH.Central.ClusterCacheCallback; // ASYNC CALLBACK
      // NOTE: ZDQ.StorageQueueRequestClusterCache() is ASYNC
      ZDQ.StorageQueueRequestClusterCache(net.plugin, net.collections,
                                          ks, watch, agent, true,
                                          auth, hres, cb);
    }
  });
}

exports.CentralHandleClusterCache = function(net, ks, watch, agent,
                                             auth, hres, next) {
  hres.ks    = ks;    // Used in response
  hres.watch = watch; // Used in response
  var suuid  = agent.uuid;
  ZH.l('ZCache.CentralHandleClusterCache: K: ' + ks.kqk + ' U: ' + suuid);
  ZMDC.GetKeyRepChans(net.plugin, net.collections, ks, function(ferr, orchans) {
    if (ferr) next(ferr, hres);
    else {
      if (!orchans) next(new Error(ZS.Errors.NoDataFound), hres);
      else {
        check_already_subscribed(net, auth, orchans, function(serr, issubbed) {
          if (serr) next(serr, null);
          else {
            if (issubbed) next(new Error(ZS.Errors.CacheOnSubscribe), null);
            else {
              handle_central_cacheable(net, ks, watch, agent, auth, hres, next);
            }
          }
        });
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL EVICT -------------------------------------------------------------

exports.HandleStorageClusterLocalEvict = function(net, ks, agent, hres, next) {
  var suuid = agent.uuid;
  ZH.l('ZCache.HandleStorageClusterLocalEvict: K: ' + ks.kqk + ' U: ' + suuid);
  set_key_watch(net, ks, suuid, false, hres, function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      ZMDC.InvalidateKeyToDevices(ks);
      next(null, hres);
    }
  });
}

exports.CentralHandleGeoClusterLocalEvict = function(net, ks, agent,
                                                     hres, next) {
  ZH.l('ZCache.CentralHandleGeoClusterLocalEvict: K: ' + ks.kqk);
  hres.ks   = ks; // Used in response
  var suuid = agent.uuid;
  set_key_watch(net, ks, suuid, false, hres, next);
  // NOTE: AsyncPushStorageLocalEvict() is ASYNC
  ZDQ.AsyncPushStorageClusterLocalEvict(net.plugin, net.collections, ks, agent);
}

exports.HandleStorageClusterEvict = function(net, ks, agent, hres, next) {
  var suuid = agent.uuid;
  ZH.l('ZCache.HandleStorageClusterEvict: K: ' + ks.kqk + ' U: ' + suuid);
  remove_cache_key_metadata(net, ks, suuid, hres, function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      ZMDC.InvalidateKeyToDevices(ks);
      next(null, hres);
    }
  });
}

exports.CentralHandleGeoClusterEvict = function(net, ks, agent, hres, next) {
  ZH.l('ZCache.CentralHandleGeoClusterEvict: K: ' + ks.kqk);
  hres.ks   = ks; // Used in response
  var suuid = agent.uuid;
  remove_cache_key_metadata(net, ks, suuid, hres, next);
  // NOTE: ZDQ.AsyncPushStorageClusterEvict() is ASYNC
  ZDQ.AsyncPushStorageClusterEvict(net.plugin, net.collections, ks, agent);
}

exports.CentralHandleClusterEvict = function(net, ks, agent, hres, next) {
  ZH.l('ZCache.CentralHandleClusterEvict: K: ' + ks.kqk);
  hres.ks = ks; // Used in response
  // ZPio.GeoBroadcastEvict() is ASYNC
  ZPio.GeoBroadcastEvict(net.plugin, net.collections, ks, agent);
  next(null, hres);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZDelt.do_commit_agent_version()
exports.AgentStoreCacheKeyMetadata = function(net, ks, watch, auth, next) {
  ZH.l('ZCache.AgentStoreCacheKeyMetadata: K: ' + ks.kqk);
  agent_store_cache_key_metadata(net, ks, watch, auth, next);
}

// NOTE: Used by ZRAD.update_subscriber_map_cached
exports.StoreCacheKeyMetadata = function(net, ks, watch, duuid, hres, next) {
  ZH.l('ZCache.StoreCacheKeyMetadata: K: ' + ks.kqk);
  store_cache_key_metadata(net, ks, watch, duuid, hres, next);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZCache']={} : exports);

