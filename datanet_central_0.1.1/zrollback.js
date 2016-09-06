"use strict";

var ZSD, ZCR, ZMDC, ZDS, ZFix, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZSD      = require('./zsubscriber_delta');
  ZCR      = require('./zcreap');
  ZMDC     = require('./zmemory_data_cache');
  ZDS      = require('./zdatastore');
  ZFix     = require('./zfixlog');
  ZS       = require('./zshared');
  ZH       = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// NOTE: IMPORTANT: ROLLBACK operations must be IDEMPOTENT

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROLLBACK AGENT-DELTA ------------------------------------------------------

function remove_agent_delta(plugin, collections, pc, next) {
  if (pc.avinc === 0) { // Overwrite Coalesced-Dentry with OLD-Dentry
    ZDS.StoreAgentDelta(plugin, collections, pc.ks, pc.odentry, pc.auth, next);
  } else {
    var navrsn = pc.new_agent_version;
    ZDS.RemoveAgentDelta(plugin, collections, pc.ks, navrsn, next);
  }
}

function rollback_push_agent_version(plugin, collections, pc, next) {
  if (pc.avinc === 0) next(null, null); // CoalescedDelta -> NO-OP
  else {
    var ks     = pc.ks;
    var navrsn = pc.new_agent_version;
    var dkey   = ZS.GetAgentKeyDeltaVersions(ks.kqk);
    ZH.l('rollback_push_agent_version: dkey: ' + dkey + ' AV: ' + navrsn);
    plugin.do_pull(collections.delta_coll, dkey, "aversions", navrsn, next);
  }
}

function rollback_agent_delta(plugin, collections, pc, next) {
  var ks       = pc.ks;
  var md       = pc.extra_data.md;
  var o_nbytes = pc.ncrdt._meta.num_bytes;
  var net      = ZH.CreateNetPerRequest(ZH.Agent);
  var akey     = ZS.GetKeyAgentVersion(ks.kqk);
  var oavrsn   = pc.old_agent_version;
  var navrsn   = pc.new_agent_version;
  net.plugin.do_set_field(net.collections.key_coll, akey, "value", oavrsn,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      ZSD.DoSetSubscriberAgentVersion(net.plugin, net.collections,
                                      ks, oavrsn, ZH.MyUUID,
      function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          if (ZH.PathologicalMode === 1) {
            return next(new Error(ZH.PathologicalDescriptions[1]), null);
          }
          ZDS.InternalStoreCrdt(net, ks, md.ocrdt, function(oerr, ores) {
            if (oerr) next(oerr, null);
            else {
              var adkey = ZS.AgentDirtyKeys;
              net.plugin.do_increment(net.collections.global_coll,
                                      adkey, ks.kqk, -1,
              function(zerr, zres) {
                if (zerr) next(zerr, null);
                else {
                  rollback_push_agent_version(net.plugin, net.collections, pc,
                  function(xerr, xres) {
                    if (xerr && xerr.message !== ZS.Errors.ElementNotFound &&
                                xerr.message !== ZS.Errors.KeyNotFound) {
                      next(xerr, null);
                    } else {
                      remove_agent_delta(net.plugin, net.collections, pc, 
                      function(rerr, rres) {
                        if (rerr) next(rerr, null);
                        else {
                          var now  = ZH.GetMsTime();
                          var dkey = ZS.AgentDirtyDeltas;
                          var dval = ZS.GetDirtyAgentDeltaKey(ks.kqk, ZH.MyUUID,
                                                              navrsn);
                          net.plugin.do_unset_field(net.collections.global_coll,
                                                  dkey, dval,
                          function(uerr, ures) {
                            if (uerr) next(uerr, null);
                            else {
                              // upc is mirror image -> to UNDO
                              var upc   = ZH.clone(pc);
                              var umd   = upc.extra_data.md;
                              umd.ocrdt = pc.ncrdt,
                              upc.ncrdt = md.ocrdt,
                              ZCR.StoreNumBytes(net, upc, o_nbytes, true, next);
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
      });
    }
  });
}

exports.RollbackAdelta = function(plugin, collections, pc, rerr, next) {
  var ks       = pc.ks;
  var internal = rerr ? false : true;
  ZH.l('ROLLBACK AGENT-DELTA: K: ' + ks.kqk);
  rollback_agent_delta(plugin, collections, pc, function(serr, sres) {
    if (serr) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(rerr, null);
    }
  });
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZRollback']={} : exports);

