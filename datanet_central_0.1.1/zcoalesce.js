"use strict";

var ZMerge, ZDelt, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZMerge    = require('./zmerge');
  ZDelt     = require('./zdeltas');
  ZS        = require('./zshared');
  ZH        = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

function do_coalesce_deltas(net, cinfo, davrsn, next) {
  var ks   = cinfo.ks;
  ZH.l('COALESCE AgentDeltas: K: ' + ks.kqk + ' AV: ' + davrsn);
  var pkey = ZS.GetAgentPersistDelta(ks.kqk, ZH.MyUUID, davrsn);
  net.plugin.do_get_field(net.collections.delta_coll, pkey, "dentry",
  function(gerr, odentry) {
    if (gerr) next(gerr, null);
    else {
      var ndentry = ZH.clone(cinfo.dentry);
      var ok      = ZMerge.CoalesceAgentDeltas(ndentry, odentry, cinfo.dentry);
      if (!ok) {
        ZDelt.DoCommitDelta(net, cinfo, 1, next);
      } else { // NOTE: avinc is ZERO
        cinfo.pdentry = ndentry;
        cinfo.odentry = odentry;
        ZDelt.DoCommitDelta(net, cinfo, 0, next);
      }
    }
  });
}

exports.CheckCoalesceDeltas = function(net, cinfo, next) {
  if (ZH.Agent.DisableDeltaCoalescing) {
    ZDelt.DoCommitDelta(net, cinfo, 1, next);
    return;
  }
  var ks      = cinfo.ks;
  var offline = ZH.GetAgentOffline();
  ZH.l('ZCOAL.CheckCoalesceDeltas: offline: ' + offline +
       ' ndirty: ' + cinfo.ndirty);
  if (!offline || !cinfo.ndirty) {
    // NOTE: when NOT coalescing: pdentry & adentry are the same
    ZDelt.DoCommitDelta(net, cinfo, 1, next);
  } else {
    var dkey = ZS.GetAgentKeyDeltaVersions(ks.kqk);
    net.plugin.do_get_array_values(net.collections.delta_coll,
                                   dkey, "aversions",
    function(verr, avrsns) {
      if (verr) next(verr, null);
      else {
        var davrsns = []; // ONLY AGENT-DELTAS
        if (avrsns) {
          for (var i = 0; i < avrsns.length; i++) {
            var avrsn = avrsns[i];
            var auuid = ZH.GetAvUUID(avrsn);
            if (auuid === ZH.MyUUID) davrsns.push(avrsn);
          }
        }
        if (davrsns.length === 0) {
          ZDelt.DoCommitDelta(net, cinfo, 1, next);
        } else {
          var davrsn = davrsns.pop(); // PreviousDelta
          var dkey   = ZS.AgentSentDeltas;
          var dval   = ZS.GetSentAgentDeltaKey(ks.kqk, davrsn);
          net.plugin.do_get_field(net.collections.global_coll, dkey, dval,
          function(gerr, sent) {
            if (gerr) next(gerr, null);
            else {
              if (sent) { // PreviousDelta already sent to Central
                ZDelt.DoCommitDelta(net, cinfo, 1, next);
              } else {
                do_coalesce_deltas(net, cinfo, davrsn, next);
              }
            }
          });
        }
      }
    });
  }
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZCoal']={} : exports);
