"use strict";

var ZDelt, ZCache, ZSD, ZAS, ZDS, ZAuth, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZDelt  = require('./zdeltas');
  ZCache = require('./zcache');
  ZSD    = require('./zsubscriber_delta');
  ZAS    = require('./zactivesync');
  ZDS    = require('./zdatastore');
  ZAuth  = require('./zauth');
  ZAF    = require('./zaflow');
  ZQ     = require('./zqueue');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DATA HEARBEAT INITIALIZATION & OPERATIONS ---------------------------

var ADHeartbeat = {Timer      : null,
                   Started    : false,
                   Sleep      : 1000,
                   Increment  : {
                     Auth  : null,
                     Field : null
                   },
                   Timestamp  : {
                     Auth    : null,
                     UUID    : null,
                     MaxSize : null,
                     Trim    : null
                   },
                   Array      : {
                     Auth    : null,
                     UUID    : null,
                     MaxSize : null,
                     Trim    : null
                   },
                   Namespace  : "production",
                   Collection : "statistics",
                   IKey       : "INCREMENT_HEARTBEAT",
                   TKey       : "TIMESTAMP_HEARTBEAT",
                   AKey       : "ARRAY_HEARTBEAT",
                  };

var ADH_Increment_JSON = {"_id"       : ADHeartbeat.IKey,
                          "_channels" : ["0"]
                         };

var ADH_Timestamp_JSON  = {"_id"       : ADHeartbeat.TKey,
                           "_channels" : ["0"],
                           "CONTENTS"  : {"_data"     : [],
                                          "_type"     : "LIST",
                                          "_metadata" : {"ORDERED" : true}
                                         }
                          };

var ADH_Array_JSON  = {"_id"       : ADHeartbeat.AKey,
                       "_channels" : ["0"],
                       "CONTENTS"  : {"_data"     : [],
                                      "_type"     : "LIST",
                                      "_metadata" : {}
                                     }
                      };

function get_adh_increment_ks() {
  return ZH.CompositeQueueKey(ADHeartbeat.Namespace,
                              ADHeartbeat.Collection,
                              ADHeartbeat.IKey);
}

function get_adh_timestamp_ks() {
  return ZH.CompositeQueueKey(ADHeartbeat.Namespace,
                              ADHeartbeat.Collection,
                              ADHeartbeat.TKey);
}

function get_adh_array_ks() {
  return ZH.CompositeQueueKey(ADHeartbeat.Namespace,
                              ADHeartbeat.Collection,
                              ADHeartbeat.AKey);
}

function create_adh_increment_json(field) {
  var json    = ADH_Increment_JSON;
  json[field] = 1;
  return json;
}

function create_adh_uuid_json(isa, uuid, mlen, trim) {
  var json = isa ? ADH_Array_JSON : ADH_Timestamp_JSON;
  json.CONTENTS._metadata['MAX-SIZE'] = mlen;
  json.CONTENTS._metadata['TRIM']     = trim;
  return json;
}

function create_adh_increment_oplog(field, do_incr) {
  if (do_incr) return [{name: "increment", path: field, args: [ 1 ] }];
  else         return [{name: "set",       path: field, args: [ 1 ] }];
}

function create_adh_uuid_entry(uuid) {
  var now = ZH.GetMsTime();
  return now + '-' + uuid; // NOTE: 'now' comes first for sorting purposes
}

function create_adh_uuid_oplog(uuid, clen) {
  var e = create_adh_uuid_entry(uuid);
  return [{name : "insert",
           path : "CONTENTS",
           args : [clen, e]}];
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DATA HEARBEAT FLOWS -------------------------------------------------

function do_flow_adh_on_uuid(net, ks, uuid, mlen, trim, isa, auth, next) {
  ZH.l('do_flow_adh_on_uuid: K: ' + ks.kqk + ' U: ' + uuid);
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function(gerr, fcrdt){
    if (gerr) next(gerr, null);
    else {
      if (!fcrdt) {
        ZH.e('ZHB.do_flow_adh_on_uuid: INIT');
        var json   = create_adh_uuid_json(isa, uuid, mlen, trim);
        var rchans = json._channels;
        ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
        function(aerr, ok) {
          if      (aerr) next(aerr, null);
          else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), null);
          else {
            var sep   = false;
            var locex = 0;
            var exp   = 0;
            ZDelt.CreateClientDelta(net, ks, true, false, false, false,
                                    json, null, null, exp,
                                    sep, locex, auth, next);
          }
        });
      } else {
        ZH.l('do_flow_adh_on_uuid: NORMAL');
        var rchans = ZH.GetReplicationChannels(fcrdt._meta);
        ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
        function(aerr, ok) {
          if      (aerr) next(aerr, null);
          else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), null);
          else {
            var json  = ZH.CreatePrettyJson(fcrdt);
            var clen  = json.CONTENTS ? json.CONTENTS.length : 0;
            var oplog = create_adh_uuid_oplog(uuid, clen);
            var meta  = fcrdt._meta;
            meta.last_member_count = meta.member_count;
            var sep   = false;
            var locex = 0;
            var exp   = 0;
            ZDelt.CreateClientDelta(net, ks, false, true, false, false,
                                    null, fcrdt, oplog, exp,
                                    sep, locex, auth, next);
          }
        });
      }
    }
  });
}

function do_flow_adh_on_increment(net, ks, field, auth, next) {
  ZH.l('do_flow_adh_on_increment: K: ' + ks.kqk + ' F: ' + field);
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function(gerr, fcrdt){
    if (gerr) next(gerr, null);
    else {
      if (!fcrdt) {
        ZH.e('ZHB.do_flow_adh_on_increment: INIT: F: ' + field);
        var json   = create_adh_increment_json(field);
        var rchans = json._channels;
        ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
        function(aerr, ok) {
          if      (aerr) next(aerr, null);
          else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), null);
          else {
            var sep   = false;
            var locex = 0;
            var exp   = 0;
            ZDelt.CreateClientDelta(net, ks, true, false, false, false,
                                    json, null, null, exp,
                                    sep, locex, auth, next);
          }
        });
      } else {
        var rchans = ZH.GetReplicationChannels(fcrdt._meta);
        ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
        function(aerr, ok) {
          if      (aerr) next(aerr, null);
          else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), null);
          else {
            var json    = ZH.CreatePrettyJson(fcrdt);
            var found   = ZH.LookupByDotNotation(json, field);
            var do_incr = found ? true : false;
            var oplog   = create_adh_increment_oplog(field, do_incr);
            var meta    = fcrdt._meta;
            meta.last_member_count = meta.member_count;
            var sep   = false;
            var locex = 0;
            var exp   = 0;
            ZDelt.CreateClientDelta(net, ks, false, true, false, false,
                                    null, fcrdt, oplog, exp,
                                    sep, locex, auth, next);
          }
        });
      }
    }
  });
}

function do_flow_heartbeat(net, ks, field, uuid, mlen, trim, isa, auth, next) {
  var md = {};
  ZSD.GetAgentSyncStatus(net, ks, md, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (md.to_sync_key || md.out_of_sync_key) {
        ZH.e('ZHB: OUT-OF-SYNC/TO-SYNC K: ' + ks.kqk + ' -> NO-OP');
        next(null, null);
      } else {
        if (field) {
          do_flow_adh_on_increment(net, ks, field, auth, next);
        } else {
          do_flow_adh_on_uuid(net, ks, uuid, mlen, trim, isa, auth, next);
        }
      }
    }
  });
}

exports.FlowHeartbeat = function(plugin, collections, qe, next) {
  var net    = qe.net;
  var qnext  = qe.next;
  var ks     = qe.ks;
  ZH.l('FlowHeartbeat: K: ' + ks.kqk);
  var data   = qe.data;
  var field  = data.field;
  var uuid   = data.uuid;
  var mlen   = data.mlen;
  var trim   = data.trim;
  var isa    = data.isa;
  var auth   = data.auth;
  do_flow_heartbeat(net, ks, field, uuid, mlen, trim, isa, auth,
  function(serr, sres) {
    qnext(serr, null);
    next(null, null);
  });
}

function do_adh_on_uuid(net, uuid, mlen, trim, isa, auth, next) {
  var ks   = isa ? get_adh_array_ks() : get_adh_timestamp_ks();
  var data = {uuid : uuid, mlen : mlen, trim : trim, isa : isa, auth : auth};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'HEARTBEAT',
                       net, data, null, null, next);
  var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function do_adh_on_increment(net, field, auth, next) {
  var ks   = get_adh_increment_ks();
  var data = {field: field, auth : auth};
  ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks, 'HEARTBEAT',
                       net, data, null, null, next);
  var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
              m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function do_heartbeat_increment(net, field, auth, next) {
  if (!field) next(null, null);
  else {
    ZH.l('do_heartbeat_increment: F: ' + field);
    do_adh_on_increment(net, field, auth, next);
  }
}

function do_heartbeat_timestamp(net, uuid, mlen, trim, auth, next) {
  if (!uuid) next(null, null);
  else {
    ZH.l('do_heartbeat_timestamp: U: ' + uuid + ' MS: ' + mlen);
    do_adh_on_uuid(net, uuid, mlen, trim, false, auth, next);
  }
}

function do_heartbeat_array(net, uuid, mlen, trim, auth, next) {
  if (!uuid) next(null, null);
  else {
    ZH.l('do_heartbeat_array: U: ' + uuid + ' MS: ' + mlen);
    do_adh_on_uuid(net, uuid, mlen, trim, true, auth, next);
  }
}

function __do_agent_data_heartbeat(next) {
  var net    = ZH.CreateNetPerRequest(ZH.Agent);
  var iauth  = ADHeartbeat.Increment.Auth;
  var ifield = ADHeartbeat.Increment.Field;
  var tauth  = ADHeartbeat.Timestamp.Auth;
  var tuuid  = ADHeartbeat.Timestamp.UUID;
  var tmlen  = ADHeartbeat.Timestamp.MaxSize;
  var ttrim  = ADHeartbeat.Timestamp.Trim;
  var aauth  = ADHeartbeat.Array.Auth;
  var auuid  = ADHeartbeat.Array.UUID;
  var amlen  = ADHeartbeat.Array.MaxSize;
  var atrim  = ADHeartbeat.Array.Trim;
  do_heartbeat_increment(net, ifield, iauth, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      do_heartbeat_timestamp(net, tuuid, tmlen, ttrim, tauth,
      function(uerr, ures) {
        if (uerr) next(uerr, null);
        else {
          do_heartbeat_array(net, auuid, amlen, atrim, aauth, next);
        }
      });
    }
  });
}

function do_agent_data_heartbeat() {
  __do_agent_data_heartbeat(function(aerr, ares) {
    if (aerr) ZH.e('do_agent_data_heartbeat: ERR: ' + aerr);
    if (ADHeartbeat.Started) {
      var sleep         = ADHeartbeat.Sleep;
      ADHeartbeat.Timer = setTimeout(do_agent_data_heartbeat, sleep);
    }
  });
}

function cache_heartbeat(net, isi, isa, auth, next) {
  var ks   = isi ? get_adh_increment_ks :
             isa ? get_adh_array_ks()   :
                   get_adh_timestamp_ks();
  var pin      = false;
  var watch    = false;
  var sticky   = false;
  var force    = false;
  var internal = true;
  ZCache.AgentCache(net, ks, pin, watch, sticky, force, internal, auth,
                    {}, next);
}

function start_agent_data_heartbeat(net, isi, isa, auth, next) {
  cache_heartbeat(net, isi, isa, auth, function(serr, sres) {
    if (serr) ZH.e('start_agent_data_heartbeat: CACHE: ' + serr.message);
    if (ADHeartbeat.Timer) next(null, null); // ALREADY RUNNING
    else {
      ADHeartbeat.Started = true;
      var sleep           = ADHeartbeat.Sleep;
      ADHeartbeat.Timer   = setTimeout(do_agent_data_heartbeat, sleep);
      next(null, null);
    }
  });
}

function summarize_heartbeat(hres, next) {
  hres.Increment = ZH.clone(ADHeartbeat.Increment);
  hres.Timestamp = ZH.clone(ADHeartbeat.Timestamp);
  hres.Array     = ZH.clone(ADHeartbeat.Array);
  if (hres.Increment.Auth) delete(hres.Increment.Auth.password);
  if (hres.Timestamp.Auth) delete(hres.Timestamp.Auth.password);
  if (hres.Array.Auth)     delete(hres.Array.Auth.password);
  hres.Increment.active = hres.Increment.Field ? true : false;
  hres.Timestamp.active = hres.Timestamp.UUID  ? true : false;
  hres.Array.active     = hres.Array.UUID      ? true : false;
  next(null, hres);
}

exports.AgentDataHeartbeat = function(net, cmd, field, uuid, mlen, trim,
                                      isi, isa, auth, hres, next) {
  if (isi) {
    ZH.l('ZHB.AgentDataHeartbeat: CMD: ' + cmd + ' F: ' + field);
  } else {
    ZH.l('ZHB.AgentDataHeartbeat: CMD: ' + cmd + ' U: ' + uuid +
         ' MS: ' + mlen + ' TRIM: ' + trim);
  }
  hres.Increment = {}; // Used in response
  hres.Timestamp = {}; // Used in response
  hres.Array     = {}; // Used in response
  var c          = cmd.toUpperCase();
  if        (c === 'START') {
    if (!field && !uuid) {
      return next(new Error(ZS.Errors.HeartbeatBadStart), hres);
    }
    if (isi) {        // INCREMENT-HEARTBEAT
      ADHeartbeat.Increment.Auth = auth;
      if (field) ADHeartbeat.Increment.Field = field;
    } else if (isa) { // ARRAY-HEARTBEAT
      ADHeartbeat.Array.Auth = auth;
      if (uuid) ADHeartbeat.Array.UUID    = uuid;
      if (mlen) ADHeartbeat.Array.MaxSize = mlen;
      if (trim) ADHeartbeat.Array.Trim    = trim;
    } else {          // TIMESTAMP-HEARTBEAT
      ADHeartbeat.Timestamp.Auth = auth;
      if (uuid) ADHeartbeat.Timestamp.UUID    = uuid;
      if (mlen) ADHeartbeat.Timestamp.MaxSize = mlen;
      if (trim) ADHeartbeat.Timestamp.Trim    = trim;
    }
    start_agent_data_heartbeat(net, isi, isa, auth, function(serr, sres) {
      if (serr) next(serr, hres);
      else      summarize_heartbeat(hres, next);
    });
  } else if (c === 'STOP') {
    if (isi) {        // INCREMENT-HEARTBEAT
      ADHeartbeat.Increment.Auth    = null;
      ADHeartbeat.Increment.Field   = null;
    } else if (isa) { // ARRAY-HEARTBEAT
      ADHeartbeat.Array.Auth        = null;
      ADHeartbeat.Array.UUID        = null;
      ADHeartbeat.Array.MaxSize     = null;
      ADHeartbeat.Array.Trim        = null;
    } else {          // TIMESTAMP-HEARTBEAT
      ADHeartbeat.Timestamp.Auth    = null;
      ADHeartbeat.Timestamp.UUID    = null;
      ADHeartbeat.Timestamp.MaxSize = null;
      ADHeartbeat.Timestamp.Trim    = null;
    }
    if (!ADHeartbeat.Increment.Field &&
        !ADHeartbeat.Array.UUID      &&
        !ADHeartbeat.Timestamp.UUID) {
      ZH.l('ALL HEARTBEATS (INCREMENT,TIMESTAMP,ARRAY) STOPPED');
      ADHeartbeat.Started = false;
      if (ADHeartbeat.Timer) {
        clearTimeout(ADHeartbeat.Timer);
        ADHeartbeat.Timer = null;
      }
    }
    summarize_heartbeat(hres, next);
  } else if (c === 'QUERY') {
    summarize_heartbeat(hres, next);
  } else {
    return next(new Error(ZS.Errors.AgentHeartbeatUsage), hres);
  }
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZHB']={} : exports);

