"use strict";

var ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

var ComputeLatencyThreshold = 10;
var NumHistBuckets          = 10;

function flatten_histogram(ohist) {
  var hist   = ZH.clone(ohist);
  var mindex = -1;
  var max    = -1;
  for (var i = 0; i < hist.length; i++) {
    var lat = hist[i];
    if (lat.count > max) {
      max    = lat.count;
      mindex = i;
    }
  }

  if      (mindex === 0)                 mindex += 1;
  else if (mindex === (hist.length - 1)) mindex -= 1;

  var mcnt     = hist[mindex].count;
  var halfc    = (mcnt / 2);
  var lhalfc   = Math.floor(halfc);
  var rhalfc   = (lhalfc === halfc) ? lhalfc : (lhalfc + 1);

  var prema    = hist[(mindex - 1)].average;
  var ma       = hist[ mindex     ].average;
  var postma   = hist[(mindex + 1)].average;

  var lhalfa   = Math.floor(prema + ((ma     - prema) / 2));
  var rhalfa   = Math.floor(ma    + ((postma - ma)    / 2));

  var llat     = {average : lhalfa, count : lhalfc};
  var rlat     = {average : rhalfa, count : rhalfc};
  hist[mindex] = rlat;
  hist.splice(mindex, 0, llat);

  if (hist.length === NumHistBuckets) return hist;
  else                                return flatten_histogram(hist);// RECURSE
}

function make_histogram(nlats) {
  var hist = [];
  var tot  = 0;
  for (var i = 0; i < nlats.length; i++) {
    tot += nlats[i].count;
  }
  var step = Math.floor(tot / NumHistBuckets);
  var high = nlats[0].value;
  var low  = high
  var cnt  = 0;
  for (var i = 0; i < nlats.length; i++) {
    cnt += nlats[i].count;
    if (cnt >= step) {
      high    = nlats[i].value;
      var avg = low + Math.floor((high - low) / 2);
      hist.push({average : avg, count : cnt});
      cnt     = 0;
      low     = high;
    }
  }
  if (cnt !== 0) {
    high      = nlats[nlats.length - 1].value;
    var avg   = low + Math.floor((high - low) / 2);
    var lhist = hist[hist.length - 1];
    if (avg === lhist.average) lhist.count += cnt;
    else                       hist.push({average : avg, count : cnt});
  }
  if (hist.length === NumHistBuckets) return hist;
  else                                return flatten_histogram(hist);
}

function cmp_lat(la, lb) {
  return (la.value === lb.value) ? 0 : ((la.value > lb.value) ? 1 : -1);
}

function create_histogram(lats) {
  var nlats = [];
  for (var i = 0; i < lats.length; i++) {
    nlats.push({count : 1, value : lats[i]});
  }
  nlats.sort(cmp_lat);
  return make_histogram(nlats);
}

function merge_histograms(ohist, lats) {
  var nlats = [];
  for (var i = 0; i < ohist.length; i++) {
    var olat = ohist[i];
    nlats.push({count : olat.count, value : olat.average});
  }
  for (var i = 0; i < lats.length; i++) {
    nlats.push({count : 1, value : lats[i]});
  }
  nlats.sort(cmp_lat);
  return make_histogram(nlats);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD TO SUBSCRIBER LATENCIES -----------------------------------------------

var SubscriberLatencies = {};

var EndToEndSuspectTraceLatency     = 1000;
var MaxIntraGeoClusterPercent       = 90;
var IntraGeoClusterMaxTraceLatency  = 1500;
var AgentToDCWarnTraceLatency       = 1000;
var DCToDCWarnTraceLatency          = 1000;

function analyze_end_to_end_latencies(ks, meta) {
  var skip = false;
  var lt   = '';
  var lat  = meta.subscriber_received - meta.agent_sent;

  if (!meta.agent_received) {
    lt   += ' AGENT_RECEIVED NOT DEFINED';
    skip  = true;
  }
  if (lat >= EndToEndSuspectTraceLatency) {
    lt += ' END_TO_END: [TOTAL:' + lat  +
          ',[AS:' + meta.agent_sent      + ',AR:' + meta.agent_received      +
          ',D:' + (meta.agent_received  - meta.agent_sent)      + ']'        +
          ',[GS:' + meta.geo_sent        + ',GR:' + meta.geo_received        +
          ',D:' + (meta.geo_received    - meta.geo_sent)        + ']'        +
          ',[SS:' + meta.subscriber_sent + ',SR:' + meta.subscriber_received +
          ',D:' + (meta.subscriber_sent - meta.subscriber_sent) + ']'        +
          ']';
    var ingeo  = meta.subscriber_sent     - meta.agent_received;
    var prct   = Math.floor((ingeo * 100) / lat);
    if (prct >= MaxIntraGeoClusterPercent) {
      lt   += ' [END_TO_END: ' + lat + ' INTRA_GEO_CLUSTER: ' + ingeo +
              ' %: ' + prct + ']';
      skip  = true;
    }
  }
  if (meta.agent_sent      && meta.agent_received) {
    var diff = meta.agent_received - meta.agent_sent;
    if (diff >= AgentToDCWarnTraceLatency) {
      lt += ' [A(O):'  + meta.agent_sent     +
            '->DC(I):' + meta.agent_received +
            ',DIFF:'   + diff                + ']';
    }
  }
  if (meta.geo_sent        && meta.geo_received) {
    var diff = meta.geo_received - meta.geo_sent;
    if (diff >= DCToDCWarnTraceLatency) {
      lt += ' [DC(O):' + meta.geo_sent     + 
            '->DC(I):' + meta.geo_received +
            ',DIFF:'   + diff              + ']';
    }
  }
  if (meta.subscriber_sent && meta.subscriber_received) {
    var diff = meta.subscriber_received - meta.subscriber_sent;
    if (diff >= AgentToDCWarnTraceLatency) {
      lt += ' [DC(O):'  + meta.subscriber_sent     + 
            '->DC(I): ' + meta.subscriber_received +
            ',DIFF:'    + diff                     + ']';
    }
  }
  if (lt.length) {
    ZH.l('LATENCY_TRACE: K: ' + ks.kqk +
         ' AV: ' + meta.author.agent_version + lt);
  }
  return skip;
}

exports.AddToSubscriberLatencies = function(net, ks, dentry, next) {
  if (ZH.Agent.DisableSubscriberLatencies) {
    next(null, null);
    return;
  }
  var meta   = dentry.delta._meta;
  var skip   = meta.dirty_central ||
               !meta.agent_sent   ||
               !meta.subscriber_received;
  if (skip) next(null, null);
  else {
    var lskip = analyze_end_to_end_latencies(ks, meta);
    if (lskip) next(null, null);
    else {
      var rguuid = meta.author.datacenter;
      if (!rguuid) next(null, null); // REPLAYED AGENT-DELTA
      else {
        var lat    = meta.subscriber_received - meta.agent_sent;
        ZH.l('DC: ' + rguuid + ' LATENCY: ' + lat);
        if (!SubscriberLatencies[rguuid]) SubscriberLatencies[rguuid] = [];
        SubscriberLatencies[rguuid].push(lat);
        var lats = SubscriberLatencies[rguuid];
        if (lats.length < ComputeLatencyThreshold) next(null, null);
        else {
          var lkey = ZS.SubscriberLatencyHistogram;
          net.plugin.do_get_field(net.collections.global_coll, lkey, rguuid,
          function(gerr, gres) {
            if (gerr) next(gerr, null);
            else {
              var nhist;
              if (!gres) nhist = create_histogram(lats);
              else       nhist = merge_histograms(gres, lats);
              net.plugin.do_set_field(net.collections.global_coll,
                                      lkey, rguuid, nhist,
              function(serr, sres) {
                if (serr) next(serr, null);
                else {
                  delete(SubscriberLatencies[rguuid]);
                  next(null, null);
                }
              });
            }
          });
        }
      }
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT GET LATENCIES ------------------------------------------------------

exports.AgentGetLatencies = function(plugin, collections, hres, next) {
  var lkey = ZS.SubscriberLatencyHistogram;
  plugin.do_get(collections.global_coll, lkey, function(gerr, gres) {
    if (gerr) next(gerr, hres);
    else {
      hres.information = {};
      if (gres.length !== 0) {
        var lats = gres[0];
        delete(lats._id);
        hres.information.latencies = lats;
      }
      next(null, hres);
    }
  });
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZLat']={} : exports);

