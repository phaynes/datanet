"use strict";

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// USER FACING API -----------------------------------------------------------

var XTM = require('./xtweet_micro');

// WARNING: EXPOSED USER-DEFINED FUNCTIONS
var MicroServices = [{name : 'xtweet_micro',
                      run  :  XTM.SendTweetAggregationService}];


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// IMPLEMENTATION ------------------------------------------------------------

var ZH     = require('./zhelper');

exports.SendEvent = function(auuid, username, ks, rchans, subs,
                            flags, json, pmd) {
  if (MicroServices.length === 0) return;
  var data = {author           : {device_uuid : auuid,
                                  username    : username},
              ks               : ks,
              rchans           : rchans,
              subs             : subs,
              flags            : flags,
              post_merge_delta : pmd,
              json             : json};
  for (var i = 0; i < MicroServices.length; i++) {
    var name = MicroServices[i].name;
    var func = MicroServices[i].run;
    try {
      func(data);
    } catch(e) {
      ZH.e('CATCH-ERROR: ZMicro.SendEvent (' + name + '): ' + e.message);
    }
  }
}

