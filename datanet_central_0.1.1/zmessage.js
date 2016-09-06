"use strict";

var ZAio, ZAuth, ZASC, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZAio   = require('./zaio');
  ZAuth  = require('./zauth');
  ZASC   = require('./zapp_server_cluster');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
} 

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MESSAGE METHODS -----------------------------------------------------------

function get_need_perms(body, next) {
  var need_perms = false;
  if (body.route && (body.route.user || body.route.device)) need_perms = true;
  //ZH.e('get_need_perms: need_perms: ' + need_perms);
  next(null, need_perms);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SIDE CLIENT-MESSAGE -------------------------------------------------

exports.HandleClientMessage = function(net, mtxt, auth, hres, next) {
  var body;
  try {
    body = JSON.parse(mtxt);
  } catch (e) {
    return next(new Error(ZS.Errors.JsonParseFail), null);
  }
  get_need_perms(body, function(gerr, need_perms) {
    if (gerr) next(gerr, hres);
    else {
      if (!need_perms) {
        ZAio.SendCentralAgentMessage(net.plugin, net.collections, body, auth,
        function(serr, sres) {
          next(serr, hres);
        });
      } else {
        ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
          if      (aerr) next(aerr, hres);
          else if (!ok)  next(new Error(ZS.Errors.MessageAuthFail), hres);
          else {
            ZAio.SendCentralAgentMessage(net.plugin, net.collections,
                                         body, auth,
            function(serr, sres) {
              next(serr, hres);
            });
          }
        })
      }
    }
  })
}

// NOTE: no Authorization: ClientRequest may bootstrap device
exports.HandleClientRequest = function(net, mtxt, auth, hres, next) {
  var body;
  try {
    body = JSON.parse(mtxt);
  } catch (e) {
    return next(new Error(ZS.Errors.JsonParseFail), null);
  }
  ZAio.SendCentralAgentRequest(net.plugin, net.collections, body, auth,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      hres.r_data = sres.r_data;
      next(null, hres);
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL-SIDE AGENT/GEO-MESSAGE ------------------------------------------------

exports.CentralHandleGeoMessage = function(net, msg, hres, next) {
  ZASC.ProcessGeoMessage(net.plugin, net.collections, msg, hres, next);
}

exports.CentralHandleAgentMessage = function(net, body, device,
                                             auth, hres, next) {
  get_need_perms(body, function(gerr, need_perms) {
    if (gerr) next(gerr, hres);
    else {
      if (!need_perms) {
        ZASC.ProcessAgentMessage(net.plugin, net.collections,
                                 body, device, auth, hres, next);
      } else {
        ZAuth.AdminAuthentication(net, auth, function(aerr, ok) {
          if      (aerr) next(aerr, hres);
          else if (!ok)  next(new Error(ZS.Errors.MessageAuthFail), hres);
          else {
            ZASC.ProcessAgentMessage(net.plugin, net.collections,
                                     body, device, auth, hres, next);
          }
        });
      }
    }
  });
}

// NOTE: no Authorization: ClientRequest may bootstrap device
exports.CentralHandleAgentRequest = function(net, body, device,
                                             auth, hres, next) {
  ZASC.ProcessAgentRequest(net.plugin, net.collections,
                           body, device, auth, hres, next);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZMessage']={} : exports);

