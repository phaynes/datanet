"use strict";

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GENERAL -------------------------------------------------------------------

exports.clone = function(o) {
  if (!o) return o;
  else    return JSON.parse(JSON.stringify(o));
}

exports.GetMsTime = function(secs) {
  var now = Date.now();
  if (secs) now = ((Math.floor(now / 1000)) * 1000);
  return now;
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZH']={} : exports);
