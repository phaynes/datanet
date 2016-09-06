
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// USER FACING API -----------------------------------------------------------

var XTF = require('./xtweet_filter');

// WARNING: EXPOSED USER-DEFINED FUNCTIONS
exports.Filters = {TweetServerFilter     : XTF.RunTweetServerFilter,
                   FiveSecondSleepFilter : run_five_second_sleep_filter};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// IMPLEMENTATION ------------------------------------------------------------

var ZS       = require('./zshared');
var ZH       = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var FilterFunctionTimeout = 1000;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RUN FILTER ----------------------------------------------------------------

exports.RunFilter = function(net, ks, pmd, pfunc, next) {
  ZH.l('ZFilter.RunFilter: K: ' + ks.kqk);
  try {
    var timedout = false;
    var to       = setTimeout(function() {
      timedout = true;
      next(new Error(ZS.Errors.FilterFunctionTimeout), null);
    }, FilterFunctionTimeout);
    pfunc(net, ks, pmd, function(serr, ferr) {
      if (timedout) return;
      clearTimeout(to);
      next(serr, ferr);
    });
  } catch(e) {
    ZH.e('CATCH-ERROR: ZFilter.RunFilter: ' + e.message);
    next(e, null);
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG USER DEFINED FILTER FUNCTIONS ---------------------------------------

// NOTE: run_five_second_sleep_filter tests FILTER TIMEOUTS
function run_five_second_sleep_filter(net, ks, pmd, next) {
  ZH.l('run_five_second_sleep_filter: K: ' + ks.kqk);
  setTimeout(function() {
    ZH.l('SLEEP FINISHED: run_five_second_sleep_filter: K: ' + ks.kqk);
    next(null, null);
  }, 5000);
}

