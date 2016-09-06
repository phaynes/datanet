
var ZH = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// TWEET FILTER --------------------------------------------------------------

var BadWords = ["FUCK", "SHIT", "BITCH", "ASS"];

function decode_tweet(tweet) {
  var res = tweet.split("|");
  return {when : Number(res[0]),
          text : res[1]};
}

function check_string_bad_words(value) {
  var ferr;
  ZH.e('BAD-WORD-CHECK: ' + value);
  var uval = value.toUpperCase();
  for (var j = 0; j < BadWords.length; j++) {
    var found = uval.search(BadWords[j]);
    if (found !== -1) {
      ferr = new Error('FILTER_FAIL: BAD-WORD: ' + uval);
      ZH.e(ferr.message);
      return ferr;
    }
  }
  return ferr;
}

exports.RunTweetServerFilter = function(net, ks, pmd, next) {
  if (ks.ns === "production" && ks.cn === "tweets") {
    ZH.l('XTF.RunTweetServerFilter: K: ' + ks.kqk); ZH.p(pmd);
    for (var i = 0; i < pmd.length; i++) {
      var pme  = pmd[i];
      var path = pme.path;
      if (path.substring(0,6) === "tweets") {
        var value = pme.value;
        var vtype = typeof(value);
        var isarr = (vtype === "object") && Array.isArray(value);
        if (!isarr) {
          var tweet = decode_tweet(value);
          var ferr  = check_string_bad_words(tweet.text);
          if (ferr) return next(null, ferr);
          else      return next(null, null);
        }
      }
    }
  }
  next(null, null);
}


