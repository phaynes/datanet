
var https = require('https');

var ZH    = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SEND TWEET AGGREGATION SERVICE --------------------------------------------

var TweetAggregationServiceMap = {"D1" : {ip   : '127.0.0.1', port : 9900},
                                  "D2" : {ip   : '127.0.0.1', port : 9901},
                                  "D3" : {ip   : '127.0.0.1', port : 9902},
                                 };

function get_my_tweet_aggregation_server() {
  var server = TweetAggregationServiceMap[ZH.MyDataCenter];
  if (!server) throw(new Error("get_my_tweet_aggregation_server LOGIC ERROR"));
  return server;
}

function send_tweet_aggregation_service(subs, username, tweet) {
  var server = get_my_tweet_aggregation_server();
  ZH.l('send_tweet_aggregation_service: U: ' + username + ' T: ' + tweet.text +
       ' P: ' + server.port);
  try {
    var body         = {action   : "tweet",
                        subs     : subs,
                        username : username,
                        tweet    : tweet
                       };
    var data         = {body : body};
    var post_data    = JSON.stringify(data);
    var post_options = {
      host               : server.ip,
      port               : server.port,
      path               : '/tweet',
      method             : 'POST',
      rejectUnauthorized : false,
      requestCert        : true,
      agent              : false,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': post_data.length
      }
    };
    var post_request = https.request(post_options, function(res) {
      res.setEncoding('utf8');
      var pdata = '';
      res.on('data', function (chunk) { pdata += chunk; });
      res.on('end', function() {
        ZH.l('TweetAggregationService: RESPONSE'); ZH.p(pdata);
      });
    });
    post_request.on('error', function(e) {
      ZH.e('ERROR: send_tweet_aggregation_service: ' + e.message);
    });
    ZH.l('send_tweet_aggregation_service: WRITE'); ZH.l(post_data);
    post_request.write(post_data);
    post_request.end();
  } catch (e) {
    ZH.e('CATCH-ERROR: send_tweet_aggregation_service: ' + e.message);
  }
}

function decode_tweet(tweet) {
  var res = tweet.split("|");
  return {when : Number(res[0]),
          text : res[1]};
}

exports.SendTweetAggregationService = function(data) {
  var flags    = data.flags;
  var is_local = flags.is_local;
  if (!is_local) return; // AGGREGATE ONLY LOCAL DELTAS
  var pmd      = data.post_merge_delta;
  var ks       = data.ks;
  if (pmd && ks.ns === "production" && ks.cn === "tweets") {
    ZH.l('XTM.SendTweetAggregationService: K: ' + ks.kqk);
    var subs     = data.subs;
    var username = data.author.username;
    for (var i = 0; i < pmd.length; i++) {
      var pme  = pmd[i];
      var path = pme.path;
      if (path.substring(0,6) === "tweets") {
        var value = pme.value;
        var tweet = decode_tweet(value);
        send_tweet_aggregation_service(subs, username, tweet);
        return;
      }
    }
  }
}


