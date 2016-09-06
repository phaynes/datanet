
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REQUIRES ------------------------------------------------------------------

var https      = require('https');
var fs         = require('fs');

require('./setImmediate');

var ZCmdClient = require('./zcmd_client');
var ZS         = require('./zshared');
var ZH         = require('./zhelper');

ZH.LogToConsole = true;
ZH.ZyncRole     = 'TWEET_SERVER';
ZH.MyUUID       = 'TWEET_SERVER';

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CONFIG --------------------------------------------------------------------

var config_file = process.argv[2];
var Cjson;
try {
  var data = fs.readFileSync(config_file, 'utf8');
  Cjson    = JSON.parse(data);
} catch (e) {
  console.error('ERROR PARSING CONFIG FILE: ' + e);
  process.exit(-1);
}

if (!Cjson.server      || !Cjson.agent      || !Cjson.memcache_cluster ||
    !Cjson.server.port || !Cjson.agent.port || !Cjson.memcache_cluster.name) {
  console.error('CONFIG-FILE must contain: server.port, agent.port,' +
                ' memcache_cluster.name');
  process.exit(-1);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CONSTANTS -----------------------------------------------------------------

var UserCollectionName      = 'users';
var TweetCollectionName     = 'tweets';
var FollowingCollectionName = 'following';
var FollowerCollectionName  = 'followers';

var NumGetAgentKeys = 20;

var GlobalAggCollName = 'aggregations';
var GlobalAggKeyName  = 'tweets';
var UserAggCollName   = 'user_aggregations';

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function create_large_list() {
  return { _data     : [],
           _type     : "LIST",
           _metadata : { LARGE       : true,
                         "PAGE-SIZE" : 10,
                         ORDERED     : true}};
}

function encode_tweet(now, t) {
  return now + "|" + t;
}

function decode_tweet(tweet) {
  var res = tweet.split("|");
  return {when : Number(res[0]),
          text : res[1]};
}

function decode_many_tweets(tweets) {
  var many = [];
  for (var i = 0; i < tweets.length; i++) {
    many.push(decode_tweet(tweets[i]));
  }
  return many;
}

function encode_follow(now, f) {
  return now + "|" + f;
}

function decode_follow(uentry) {
  var res = uentry.split("|");
  return res[1]; // 'when' is not important, only 'username'
}

function decode_many_follow(follows) {
  var many = [];
  for (var i = 0; i < follows.length; i++) {
    many.push(decode_follow(follows[i]));
  }
  return many;
}

function cmp_tweet_stream(t1, t2) {
  return (t1.when === t2.when) ? 0 : (t1.when > t2.when) ? -1 : 1; // DESC
}

function trim_if_too_big(arr, maxlen, cmp_func) {
  if (arr.length > maxlen) {
    arr.sort(cmp_func);
    var diff = arr.length - maxlen;
    for (var i = (arr.length - 1); i >= 0; i--) {
      arr.splice(i, 1);
      diff -= 1;
      if (diff === 0) break;
    }
  }
}

function get_user_channel(username) {
  return "USER_" + username;
}

function get_follow_channel(username) {
  return "FOLLOW_" + username;
}

function get_tweets_channel(username) {
  return "TWEETS_" + username;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HTTPS SERVER --------------------------------------------------------------

var Hport = Cjson.server.port;
ZH.l('Listening on https://localhost:' + Hport);

var Hoptions = {
  key  : fs.readFileSync('./ssl/server.private.key'),
  cert : fs.readFileSync('./ssl/server.crt')
};

function send_200_message_response(response) {
  response.writeHead(200);
  response.write("OK SERVER RESPONSE BODY");
  response.end();
}

function respond_200(response, rbody) {
  ZH.e('SEND'); ZH.e(rbody);
  var btxt = JSON.stringify(rbody);
  response.writeHead(200);
  response.write(btxt);
  response.end();
}

function respond_404(response) {
  response.writeHead(404);
  response.write("INVALID MESSAGE/REQUEST");
  response.end();
}

function reply_message(err, suuid, rbody) {
  var msg;
  if (err) {
    msg = {type : "error",
           error : err.message};
  } else {
    ZH.l('reply_message: U; ' + suuid);
    msg = {type : "response",
           body : rbody};
  }
  var msg = {"route"    : {"device" : suuid},
             "message"  : msg};
  var mtxt = JSON.stringify(msg);
  AdminCli.Message(mtxt, ZH.OnErrLog);
}

var RequestHandlers = {internal     : do_internal,
                       register     : do_register,
                       login        : do_login,
                       profile      : do_profile,
                       follow       : do_follow,
                       aggregations : do_aggregations};

function handle_request(request, pdata, response) {
  ZH.l('URL: ' + request.url);
  var data;
  try {
    data = JSON.parse(pdata);
  } catch(e) {
    response.writeHead(200);
    response.write("ERROR: JSON.parse(): " + e);
    response.end();
    return;
  }
  var mbody   = data.body;
  ZH.l('RECIEVED'); ZH.p(mbody);
  var action  = mbody.action;
  var handler = RequestHandlers[action];
  if (handler) handler(data, response);
  else         respond_404(response);
}

function init_https_server() {
  https.createServer(Hoptions, function (request, response) {
    var pdata = '';
    request.addListener('data', function(chunk) { pdata += chunk; });
    request.addListener('end', function() {
      handle_request(request, pdata, response);
    });
  }).listen(Hport);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA CLIENT ---------------------------------------------------------------

var Zport = Cjson.agent.port;
ZH.l('Connecting to localhost agent: port: ' + Zport);

var AdminUsername = 'admin';
var AdminPassword = 'apassword';
var AdminAuth     = {username : AdminUsername, password : AdminPassword};
var Nsname        = 'production';
var AdminCli      = ZCmdClient.ConstructCmdClient('ADMIN_APP_SERVER');

AdminCli.SetAgentAddress('127.0.0.1', Zport);
AdminCli.SetClientAuthentication(AdminUsername, AdminPassword);
AdminCli.SetNamespace(Nsname);

function init_data_client(next) {
  var clname = Cjson.memcache_cluster.name;
  AdminCli.GetMemcacheDaemonClusterState(clname, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STARTUP -------------------------------------------------------------------

init_data_client(function(ierr, ires) {
  if (ierr) throw(ierr);
  else {
    init_https_server();
  }
});

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA HELPERS --------------------------------------------------------------

function do_cache_method(method, cname, kname, next) {
  var collection = AdminCli.Collection(cname);
  collection[method](kname, function(gerr, zdoc) {
    if (!gerr) next(null, zdoc);
    else {
      var notfound = (gerr.message === ZS.Errors.NoDataFound);
      if (notfound) next(null, null);
      else          next(gerr, null);
    }
  });
}

function do_cache(cname, kname, next) {
  var method = 'cache';
  do_cache_method(method, cname, kname, next);
}

function do_sticky_cache(cname, kname, next) {
  var method = 'sticky_cache';
  do_cache_method(method, cname, kname, next);
}

function do_store(cname, kname, doc, next) {
  ZH.l('do_store: C: ' + cname + ' K: ' + kname); ZH.p(doc);
  var collection = AdminCli.Collection(cname);
  collection.store(kname, doc, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INTERNAL HANDLER ----------------------------------------------------------

function do_internal(data, response) {
  ZH.l('do_internal'); //ZH.p(data);
  var mbody  = data.body;
  var mstate = mbody.cluster_state;
  AdminCli.SetCientMemcacheDaemonClusterState(mstate);
  var rbody  = {status : "OK"};
  respond_200(response, rbody);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REGISTER ------------------------------------------------------------------

function set_new_user_permissions(username, password, next) {
  AdminCli.AddUser(username, password, 'USER', function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var uchanid = get_user_channel(username);
      AdminCli.GrantUser(username, uchanid, 'WRITE', function(uerr, ures) {
        if (uerr) next(uerr, null);
        else {
          var fchanid = get_follow_channel(username);
          AdminCli.GrantUser(username, fchanid, 'WRITE', function(ferr, fres) {
            if (ferr) next(ferr, null);
            else {
              var tchanid = get_tweets_channel(username);
              // NOTE: USER WRITE PRIVILIGES ON TWEETS
              AdminCli.GrantUser(username, tchanid, 'WRITE',
              function(aerr, ares) {
                if (aerr) next(aerr, null);
                else {
                  // NOTE: WILDCARD READ PRIVILIGES ON TWEETS
                  AdminCli.GrantUser(ZH.WildCardUser, tchanid, 'READ', next);
                }
              });
            }
          });
        }
      });
    }
  });
}

function add_user_info_doc(username, next) {
  var cname = UserCollectionName;
  var kname = username;
  var doc   = {_channels         : [get_user_channel(username)],
               username          : username,
               registration_date : ZH.GetMsTime(),
               num_tweets        : 0,
               num_following     : 0,
               num_followers     : 0
              };
  do_store(cname, kname, doc, next);
}

function add_following_doc(username, next) {
  var cname = FollowingCollectionName;
  var kname = username;
  var doc   = {_channels : [get_follow_channel(username)],
               following : create_large_list()
              };
  do_store(cname, kname, doc, next);
}

function add_follower_doc(username, next) {
  var cname = FollowerCollectionName;
  var kname = username;
  var doc   = {_channels : [get_follow_channel(username)],
               followers : create_large_list()
              };
  do_store(cname, kname, doc, next);
}

function add_my_tweets_doc(username, next) {
  var cname = TweetCollectionName;
  var kname = username;
  var doc   = {_channels      : [get_tweets_channel(username)],
               _server_filter : "TweetServerFilter",
               tweets         : create_large_list()
              };
  do_store(cname, kname, doc, next);
}

function create_all_user_docs(username, next) {
  add_user_info_doc(username, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      add_following_doc(username, function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          add_follower_doc(username, function(aerr, ares) {
            if (aerr) next(aerr, null);
            else      add_my_tweets_doc(username, next);
          });
        }
      });
    }
  });
}

function do_register(data, response) {
  var mbody    = data.body;
  var username = mbody.username;
  var password = mbody.password;
  ZH.l('do_register: UN: ' + username);
  var cname    = UserCollectionName;
  AdminCli.SwitchUser(AdminAuth, function(werr, wres) {
    if (werr) next(werr, null);
    else { // CHECKS if username is taken
      do_sticky_cache(cname, username, function(gerr, zdoc) {
        if (gerr) respond_404(response);
        else {
          if (zdoc) {
            var etxt  = 'Username: "' + username + '" TAKEN';
            var rbody = {error  : etxt,
                         status : "FAIL"};
            respond_200(response, rbody);
          } else {
            ZH.l('OK: REGISTRATION WORKED');
            set_new_user_permissions(username, password, function(serr, sres) {
              if (serr) respond_404(response);
              else {
                create_all_user_docs(username, function(aerr, ares) {
                  if (aerr) respond_404(response);
                  else {
                    var rbody = {status : "OK"};
                    respond_200(response, rbody);
                  }
                });
              }
            });
          }
        }
      });
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOGIN ---------------------------------------------------------------------

//TODO need.done loop for parallelism
function cache_tweets_for_stream(minage, unames, stream, next) {
  if (unames.length === 0) next(null, stream);
  else {
    var ousername = unames.shift();
    var cname     = TweetCollectionName;
    do_cache(cname, ousername, function(gerr, zdoc) {
      if (gerr) next(gerr, null);
      else {
        var json   = zdoc._;
        var tweets = json.tweets;
        for (var i = 0; i < tweets.length; i++) {
          var tweet  = tweets[i];
          var sentry = decode_tweet(tweet);
          var ok     = minage ? (sentry.when > minage) : true;
          if (ok) {
            sentry.username = ousername;
            stream.push(sentry);
          }
        }
        setImmediate(cache_tweets_for_stream, minage, unames, stream, next);
      }
    });
  }
}

function fetch_repeat_login_stream(duuid, minage, next) {
  AdminCli.GetAgentKeys(duuid, NumGetAgentKeys, minage, true,
  function(derr, dres) {
    if (derr) next(derr, null);
    else {
      var kss    = dres.kss;
      var stream = [];
      if (kss.length === 0) next(null, stream);
      else {
        var unames = [];
        for (var i = 0; i < kss.length; i++) {
          var ks  = kss[i];
          var cks = ZH.ParseKQK(ks.kqk); // missing [ns,cn,key]
          unames.push(cks.key);
        }
        cache_tweets_for_stream(minage, unames, stream, next);
      }
    }
  });
}

function fetch_stream(username, duuid, minage, ldata, next) {
  if (!minage) { // BOOTSTRAP DEVICE LOGIN
    var unames = ZH.clone(ldata.following);
    unames.unshift(username);
    var stream = [];
    cache_tweets_for_stream(minage, unames, stream, function(ferr, stream) {
      if (ferr) next(ferr, null);
      else {
        ldata.stream = stream; 
        next(null, ldata);
      }
    });
  } else {       // REPEAT LOGIN FROM DEVICE
    fetch_repeat_login_stream(duuid, minage, function(ferr, stream) {
      if (ferr) next(ferr, null);
      else {
        ldata.stream = stream;
        next(null, ldata);
      }
    });
  }
}

function fetch_login_data(username, duuid, minage, next) {
  var ldata = {};
  var cname = FollowingCollectionName;
  do_sticky_cache(cname, username, function(gerr, zdoc) {
    if (gerr) next(gerr, null);
    else {
      var json        = zdoc._;
      ldata.following = decode_many_follow(json.following);
      var cname       = FollowerCollectionName;
      do_sticky_cache(cname, username, function(gerr, zdoc) {
        if (gerr) next(gerr, null);
        else {
          var json        = zdoc._;
          ldata.followers = decode_many_follow(json.followers);
          var cname       = TweetCollectionName;
          do_sticky_cache(cname, username, function(gerr, zdoc) {
            if (gerr) next(gerr, null);
            else {
              var json     = zdoc._;
              ldata.tweets = decode_many_tweets(json.tweets);
              fetch_stream(username, duuid, minage, ldata, next);
            }
          });
        }
      });
    }
  });
}

function do_login(data, response) {
  var mbody      = data.body;
  var duuid      = mbody.device.uuid;
  var username   = mbody.username;
  var password   = mbody.password;
  var max_ts_len = mbody.max_tweet_stream;
  var max_mt_len = mbody.max_my_tweets;
  var minage     = mbody.last_update;
  ZH.l('do_login: U: ' + duuid + ' UN: ' + username);
  var cname      = UserCollectionName;
  AdminCli.SwitchUser(AdminAuth, function(werr, wres) {
    if (werr) next(werr, null);
    else { // CHECKS if username exists
      do_sticky_cache(cname, username, function(gerr, zdoc) {
        if (gerr) respond_404(response);
        else {
          if (!zdoc) {
            var etxt  = 'Invalid Username/Password';
            var rbody = {error  : etxt,
                         status : "FAIL"};
            respond_200(response, rbody);
          } else {
            ZH.l('OK: LOGIN WORKED');
            var json  = zdoc._;
            var uinfo = {name          : username,
                         num_tweets    : json.num_tweets,
                         num_following : json.num_following,
                         num_followers : json.num_followers
                        };
            fetch_login_data(username, duuid, minage, function(ferr, ldata) {
              if (ferr) respond_404(response);
              else {
                trim_if_too_big(ldata.tweets, max_mt_len, cmp_tweet_stream);
                trim_if_too_big(ldata.stream, max_ts_len, cmp_tweet_stream);
                var rbody = {user_info : uinfo,
                             following : ldata.following,
                             followers : ldata.followers,
                             tweets    : ldata.tweets,
                             stream    : ldata.stream,
                             status    : "OK"};
                respond_200(response, rbody);
              }
            });
          }
        }
      });
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FOLLOW --------------------------------------------------------------------

function add_following(username, following, next) {
  do_cache(FollowingCollectionName, username, function(gerr, zdoc) {
    if (gerr) respond_404(response);
    else {
      if (!zdoc) next(new Error("INTERNAL SERVER ERROR"), null);
      else {
        ZH.l('add_following: U: ' + username + ' -> ' + following);
        var uentry = encode_follow(ZH.GetMsTime(), following);
        var oerr   = zdoc.rpush("following", uentry);
        if (oerr) next(oerr, null);
        else      zdoc.commit(next);
      }
    }
  });
}

function add_follower(following, username, next) {
  do_sticky_cache(FollowerCollectionName, following, function(gerr, zdoc) {
    if (gerr) respond_404(response);
    else {
      if (!zdoc) next(new Error("INTERNAL SERVER ERROR"), null);
      else {
        ZH.l('add_followers: U: ' + following + ' <- ' + username);
        var uentry = encode_follow(ZH.GetMsTime(), username);
        var oerr   = zdoc.rpush("followers", uentry);
        if (oerr) next(oerr, null);
        else      zdoc.commit(next);
      }
    }
  });
}

function do_follow(data, response) {
  var mbody     = data.body;
  var username  = mbody.username;
  var following = mbody.following;
  ZH.l('do_follow: U: ' + username + ' F: ' + following);
  var cname     = UserCollectionName;
  AdminCli.SwitchUser(AdminAuth, function(werr, wres) {
    if (werr) next(werr, null);
    else {
      do_cache(cname, following, function(gerr, zdoc) {// CHECKS if user exists
        if (gerr) respond_404(response);
        else {
          if (!zdoc) {
            var etxt  = 'User: "' + following + '" does not exist';
            var rbody = {error  : etxt,
                         status : "FAIL"};
            respond_200(response, rbody);
          } else {
            ZH.l('OK: FOLLOW USER EXISTS');
            var oerr = zdoc.incr("num_followers", 1);
            if (oerr) respond_404(response);
            else {
              zdoc.commit(function(cerr, cres) {
                if (cerr) respond_404(response);
                else {
                  add_follower(following, username, function(aerr, ares) {
                    if (aerr) respond_404(response);
                    else {
                      add_following(username, following, function(aerr, ares) {
                        if (aerr) respond_404(response);
                        else {
                          var rbody = {status : "OK"};
                          respond_200(response, rbody);
                        }
                      });
                    }
                  });
                }
              });
            }
          }
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PROFILE -------------------------------------------------------------------

function cache_array_field(cname, kname, fname, next) {
  var collection = AdminCli.Collection(cname);
  collection.cache(kname, function(gerr, zdoc) {
    if (gerr) {
      var notfound = (gerr.message === ZS.Errors.NoDataFound);
      if (notfound) next(null, []);
      else          next(gerr, null);
    } else {
      var val = zdoc._[fname];
      next(null, val);
    }
  });
}

function do_profile(data, response) {
  var next     = reply_message;
  var suuid    = data.sender.device_uuid;
  var mbody    = data.body;
  var username = mbody.username;
  ZH.e('do_profile: U: ' + username);
  AdminCli.SwitchUser(AdminAuth, function(werr, wres) {
    if (werr) next(werr, null);
    else {
      cache_array_field(TweetCollectionName, username, 'tweets',
      function(ferr, tweets) {
        if (ferr) next(ferr, suuid);
        else {
          cache_array_field(FollowingCollectionName, username, 'following',
          function(gerr, following) {
            if (gerr) next(gerr, suuid);
            else {
              cache_array_field(FollowerCollectionName, username, 'followers',
              function(cerr, followers) {
                if (cerr) next(cerr, suuid);
                else {
                  var rbody = {action    : "profile",
                               status    : "OK",
                               username  : username,
                               tweets    : tweets,
                               following : following,
                               followers : followers};
                  next(null, suuid, rbody);
                  // NOTE: send_200_message_response() is ASYNC
                  send_200_message_response(response);
                }
              });
            }
          });
        }
      });
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGGREGATIONS --------------------------------------------------------------

function do_aggregations(data, response) {
  var next     = reply_message;
  var suuid    = data.sender.device_uuid;
  var mbody    = data.body;
  var username = mbody.username;
  ZH.e('do_aggregations: U: ' + username);
  var ns       = Nsname;
  var cn       = GlobalAggCollName;
  var key      = GlobalAggKeyName;
  AdminCli.SwitchUser(AdminAuth, function(werr, wres) {
    if (werr) next(werr, null);
    else {
      AdminCli.MemcacheFetch(ns, cn, key, "num_tweets",
      function(gerr, ntweets) {
        if (gerr) next(gerr, null);
        else {
          var cn  = UserAggCollName;
          var key = username;
          AdminCli.MemcacheFetch(ns, cn, key, "num_tweets_sent",
          function(ferr, nmsent) {
            if (ferr) next(ferr, null);
            else {
              var rbody = {action              : "aggregations",
                           status              : "OK",
                           num_total_tweets    : ntweets,
                           num_my_tweet_fanout : nmsent};
              next(null, suuid, rbody);
              // NOTE: send_200_message_response() is ASYNC
              send_200_message_response(response);
            }
          });
        }
      });
    }
  });
}

