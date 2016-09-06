
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REQUIRES ------------------------------------------------------------------

var https      = require('https');
var fs         = require('fs');

var ZCmdClient = require('./zcmd_client');
var ZS         = require('./zshared');
var ZH         = require('./zhelper');

ZH.LogToConsole = true;
ZH.ZyncRole     = 'TWEET_AGGREGATOR';
ZH.MyUUID       = 'TWEET_AGGREGATOR';

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

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HTTPS SERVER --------------------------------------------------------------

var Hport    = Cjson.server.port;
var Hoptions = {
  key  : fs.readFileSync('./ssl/server.private.key'),
  cert : fs.readFileSync('./ssl/server.crt')
};

function respond_200(response, rbody) {
  ZH.e('SEND'); ZH.e(rbody);
  var btxt = JSON.stringify(rbody);
  response.writeHead(200);
  response.write(btxt);
  response.end();
}

function respond_404(response) {
  ZH.l('respond_404');
  response.writeHead(404);
  response.write("INVALID MESSAGE/REQUEST");
  response.end();
}

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
  var mbody = data.body;
  ZH.l('RECIEVED'); ZH.p(mbody);
  var action = mbody.action;
  if      (action === "tweet") aggregate_tweet(data, response);
  else                         respond_404(response);
}

function init_https_server() {
  ZH.l('Listening on https://localhost:' + Hport);
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
// MEMCACHE CLIENT -----------------------------------------------------------

var Mip   = Cjson.agent.ip;
var Mport = Cjson.agent.port;
ZH.l('Connecting to agent: ip: ' + Mip + ' port: ' + Mport);

var Username = 'tester';
var Password = 'password';
var Nsname   = 'production';
var Cli      = ZCmdClient.ConstructCmdClient('ADMIN_APP_SERVER');

Cli.SetAgentAddress(Mip, Mport);
Cli.SetClientAuthentication(Username, Password);
Cli.SetNamespace(Nsname);

var GlobalAggCollName = 'aggregations';
var GlobalAggKeyName  = 'tweets';

var UserAggCollName   = 'user_aggregations';
var UserAggRepChans   = [7];

function init_data_client(next) {
  var clname = Cjson.memcache_cluster.name;
  Cli.GetMemcacheClusterState(clname, function(gerr, mstate) {
    if (gerr) next(gerr, null);
    else {
      init_https_server();
      next(null, null);
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STARTUP -------------------------------------------------------------------

init_data_client(ZH.OnErrLog);

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGGREGATE -----------------------------------------------------------------

function update_global_aggregations(next) {
  var collection = Cli.Collection(GlobalAggCollName);
  var key        = GlobalAggKeyName;
  collection.memcache_create(key, function(cerr, zdoc) {
    if (cerr) next(cerr, null);
    else {
      zdoc.set_channels(UserAggRepChans);
      zdoc.incr("num_tweets", 1);
      var sep = true;
      var ex  = 0;
      zdoc.memcache_commit(sep, ex, next);
    }
  });
}

function update_user_aggregations(username, slen, next) {
  var collection = Cli.Collection(UserAggCollName);
  var key        = username;
  collection.memcache_create(key, function(cerr, zdoc) {
    if (cerr) next(cerr, null);
    else {
      zdoc.set_channels(UserAggRepChans);
      zdoc.incr("num_tweets_sent", slen);
      var sep = true;
      var ex  = 0;
      zdoc.memcache_commit(sep, ex, next);
    }
  });
}

function aggregate_tweet(data, response) {
  var mbody    = data.body;
  var subs     = mbody.subs;
  var username = mbody.username;
  var tweet    = mbody.tweet;
  var suuids   = subs.uuids;
  var slen     = suuids.length;
  ZH.l('aggregate_tweet: U: ' + username + ' T: ' + tweet.text);
  update_global_aggregations(function(serr, sres) {
    if (serr) respond_404(response);
    else {
      update_user_aggregations(username, slen, function(uerr, ures) {
        if (uerr) respond_404(response);
        else {
          var rbody = {status : "OK"};
          respond_200(response, rbody);
        }
      });
    }
  });
}

