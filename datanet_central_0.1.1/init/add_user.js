
var ZS       = require('../zshared');
var ZH       = require('../zhelper');
var ZAuth    = require('../zauth');
var ZDBP     = require('../zdb_plugin');

var RedisPort   = process.argv[2];
var DataCenter  = process.argv[3];
var DeviceUUID  = process.argv[4];
var Username    = process.argv[5];
var Password    = process.argv[6];
var Role        = process.argv[7];
var ClusterRole = process.argv[8];

if (RedisPort == 0) ZDBP.SetPlugin('MONGODB');
else                ZDBP.SetPlugin('REDIS');

if (typeof(DataCenter)  === 'undefined') Usage();
if (typeof(DeviceUUID)  === 'undefined') Usage();
if (typeof(Username)    === 'undefined') Usage();
if (typeof(Password)    === 'undefined') Usage();
if (typeof(Role)        === 'undefined') Usage();
if (typeof(ClusterRole) === 'undefined') Usage();

ZDBP.plugin.ip = "127.0.0.1";
if (RedisPort != 0) ZDBP.plugin.port = RedisPort;
else                ZDBP.plugin.port = 27017; // MONGO default

ZDBP.SetDeviceUUID(DeviceUUID);
ZH.AmCentral = true;
ZH.AmRouter  = (ClusterRole === "ROUTER");
ZH.AmStorage = (ClusterRole === "STORAGE");
ZDBP.SetDataCenter(DataCenter);

ZH.l('add_user.js ROLE: ' + Role + ' ClusterRole: ' + ClusterRole);

function Usage(err) {
  if (typeof(err) !== 'undefined') console.log('ERROR: ' + err);
  console.log('Usage: ' + process.argv[0] + ' ' + process.argv[1] +
               ' datacenter clusternode-uuid username password' +
               ' role cluster-role');
  process.exit(-1);
}


var Script = this;
Script.net = {};

var Namespace    = 'datastore';
var InitCollName = 'testdata';

ZDBP.PluginConnect(Namespace, function(cerr, zhndl) {
  if (cerr) throw cerr;
  Script.net.zhndl       = zhndl;
  Script.net.plugin      = zhndl.plugin;
  Script.net.db_handle   = zhndl.db_handle;
  Script.net.collections = zhndl.collections;
  ZDBP.AdminConnect(true, function(aerr, ares) {
    if (aerr) throw aerr;
    var plugin      = Script.net.plugin;
    var collections = Script.net.collections;
    var auth        = {username : Username,
                       password : Password};
    console.log('ADD-USER: ' + Username);
    ZAuth.StoreUserAuthentication(plugin, collections, auth, Role,
    function(err, res) {
      ZDBP.Close();
      if (err) throw(err);
      process.exit(0);
    });
  });
});

