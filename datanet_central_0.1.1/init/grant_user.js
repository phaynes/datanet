
var ZS       = require('../zshared');
var ZH       = require('../zhelper');
var ZUM      = require('../zuser_management');
var ZDBP     = require('../zdb_plugin');

var RedisPort   = process.argv[2];
var DataCenter  = process.argv[3];
var DeviceUUID  = process.argv[4];
var Username    = process.argv[5];
var Channel     = process.argv[6];
var Perms       = process.argv[7];
var ClusterRole = process.argv[8];

if (RedisPort == 0) ZDBP.SetPlugin('MONGODB');
else                ZDBP.SetPlugin('REDIS');

if (typeof(DataCenter)  === 'undefined') Usage();
if (typeof(DeviceUUID)  === 'undefined') Usage();
if (typeof(Username)    === 'undefined') Usage();
if (typeof(Channel)     === 'undefined') Usage();
if (typeof(Perms)       === 'undefined') Usage();
if (typeof(ClusterRole) === 'undefined') Usage();

ZDBP.plugin.ip = "127.0.0.1";
if (RedisPort != 0) ZDBP.plugin.port = RedisPort;
else                ZDBP.plugin.port = 27017; // MONGO default

ZDBP.SetDeviceUUID(DeviceUUID);
ZH.AmCentral = true;
ZH.AmRouter  = (ClusterRole === "ROUTER");
ZH.AmStorage = (ClusterRole === "STORAGE");
ZDBP.SetDataCenter(DataCenter);

ZH.l('grant_user.js PERMS: ' + Perms + ' ClusterRole: ' + ClusterRole);

function Usage(err) {
  if (typeof(err) !== 'undefined') console.log('ERROR: ' + err);
  console.log('Usage: ' + process.argv[0] + ' ' + process.argv[1] +
               ' datacenter clusternode-uuid username channel' +
               ' perms cluster-role');
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
    console.log('GRANT-USER: ' + Username + ' R: ' + Channel);
    ZUM.GrantUser(plugin, collections, Username, Channel, Perms,
    function(err, res) {
      ZDBP.Close();
      if (err) throw(err);
      process.exit(0);
    });
  });
});

