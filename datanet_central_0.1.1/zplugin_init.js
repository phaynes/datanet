
var ZPlugins = require('./plugins/plugins');
var ZH       = require('./zhelper');

// NOTE: code is very similar to ZDQ.init_queue_plugin() -> REFACTOR?

function init_collections(me, plugin, db_handle, cname, dbcname, next) {
  var do_coll = plugin.do_create_collection;
  me[cname]   = do_coll(db_handle, dbcname, true);
  next(null, null);
}

exports.InitPlugin = function(me, db, info, next) {
  var plugin   = ZPlugins.GetPlugin(db.name);
  plugin.ip    = db.ip;
  plugin.port  = db.port;
  plugin.xname = info.xname;
  plugin.do_open(plugin.ip, plugin.port, info.namespace, plugin.xname,
  function(cerr, db_handle){
    if (cerr) next(cerr, null);
    else {
      me.db_handle = db_handle;
      me.plugin    = plugin;
      ZH.e('PLUGIN CONNECTED TO DB (' + plugin.xname + '): IP: ' +
           plugin.ip + ' P: ' + plugin.port);
      init_collections(me, plugin, db_handle, info.cname, info.dbcname,
      function(ierr, ires) {
        next(ierr, plugin);
      });
    }
  });
}

exports.InitPluginConnection = function(db, info, next) {
  var me = this;
  exports.InitPlugin(me, db, info, function(ierr, ires) {
    next(ierr, me);
  });
}

