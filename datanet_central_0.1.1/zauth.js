"use strict";

var ZAio, ZMDC, ZDS, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZAio     = require('./zaio');
  ZMDC     = require('./zmemory_data_cache');
  ZDS      = require('./zdatastore');
  ZQ       = require('./zqueue');
  ZS       = require('./zshared');
  ZH       = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD-USER HELPERS ----------------------------------------------------------

function get_rchans(n, ks, next) {
  ZMDC.GetKeyRepChans(n.plugin, n.collections, ks, function(gerr, rchans) {
    if (gerr) next(gerr, null);
    else {
      if (!rchans) next(new Error(ZS.Errors.NoDataFound), null);
      else         next(null, rchans);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

exports.StoreUserAuthentication = function(plugin, collections,
                                           auth, role, next) {
  ZH.HashPassword(auth.password, function(herr, phash) {
    if (herr) next(herr, null);
    else {
      var pkey    = ZS.GetUserAuthentication(auth.username);
      var p_entry = { _id : pkey, phash : phash, role : role};
      plugin.do_set(collections.uauth_coll, pkey, p_entry, next);
    }
  });
}

function authenticate(plugin, collections, auth, next) {
  ZH.l('authenticate: UN: ' + auth.username);
  var pw = ZH.UserPasswordMap[auth.username];
  if (pw) {
    var ok = (pw === auth.password);
    next(null, ok);
  } else if (!collections || !collections.uauth_coll) {
    next(null, null);
  } else {
    var ukey = ZS.GetUserAuthentication(auth.username);
    plugin.do_get_field(collections.uauth_coll, ukey, 'phash',
    function(perr, db_phash) {
      if (perr) next(perr, null);
      else {
        if (db_phash === null) next(null, null); // NO-ENTRY -> SendCentral
        else {
          // NOTE: this is DOG-slow in the browser, In-memory maps to avoid it
          var ok = ZH.ComparePasswordHash(auth.password, db_phash);
          if (ok) ZH.UserPasswordMap[auth.username] = auth.password;
          next(null, ok);
        }
      }
    });
  }
}

function admin_authenticate(plugin, collections, auth, next) {
  ZH.l('admin_authenticate: UN: ' + auth.username);
  var role = 'ADMIN';
  authenticate(plugin, collections, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(null, ok);   // ok may be NULL or FALSE
    else {
      var ukey = ZS.GetUserAuthentication(auth.username);
      plugin.do_get_field(collections.uauth_coll, ukey, 'role',
      function(perr, db_role) {
        if (perr) next(perr, null);
        else {
          if (db_role === null) next(null, null); // NO-ENTRY -> SendCentral
          else {
            if (db_role === role) next(null, true);
            else                  next(null, false);
          }
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEFAULT BACKDOOR ----------------------------------------------------------

var DefaultChannelID = "0";

function check_default_backdoor(schanid) {
  if (schanid !== DefaultChannelID) return false;
  ZH.l('check_default_backdoor: TRUE');
  return true;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BASIC AUTHENTICATION ------------------------------------------------------

exports.BasicAuthentication = function(n, auth, next) {
  authenticate(n.plugin, n.collections, auth, function(aerr, ok) {
    if      (aerr)        next(aerr, null);
    else if (ok !== null) next(null, ok);
    else {
      if (ZH.AmCentral) next(null, false);
      else { // BROWSER/AGENT has no info for this user -> Fetch from Central
        if        (ZH.Isolation) {
          next(new Error(ZS.Errors.AuthIsolation), null);
        } else if (!ZH.Agent.cconn) {
          next(new Error(ZS.Errors.AuthNoCentralConn), null);
        } else {
          ZH.l('AUTHENTICATE @ CENTRAL');
          ZAio.SendCentralAgentAuthenticate(n.plugin, n.collections, auth,
          function(gerr, dres) {
            if (gerr) next(gerr, null);
            else {
              var ok = dres.ok;
              if (!ok) next(null, false);
              else { // STORE phash locally in uauth_coll
                var role = dres.role;
                exports.StoreUserAuthentication(n.plugin, n.collections,
                                                auth, role,
                function(perr, pres) {
                  if (perr) next(perr, null);
                  else      next(null, true);
                });
              }
            }
          });
        }
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN AUTHENTICATION ------------------------------------------------------

exports.AdminAuthentication = function(n, auth, next) {
  ZH.l('AdminAuthentication: U: ' + auth.username);
  admin_authenticate(n.plugin, n.collections, auth, function(aerr, ok) {
    if      (aerr)        next(aerr, null);
    else if (ok !== null) next(null, ok);
    else {
      if (ZH.AmCentral) next(null, false);
      else { // BROWSER/AGENT has no info for this user -> Fetch from Central
        if        (ZH.Isolation) {
          next(new Error(ZS.Errors.AuthIsolation), null);
        } else if (!ZH.Agent.cconn) {
          next(new Error(ZS.Errors.AuthNoCentralConn), null);
        } else {
          ZH.l('ADMIN AUTHENTICATE @ CENTRAL');
          ZAio.SendCentralAdminAuthenticate(n.plugin, n.collections, auth,
          function(derr, dres) {
            if (derr) next(derr, null);
            else {
              var ok = dres.ok;
              if (!ok) next(null, false);
              else { // STORE phash locally in uauth_coll
                var role = dres.role;
                exports.StoreUserAuthentication(n.plugin, n.collections,
                                                auth, role,
                function(perr, pres) {
                  if (perr) next(perr, null);
                  else {
                    admin_authenticate(n.plugin, n.collections, auth, next);
                  }
                });
              }
            }
          });
        }
      }
    }
  });
}



// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WILDCARD PERMISSIONS ------------------------------------------------------

function get_wildcard_perms(plugin, collections, schanid, next) {
  var wkey = ZS.GetUserChannelPermissions(ZH.WildCardUser);
  ZH.l('get_wildcard_perms: PK: ' + wkey + ' R: ' + schanid);
  plugin.do_get_field(collections.uperms_coll, wkey, schanid, next);
}

function do_get_rchans_wildcard_perms(plugin, collections,
                                      rperms, rchans, next) {
  if (rchans.length === 0) next(null, rperms);
  else {
    var rchan = rchans.shift();
    get_wildcard_perms(plugin, collections, rchan, function(gerr, perms) {
      if (gerr) next(gerr, null);
      else {
        if (perms === 'W') next(null, 'W'); // 'W' is dominant
        else {
          if (perms === 'R') rperms = 'R';  // 'R' is recessive
          setImmediate(do_get_rchans_wildcard_perms, plugin, collections,
                       rperms, rchans, next);
        }
      }
    });
  }
}

function get_rchans_wildcard_perms(plugin, collections, rchans, next) {
  var rperms  = null;
  var crchans = ZH.clone(rchans);// crchans[] gets shift()ed
  do_get_rchans_wildcard_perms(plugin, collections, rperms, crchans, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET DATA/SUBSCRIBE PERMISSIONS --------------------------------------------

function get_key_to_user_perms(plugin, collections, rchan, auth, next) {
  ZH.l('get_key_to_user_perms R: ' + rchan + ' UN: ' + auth.username);
  var pkey = ZS.GetUserChannelPermissions(auth.username);
  plugin.do_get_field(collections.uperms_coll, pkey, rchan, 
  function(gerr, perms) {
    if (gerr) next(gerr, null);
    else {
      if (perms) next(null, perms);
      else {
        get_wildcard_perms(plugin, collections, rchan, function(aerr, operms) {
          if (aerr) next(aerr, null);
          else {
            if (!operms) next(null, null);
            else {
              if (operms === "R*") next(null, "R");
              else                 next(null, "W");
            }
          }
        });
      }
    }
  });
}

// CENTRAL just needs straight permissions
// NOTE: used in exports.HasAgentStorePermissions()
function get_simple_rchan_data_perms(plugin, collections,
                                     ks, duuid, rchan, auth, next) {
  ZH.l('get_simple_rchan_data_perms: K: ' + ks.kqk + ' R: ' + rchan +
       ' UN: ' + auth.username + ' U: ' + duuid);
  if (check_default_backdoor(rchan)) next(null, "W");
  else {
    get_key_to_user_perms(plugin, collections, rchan, auth,
    function (kerr, kperms) {
      if (kerr) next(kerr, null);
      else {
        if (kperms) next(null, kperms);
        else {
          var pkey = ZS.GetUserChannelSubscriptions(auth.username);
          plugin.do_get_field(collections.usubs_coll, pkey, rchan, next);
        }
      }
    });
  }
}

// AGENT needs permission and SUBSCRIBED or CACHED
function get_agent_rchan_data_perms(plugin, collections,
                                    ks, rchan, auth, next) {
  var username = auth.username;
  ZH.l('get_agent_rchan_data_perms: K: ' + ks.kqk + ' R: ' + rchan +
       ' UN: ' + username);
  if (check_default_backdoor(rchan)) next(null, "W");
  else {
    var pkey = ZS.GetUserChannelSubscriptions(username);
    plugin.do_get_field(collections.usubs_coll, pkey, rchan,
    function(gerr, usubs) {
      if (gerr) next(gerr, null);
      else {
        if (usubs) next(null, usubs); // SUBSCRIBED
        else {
          var auk_key = ZS.GetAgentUserToCachedKeys(username);
          plugin.do_get_field(collections.key_coll, auk_key, ks.kqk,
          function (kerr, kres) {
            if (kerr) next(kerr, null);
            else {
              if (!kres) next(null, null); // NOT CACHED & NOT SUBSCRIBED
              else {
                get_key_to_user_perms(plugin, collections, rchan, auth, next);
              }
            }
          });
        }
      }
    });
  }
}

function get_rchan_data_perms(plugin, collections,
                              ks, duuid, rchan, simple, auth, next) {
  if (simple) {
    get_simple_rchan_data_perms(plugin, collections,
                                 ks, duuid, rchan, auth, next);
  } else {
    get_agent_rchan_data_perms(plugin, collections, ks, rchan, auth, next);
  }
}

function get_all_write_perms(plugin, collections, rperms,
                             ks, duuid, rchans, simple, auth, next) {
  if (rchans.length === 0) next(null, rperms);
  else {
    var rchan = rchans.shift();
    get_rchan_data_perms(plugin, collections, ks, duuid, rchan, simple, auth,
    function(gerr, perms) {
      if (gerr) next(gerr, null);
      else {
        if (!perms) next(null, null); // ALL or NOTHING
        else {
          if      (perms === 'R') rperms = 'R'; // 'R' is dominant
          else if (!rperms)       rperms = 'W'; // 'W' is recessive
          setImmediate(get_all_write_perms, plugin, collections,
                       rperms, ks, duuid, rchans, simple, auth, next);
        }
      }
    });
  }
}

function get_rw_perms(plugin, collections, rperms,
                      ks, duuid, rchans, simple, auth, next) {
  if (rchans.length === 0) next(null, rperms);
  else {
    var rchan = rchans.shift();
    get_rchan_data_perms(plugin, collections, ks, duuid, rchan, simple, auth,
    function(gerr, perms) {
      if (gerr) next(gerr, null);
      else {
        if (perms === 'W') next(null, 'W'); // 'W' is dominant
        else {
          if (perms === 'R') rperms = 'R';  // 'R' is recessive
          setImmediate(get_rw_perms, plugin, collections,
                       rperms, ks, duuid, rchans, simple, auth, next);
        }
      }
    });
  }
}

function get_data_perms(plugin, collections,
                        ks, duuid, rchans, all, simple, auth, next) {
  var rperms  = null;
  var crchans = ZH.clone(rchans); // crchans[] will get shift()ed
  if (all) {
    get_all_write_perms(plugin, collections,
                        rperms, ks, duuid, crchans, simple, auth, next);
  } else {
    get_rw_perms(plugin, collections, 
                 rperms, ks, duuid, crchans, simple, auth, next);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// READ & WRITE PERMISSIONS --------------------------------------------------

exports.HasReadPermissions = function(n, auth, ks, duuid, next) {
  ZH.l('HasReadPermissions: K: ' + ks.kqk);
  exports.AdminAuthentication(n, auth, function(aerr, ok) {
    if (aerr) next(aerr, null);
    else {
      if (ok) next(null, ok);
      else {
        authenticate(n.plugin, n.collections, auth, function(aerr, ok) {
          if      (aerr) next(aerr, null);
          else if (!ok)  next(new Error(ZS.Errors.AuthError), null);
          else {
            get_rchans(n, ks, function(gerr, rchans) {
              if (gerr) next(gerr, null);
              else {
                var all    = false;
                var simple = ZH.AmCentral;
                get_data_perms(n.plugin, n.collections,
                               ks, duuid, rchans, all, simple, auth,
                function(perr, perms) {
                  if      (perr)           next(perr, null);
                  else if (perms === null) next(null, false);
                  else                     next(null, true); // BOTH [R,W]
                });
              }
            });
          }
        });
      }
    }
  });
}

exports.HasWritePermissions = function(n, auth, ks, duuid, rchans, next) {
  if (!rchans) rchans = [];
  ZH.l('HasWritePermissions: R: ' + rchans);
  exports.AdminAuthentication(n, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (ok)   next(null, ok);
    else {
      authenticate(n.plugin, n.collections, auth, function(aerr, ok) {
        if      (aerr) next(aerr, null);
        else if (!ok)  next(new Error(ZS.Errors.AuthError), null);
        else {
          var all    = true;
          var simple = ZH.AmCentral;
          get_data_perms(n.plugin, n.collections,
                         ks, duuid, rchans, all, simple, auth,
          function(perr, perms) {
            if      (perr)           next(perr, null);
            else if (perms === null) next(null, false);
            else {
              if (perms === 'W') next(null, true);
              else               next(null, false);
            }
          });
        }
      });
    }
  });
}

exports.HasWritePermissionsOnKey = function(n, auth, ks, duuid, next) {
  get_rchans(n, ks, function(gerr, rchans) {
    if (gerr) next(gerr, null);
    else      exports.HasWritePermissions(n, auth, ks, duuid, rchans, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBE PERMISSIONS -----------------------------------------------------

function get_subscribe_perms(plugin, collections,
                             auth, schanid, is_agent, next) {
  ZH.l('get_subscribe_perms: R: ' + schanid + ' UN: ' + auth.username);
  var pkey = ZS.GetUserChannelPermissions(auth.username);
  plugin.do_get_field(collections.uperms_coll, pkey, schanid,
  function(gerr, perms) {
    if (gerr) next(gerr, null);
    else {
      if (is_agent) next(null, perms);
      else {
        if (perms) next(null, perms);
        else       get_wildcard_perms(plugin, collections, schanid, next)
      }
    }
  });
}

exports.HandleCentralHasSubscriberPermissions = function(plugin, collections,
                                                         auth, schanid, 
                                                         hres, next) {
  get_subscribe_perms(plugin, collections, auth, schanid, false,
  function(gerr, ok) {
    if (gerr) next(gerr, hres);
    else {
      hres.ok = ok; // Used in response
      next(null, hres);
    }
  });
}

function has_subscribe_permissions(n, auth, schanid, is_agent, next) {
  exports.AdminAuthentication(n, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (ok)   next(null, ok);
    else {
      authenticate(n.plugin, n.collections, auth, function(aerr, ok) {
        if      (aerr) next(aerr, null);
        else if (!ok)  next(new Error(ZS.Errors.AuthError), null);
        else {
          get_subscribe_perms(n.plugin, n.collections, auth, schanid, is_agent,
          function(perr, perms) {
            if (perr) next(perr, null);
            else {
              if (perms) next(null, true);
              else {
                if (!is_agent) next(null, false);
                else { // AGENT asks CENTRAL, may be a PUBLIC channel
                  ZH.l('HAS-SUBSCRIBE-PERMISSIONS @ CENTRAL');
                  var sfunc = ZAio.SendCentralAgentHasSubscribePermissions;
                  sfunc(n.plugin, n.collections, schanid, auth,
                  function(derr, dres) {
                    if (derr) next(derr, null);
                    else {
                      var ok = dres.ok;
                      next(null, ok);
                    }
                  });
                }
              }
            }
          });
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HAS AGENT STORE PERMISSIONS -----------------------------------------------

function persist_user_chan_permissions(plugin, collections, ps, next) {
  if (ps.length === 0) next(null, null);
  else {
    var p        = ps.shift();
    var username = p.username;
    var rchan    = p.channel.id;
    var priv     = p.channel.permissions;
    ZH.l('persist_uchanperms: U: ' + username + ' R: ' + rchan + ' P: ' + priv);
    var pkey     = ZS.GetUserChannelPermissions(username);
    plugin.do_set_field(collections.uperms_coll, pkey, rchan, priv,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        setImmediate(persist_user_chan_permissions, plugin, collections,
                     ps, next);
      }
    });
  }
}

function has_simple_store_permissions(n, ks, rchans, auth, next) {
  var all    = true;
  var simple = true; // NOTE: simple is true, SUBSCRIBE or CACHE NOT NEEDED
  get_data_perms(n.plugin, n.collections,
                 ks, ZH.MyUUID, rchans, all, simple, auth,
  function(perr, perms) {
    if (perr) next(perr, null);
    else {
      if (!perms) next(null, false);
      else {
        if (perms === 'W') next(null, true);
        else               next(null, false);
      }
    }
  });
}

function send_central_get_user_chan_perms(n, ks, rchans, auth, next) {
  var username = auth.username;
  var sfunc    = ZAio.SendCentralGetUserChannelPermissions;
  sfunc(n.plugin, n.collections, rchans, auth, function(serr, dres) {
    if (serr) next(serr, null);
    else {
      var ps = dres.permissions;
      persist_user_chan_permissions(n.plugin, n.collections, ps,
      function(perr, pres) {
        if (perr) next(perr, null);
        else {
          has_simple_store_permissions(n, ks, rchans, auth, next);
        }
      });
    }
  });
}

exports.HasAgentStorePermissions = function(n, auth, ks, rchans, next) {
  if (!rchans) rchans = [];
  exports.AdminAuthentication(n, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (ok)   next(null, ok);
    else {
      authenticate(n.plugin, n.collections, auth, function(aerr, ok) {
        if      (aerr) next(aerr, null);
        else if (!ok)  next(new Error(ZS.Errors.AuthError), null);
        else { // NOTE simple is false, this is the AGENT check
          var all    = true;
          var simple = false;
          get_data_perms(n.plugin, n.collections,
                         ks, ZH.MyUUID, rchans, all, simple, auth,
          function(perr, perms) {
            if (perr) next(perr, null);
            else {
              if (perms) {
                if (perms === 'W') next(null, true);
                else               next(null, false);
              } else { // KEY NOT PRESENT -> ASK CENTRAL ABOUT PERMISSIONS
                send_central_get_user_chan_perms(n, ks, rchans, auth, next);
              }
            }
          });
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBE PERMISSIONS -----------------------------------------------------

exports.HasAgentSubscribePermissions = function(n, auth, schanid, next) {
  ZH.l('HasAgentSubscribePermissions: R: ' + schanid);
  has_subscribe_permissions(n, auth, schanid, true, next);
}

exports.HasCentralSubscribePermissions = function(n, auth, schanid, next) {
  ZH.l('HasCentralSubscribePermissions: R: ' + schanid);
  has_subscribe_permissions(n, auth, schanid, false, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CACHE PERMISSIONS ---------------------------------------------------------

exports.HasCentralCachePermissions = function(n, auth, ks, duuid, next) {
  ZH.l('HasCentralCachePermissions: K: ' + ks.kqk + ' UN: ' + auth.username);
  exports.AdminAuthentication(n, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (ok)   next(null, ok);
    else {
      authenticate(n.plugin, n.collections, auth, function(aerr, ok) {
        if      (aerr) next(aerr, null);
        else if (!ok)  next(new Error(ZS.Errors.AuthError), null);
        else {
          get_rchans(n, ks, function(gerr, rchans) {
            if (gerr) next(gerr, null);
            else {
              var all    = true;
              var simple = true;
              get_data_perms(n.plugin, n.collections,
                             ks, duuid, rchans, all, simple, auth,
              function(perr, perms) {
                if      (perr)  next(perr, null);
                else if (perms) next(null, true); // BOTH [R,W]
                else {
                  get_rchans_wildcard_perms(n.plugin, n.collections, rchans,
                  function(werr, operms) {
                    if (werr) next(werr, null);
                    else {
                      if (operms) next(null, true); // BOTH [R,W]
                      else        next(null, false);
                    }
                  });
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
// IS AUTO CACHE KEY ---------------------------------------------------------

function is_subscribed(net, auth, ks, rchans, next) {
  if (rchans.length === 0) next(null, false);
  else {
    var rchan = rchans.shift();
    var pkey  = ZS.GetUserChannelSubscriptions(auth.username);
    net.plugin.do_get_field(net.collections.usubs_coll, pkey, rchan,
    function(gerr, usubs) {
      if (gerr) next(gerr, null);
      else {
        if (usubs) next(null, true); // SUBSCRIBED
        else       setImmediate(is_subscribed, net, auth, ks, rchans, next);
      }
    });
  }
}

exports.IsAgentAutoCacheKey = function(net, auth, ks, rchans, next) {
  ZH.l('ZAuth.IsAgentAutoCacheKey: U: ' + auth.username + ' K: ' + ks.kqk);
  var crchans = ZH.clone(rchans);
  is_subscribed(net, auth, ks, crchans, function(serr, subscribed) {
    if (serr) next(serr, null);
    else {
      //ZH.l('subscribed: ' + subscribed);
      if (subscribed) next(null, false); // SUBSCRIBED -> NOT AUTO-CACHE
      else {
        ZMDC.GetKeyToDevices(net.plugin, net.collections, ks, ZH.MyUUID,
        function (kerr, kres) {
          if (kerr) next(kerr, null);
          else {
            //ZH.l('cached: ' + cached);
            var cached = kres ? true : false;
            var acache = !cached
            next(null, acache);
          }
        });
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL METHOD HANDLER ----------------------------------------------------

exports.HandleCentralAgentAuthenticate = function(n, auth, hres, next) {
  ZH.l('HandleCentralAgentAuthenticate: UN: ' + auth.username);
  exports.BasicAuthentication(n, auth, function(aerr, ok) {
    if (aerr) next(aerr, hres);
    else {
      hres.ok = ok; // Used in response
      var ukey = ZS.GetUserAuthentication(auth.username);
      n.plugin.do_get_field(n.collections.uauth_coll, ukey, 'role',
      function(perr, db_role) {
        if (perr) next(perr, null);
        else {
          hres.role = db_role; // Used in response
          next(null, hres);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// NEED MERGE PERMISSIONS ----------------------------------------------------

exports.HasSecurityTokenPermissions = function(n, ks, next) {
  ZH.l('HasSecurityTokenPermissions: K: ' + ks.kqk +
       ' TOK: ' + ks.security_token);
  var sectok = ZH.GenerateKeySecurityToken(ks);
  var ok = (ks.security_token === sectok) ? true : false;
  next(null, ok);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET ALL USER CHANNEL PERMISSIONS ------------------------------------------

// NOTE: used by ZChannel.CentralHandleGetUserSubscriptions() &
//               ZCache.get_user_to_key_permissions()
exports.GetAllUserChannelPermissions = function(plugin, collections, auth,
                                                hres, next) {
  var pkey = ZS.GetUserChannelPermissions(auth.username);
  plugin.do_get(collections.uperms_coll, pkey, function (gerr, gres) {
    if (gerr) next(gerr, hres);
    else {
      var res = {};
      if (gres.length !== 0) {
        hres.permissions = {};
        var perms = gres[0];
        delete(perms._id);
        for (var k in perms) {
          hres.permissions[k] = perms[k];
        }
      }
      next(null, hres);
    }
  });
}
 

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZDS.scan_read_perms_data() &
//               ZDS.find_read_perms_data()
exports.GetAgentDataReadPermissions = function(plugin, collections,
                                               ks, rchans, auth, next) {
  var all    = false;
  var simple = false;
  get_data_perms(plugin, collections,
                 ks, ZH.MyUUID, rchans, all, simple, auth, next);
}

// NOTE: Used by ZChannel.CentralHandleAgentSubscribe()
exports.GetCentralSubscribePermissions = function(plugin, collections,
                                                  auth, schanid, next) {
  get_subscribe_perms(plugin, collections, auth, schanid, false, next);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZAuth']={} : exports);

