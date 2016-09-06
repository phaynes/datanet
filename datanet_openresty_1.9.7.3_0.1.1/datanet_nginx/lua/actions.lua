
local Helper        = require "datanet.helpers";
local Authorization = require "datanet.authorization";
local BaseDocument  = require "datanet.base_document";
local RunMethod     = require "datanet.run_method";
local WorkerDaemon  = require "datanet.daemon";
local Error         = require "datanet.error";
local Debug         = require "datanet.debug";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _DA   = {};
_DA.__index = _DA;

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLIENT CALLS -------------------------------------------------------------

function run_worker_return_ok(kqk, jreq)
  local res, err = RunMethod:RunWorker(kqk, jreq, nil);
  if (err ~= nil) then return nil, err; end
  return true, nil;
end

function handle_client_document_retrieve(ns, cn, res, err)
  if (err ~= nil) then
    if (err == Error.ReadNoDoc) then return nil, nil;
    else                             return nil, err;
    end
  end
  if (res == nil) then return nil, nil
  else                 return BaseDocument:Create(ns, cn, res);
  end
end
function _DA.ClientFetch(self, ns, cn, key, cache)
  if (cache == true) then
    return self:ClientCache(ns, cn, key, false, false, false);
  else
    local kqk      = Helper:CreateKqk(ns, cn, key);
    LT('Actions:ClientFetch: K: ' .. kqk);
    local jreq     = Helper:CreateClientFetchRequest(ns, cn, key);
    local res, err = RunMethod:RunWorker(kqk, jreq, nil);
    return handle_client_document_retrieve(ns, cn, res, err);
  end
end

function _DA.ClientStore(self, ns, cn, sval)
  LT('Actions:ClientStore');
  local config          = WorkerDaemon.config;
  local key, nsval, err = Helper:CheckClientStore(config, sval);
  if (err   ~= nil) then return nil, err; end
  if (nsval ~= nil) then sval = nsval; end -- ADDED _channels:[]
  local kqk      = Helper:CreateKqk(ns, cn, key);
  local jreq     = Helper:CreateClientStoreRequest(ns, cn, key, sval);
  local res, err = RunMethod:RunWorker(kqk, jreq, nil);
  if (err ~= nil) then return nil, err; end
  return BaseDocument:Create(ns, cn, res);
end

-- NOTE: CLIENT-COMMIT in document.lua

function _DA.ClientRemove(self, ns, cn, key)
  local kqk  = Helper:CreateKqk(ns, cn, key);
  LT('Actions:ClientRemove: K: ' .. kqk);
  local jreq = Helper:CreateClientRemoveRequest(ns, cn, key);
  return run_worker_return_ok(kqk, jreq);
end

function _DA.ClientCache(self, ns, cn, key, pin, watch, sticky)
  local kqk      = Helper:CreateKqk(ns, cn, key);
  LT('Actions:ClientCache: K: ' .. kqk);
  local jreq     = Helper:CreateClientCacheRequest(ns, cn, key,
                                                   pin, watch, sticky);
  local res, err = RunMethod:RunWorker(kqk, jreq, nil);
  return handle_client_document_retrieve(ns, cn, res, err);
end

function _DA.ClientEvict(self, ns, cn, key)
  local kqk  = Helper:CreateKqk(ns, cn, key);
  LT('Actions:ClientEvict: K: ' .. kqk);
  local jreq = Helper:CreateClientEvictRequest(ns, cn, key);
  return run_worker_return_ok(kqk, jreq);
end

-- TODO ClientLocalEvict

function _DA.ClientExpire(self, ns, cn, key, expire)
  local kqk  = Helper:CreateKqk(ns, cn, key);
  LT('Actions:ClientExpire: K: ' .. kqk .. ' E: ' .. expire);
  local jreq = Helper:CreateClientExpireRequest(ns, cn, key, expire);
  return run_worker_return_ok(kqk, jreq);
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- RUN-LOCAL -----------------------------------------------------------------

function run_local_return_ok(jreq)
  local res, err = RunMethod:RunLocal(jreq, nil);
  if (err) then return nil, err; end
  return true, nil;
end

function _DA.ClientSwitchUser(self, username, password)
  LT('Actions:ClientSwitchUser');
  Helper:SetAuthentication(username, password);
  local ares, miss, err = Authorization:DoAuthorize("BASIC", nil, nil, nil);
  if (err) then return nil, err; end
  return true, nil;
end

function _DA.ClientIsUserStationed(self, username)
  local jreq     = Helper:CreateClientGetStationedUsersRequest();
  local res, err = RunMethod:RunLocal(jreq, nil);
  if (err) then return nil, err; end
  local users    = res.users;
  if (users == nil) then return false, nil; end
  for k, user in pairs(users) do
    if (user == username) then return true, nil; end
  end
  return false, nil;
end

function _DA.ClientStationUser(self)
  LT('Actions:ClientStationUser');
  local jreq = Helper:CreateAgentStationUserRequest();
  return run_local_return_ok(jreq);
end

function _DA.ClientDestationUser(self)
  LT('Actions:ClientDestationUser');
  local jreq = Helper:CreateAgentDestationUserRequest();
  return run_local_return_ok(jreq);
end

function _DA.ClientIsSubscribed(self, schanid)
  LT('Actions:ClientIsSubscribed');
  local username = Helper.username;
  local jreq     = Helper:CreateClientGetUserSubscriptionsRequest(username);
  local res, err = RunMethod:RunLocal(jreq, nil);
  if (err) then return nil, err; end
  local subs     = res.subscriptions;
  if (subs == nil) then return false, nil; end
  for k, chan in pairs(subs) do
    if (chan == schanid) then return true, nil; end
  end
  return false, nil;
end

function _DA.ClientSubscribe(self, schanid)
  LT('Actions:ClientSubscribe');
  local jreq = Helper:CreateAgentSubscribeRequest(schanid);
  return run_local_return_ok(jreq);
end

function _DA.ClientUnsubscribe(self, schanid)
  LT('Actions:ClientUnsubscribe');
  local jreq = Helper:CreateAgentUnsubscribeRequest(schanid);
  return run_local_return_ok(jreq);
end

function _DA.ClientIsolation(self, isolate)
  LT('Actions:ClientIsolation');
  local jreq = Helper:CreateClientIsolationRequest(isolate);
  return run_local_return_ok(jreq);
end

function _DA.ClientHeartbeat(self, ns, cn, cmd, field, uuid,
                             mlen, trim, isi, isa)
  LT('Actions:ClientHeartbeat');
  local jreq = Helper:CreateClientHeartbeatRequest(ns, cn, cmd, field,
                                                   uuid, mlen, trim, isi, isa);
  return run_local_return_ok(jreq);
end

if (Debug.AllowAdminMethods) then
  function _DA.AdminGrantUser(self, username, schanid, priv)
    local jreq = Helper:CreateAdminGrantUserRequest(username, schanid, priv);
    return run_local_return_ok(jreq);
  end
end

return _DA;
