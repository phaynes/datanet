
local Helper       = require "datanet.helpers";
local Network      = require "datanet.network";
local WorkerDaemon = require "datanet.daemon";
local Error        = require "datanet.error";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _A   = {};
_A.__index = _A;

-- AUTHORIZE-TYPE: [BASIC,READ,WRITE,STORE,SUBSCRIBE]
function _A.DoAuthorize(self, atype, kqk, rchans, schanid)
  local jbody    = Helper:CreateLocalAuthorizeJRB(atype, kqk, rchans, schanid);
  local res, err = Helper:RunLocalClientMethod(jbody);
  if (err ~= nil) then return nil, nil, err; end
  local miss  = res.miss;
  local ok    = res.ok;
  -- READ & WRITE: NO CENTRAL REQUEST
  local noc   = (kqk ~= nil) and (rchans == nil) and (schanid == nil);
  if     (noc)      then return res, miss, nil;
  elseif (not miss) then return res, miss, nil;
  else -- MISS: ASK CENTRAL
    local config = WorkerDaemon.config;
    local jreq   = cjson.decode(jbody);
    local id     = jreq.id;
    local abody  = Helper:CreateAgentAuthorizeRequest(jreq);
    local params = jreq.params;
    local auth   = params.authentication;
    Helper:SetAuthentication(auth.username, auth.password);
    local res, err = Network:SendCentralHttpsRequest(config, abody);
    if     (err   ~= nil)     then return nil, nil, err;
    elseif (atype ~= "STORE") then return res, nil, nil;
    else
      local ok   = Helper:HasAllWritePermissions(rchans, res.permissions);
      local ares = {ok = ok};
      return ares, nil, nil;
    end
  end
end

function _A.BasicAuthorize(self)
  LT('Authorization:BasicAuthorize');
  local err             = Helper:CheckAuthentication();
  if (err ~= nil) then return err; end
  local ares, miss, err = self:DoAuthorize("BASIC", nil, nil, nil);
  if (err ~= nil) then return err; end
  if (not ares.ok) then return Error.BasicAuthFail; end
  return nil;
end

function _A.KeyReadAuthorize(self, kqk)
  LT('Authorization:KeyReadAuthorize');
  local err             = Helper:CheckAuthentication();
  if (err ~= nil) then return err; end
  local ares, miss, err = self:DoAuthorize("READ", kqk, nil, nil);
  if (err ~= nil) then return err; end
  if (not ares.ok) then
    if (miss) then return Error.ReadNoDoc;
    else           return Error.ReadAuthFail;
    end
  end
  return nil;
end

function _A.KeyCommitAuthorize(self, kqk)
  LT('Authorization:KeyCommitAuthorize');
  local err             = Helper:CheckAuthentication();
  if (err ~= nil) then return err; end
  local ares, miss, err = self:DoAuthorize("WRITE", kqk, nil, nil);
  if (err ~= nil) then return err; end
  if (not ares.ok) then return Error.WriteAuthFail; end
  return nil;
end

function _A.KeyStoreAuthorize(self, kqk, rchans)
  LT('Authorization:KeyStoreAuthorize');
  local err             = Helper:CheckAuthentication();
  if (err ~= nil) then return err; end
  local ares, miss, err = self:DoAuthorize("STORE", kqk, rchans, nil);
  if (err ~= nil) then return err; end
  if (not ares.ok) then return Error.StoreAuthFail; end
  return nil;
end

function _A.SubscribeAuthorize(self, schanid)
  LT('Authorization:SubscribeAuthorize');
  local ares, miss, err = self:DoAuthorize("SUBSCRIBE", nil, nil, schanid);
  if (err ~= nil) then  return err; end
  if (not ares.ok) then return Error.SubscribeAuthFail; end
  return nil;
end

return _A;
