
local Helper       = require "datanet.helpers";
local Network      = require "datanet.network";
local RunMethod    = require "datanet.run_method";
local WorkerDaemon = require "datanet.daemon";
local ClientMethod = require "datanet.client_method";
local Debug        = require "datanet.debug";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _FE   = {};
_FE.__index = _FE;

-- NOTE: SENDS HTTP RESPONSE (ngx.say())
function _FE.AgentCall(self, jbody)
  local jreq, err = Network:SafeDecode(jbody);
  if (err ~= nil) then
    LE('Frontend:AgentCall: JSON PARSE ERROR: ' .. tostring(err));
    Helper:HttpRespondData(nil, nil, err, nil);
    return;
  end
  local method = jreq.method;
  local spot   = ClientMethod.Map[method];
  if (Debug.FullFrontend) then
    LD('Frontend:AgentCall: method: ' .. method ..
       ' spot: ' .. tostring(spot) .. ' BODY: ' .. jbody)
  else
    LT('Frontend:AgentCall: method: ' .. method);
  end
  if (spot == nil) then
    local err = "METHOD (" .. tostring(method) .. ") NOT SUPPORTED";
    LE('ERROR: FRONTEND: ' .. tostring(err));
    Helper:HttpRespondData(jreq.id, nil, err, nil);
    return
  end
  Helper:SetAuthentication(jreq.params.authentication.username,
                           jreq.params.authentication.password);
  local rres, rerr;
  if (spot == ClientMethod.Type.Worker or
      spot == ClientMethod.Type.Cache) then
    local config           = WorkerDaemon.config;
    local kqk, rchans, err = Helper:CreateKqkFromWorkerCall(config, jreq);
    if (err ~= nil) then
      LE('ERROR: FRONTEND: ' .. tostring(err));
      Helper:HttpRespondData(jreq.id, nil, err, nil);
    return
    end
    rres, rerr = RunMethod:RunWorker(kqk, jreq, jbody);
  else
    rres, rerr = RunMethod:RunLocal(jreq, jbody);
  end
  local sres = Network:FormatAgentResponse(jreq.id, rres, rerr);
  if (Debug.FullFrontend) then
    LD('LOCATION: FRONTEND.LUA: RESPONSE: ' .. sres);
  end
  ngx.say(sres);
end

return _FE;
