
local KeyPartition = require "datanet.key_partition";
local Agent        = require "datanet.agent";
local WorkerDaemon = require "datanet.daemon";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _RA   = {};
_RA.__index = _RA;

function _RA.RunWorker(self, kqk, jreq, jbody)
  LT('RunMethod:RunWorker');
  local config        = WorkerDaemon.config;
  local sus, ism, err = KeyPartition:GetStickyUnixSocketKqk(config, kqk);
  if (err ~= nil) then return nil, err; end
  if (ism) then return Agent:InternalAgentCall(jreq, jbody);
  else          return Agent:AgentHttpRequest(sus, jreq, jbody);
  end
end

function _RA.RunLocal(self, jreq, jbody)
  LT('RunMethod:RunLocal');
  return Agent:InternalAgentCall(jreq, jbody);
end

return _RA;
