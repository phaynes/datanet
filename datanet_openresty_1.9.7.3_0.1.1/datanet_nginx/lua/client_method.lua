
local Debug = require "datanet.debug";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _CMM  = {};
_CMM.__index = _CMM;

_CMM.Type = {Internal = 1,
             Local    = 2,
             Primary  = 3,
             Worker   = 4,
             Cache    = 5,
             Evict    = 6,
            };

_CMM.Map = {GetInternalAgentSettings    = _CMM.Type.Internal,
            ClientSwitchUser            = _CMM.Type.Internal,
            ClientPing                  = _CMM.Type.Internal,

            AgentStationUser            = _CMM.Type.Primary,
            AgentDestationUser          = _CMM.Type.Primary,
            AgentSubscribe              = _CMM.Type.Primary,
            AgentUnsubscribe            = _CMM.Type.Primary,

            ClientHeartbeat             = _CMM.Type.Internal,
            ClientIsolation             = _CMM.Type.Local,

            SubscriberDelta             = _CMM.Type.Internal,
            SubscriberDentries          = _CMM.Type.Internal,
            SubscriberCommitDelta       = _CMM.Type.Internal,
            SubscriberMerge             = _CMM.Type.Internal,
            AgentDeltaError             = _CMM.Type.Internal,
            SubscriberReconnect         = _CMM.Type.Internal,

            InternalRunCacheReaper      = _CMM.Type.Internal,
            InternalAgentSettings       = _CMM.Type.Internal,
            InternalRunToSyncKeysDaemon = _CMM.Type.Internal,
            DocumentCacheDelta          = _CMM.Type.Internal,
            InternalCommit              = _CMM.Type.Internal,

            ClientFetch                 = _CMM.Type.Worker,
            ClientFind                  = _CMM.Type.Worker,
            ClientStore                 = _CMM.Type.Worker,
            ClientCommit                = _CMM.Type.Worker,
            ClientRemove                = _CMM.Type.Worker,
            ClientExpire                = _CMM.Type.Worker,
            InternalHeartbeat           = _CMM.Type.Worker,

            ClientCache                 = _CMM.Type.Cache,

            ClientEvict                 = _CMM.Type.Evict,
            ClientLocalEvict            = _CMM.Type.Evict,
         };

-- NOTE: FRONTEND METHODS DO NOT HAVE A LUA API
--       only callabe via Datanet:frontend_call()
--       which is exposed in locations/agent.lua
local FEM = {ClientStationUser           = _CMM.Type.Internal,
             ClientDestationUser         = _CMM.Type.Internal,
             ClientSubscribe             = _CMM.Type.Internal,
             ClientUnsubscribe           = _CMM.Type.Internal,

             ClientNotify                = _CMM.Type.Local,

             ClientGetClusterInfo        = _CMM.Type.Local,
             ClientGetStationedUsers     = _CMM.Type.Local,
             ClientGetAgentSubscriptions = _CMM.Type.Local,
             ClientGetUserSubscriptions  = _CMM.Type.Local,
            };

if (Debug.AllowFrontendMethods) then
  for k, v in pairs(FEM) do
    _CMM.Map[k] = v;
  end
end

local AEM = {AdminAddUser                = _CMM.Type.Primary,
             AdminGrantUser              = _CMM.Type.Primary,
             AdminRemoveUser             = _CMM.Type.Primary,
            };

if (Debug.AllowAdminMethods) then
  for k, v in pairs(AEM) do
    _CMM.Map[k] = v;
  end
end

return _CMM;
