
local Helper        = require "datanet.helpers";
local Network       = require "datanet.network";
local RunMethod     = require "datanet.run_method";
local WorkerDaemon  = require "datanet.daemon";
local Debug         = require "datanet.debug";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _S   = {};
_S.__index = _S;

local function run_subscriber_local_method(jreq, jbody)
  LT('run_subscriber_local_method');
  return Helper:RunLocalClientMethod(jbody);
end

local function run_subscriber_worker_method(jreq, jbody)
  local kqk = jreq.params.data.ks.kqk;
  LT('run_subscriber_worker_method: K: ' .. kqk);
  return RunMethod:RunWorker(kqk, jreq, jbody);
end

local SubscriberMethod = {Local  = 1,
                          Worker = 2,
                         };

local SubscriberMethodRunFunctions = {run_subscriber_local_method,
                                      run_subscriber_worker_method,
                                     };
local SubscriberMethodMap = {
                   SubscriberDelta          = SubscriberMethod.Worker,
                   SubscriberDentries       = SubscriberMethod.Worker,
                   SubscriberCommitDelta    = SubscriberMethod.Worker,
                   SubscriberMerge          = SubscriberMethod.Worker,
                   AgentDeltaError          = SubscriberMethod.Worker,

                   SubscriberReconnect      = SubscriberMethod.Local,
                   SubscriberGeoStateChange = SubscriberMethod.Local,
                   -- SubscriberMessage & SubscriberAnnounceNewMemcacheCluster
                   PropogateSubscribe       = SubscriberMethod.Local,
                   PropogateUnsubscribe     = SubscriberMethod.Local,
                   PropogateRemoveUser      = SubscriberMethod.Local,
                   PropogateGrantUser       = SubscriberMethod.Local,
                   AgentBackoff             = SubscriberMethod.Local,
                  };

-- NOTE: SENDS HTTP RESPONSE (ngx.say())
function _S.SubscriberCall(self, jbody)
  local jreq, err = Network:SafeDecode(jbody);
  if (err ~= nil) then
    LE('Subscriber:SubscriberCall: JSON PARSE ERROR: ' .. tostring(err));
    return;
  end
  local config = WorkerDaemon.config;
  local cbkey  = jreq.params.data.callback_key;
  if (cbkey ~= config.central.callback.key) then
    ngx.log(ngx.ERR, 'DENY-REQUEST: INVALID: CALLBACK-KEY: ' .. cbkey);
    return;
  end
  local method = jreq.method;
  local spot   = SubscriberMethodMap[method];
  LD('Subscriber:SubscriberCall: method: ' .. method ..
     ' spot: ' .. tostring(spot));
  if (spot == nil) then
    local err = "METHOD (" .. tostring(method) .. ") NOT SUPPORTED";
    LE('ERROR: SUBSCRIBER: ' .. tostring(err));
    Helper:HttpRespondData(jreq.id, nil, err, nil);
    return
  end
  local f        = SubscriberMethodRunFunctions[spot];
  local res, err = f(jreq, jbody);
  local sres     = Network:FormatAgentResponse(jreq.id, res, err);
  if (Debug.FullSubscriber) then
    LD('LOCATION: SUBSCRIBER.LUA: RESPONSE: ' .. sres);
  end
  ngx.say(sres);
end

return _S;
