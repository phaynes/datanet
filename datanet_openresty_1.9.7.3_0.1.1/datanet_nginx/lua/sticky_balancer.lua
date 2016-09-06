local balancer     = require "ngx.balancer"

local Helper       = require "datanet.helpers";
local KeyPartition = require "datanet.key_partition";
local WorkerDaemon = require "datanet.daemon";
local Cluster      = require "datanet.cluster";

local skv = KeyPartition:GetShardKeyValue();
if (skv == nil) then
  ngx.log(ngx.ERR, "missing required url-param: 'shard-key'");
  return ngx.exit(500)
end

--TODO ON SOCKET FAILURE  -> TRY AGAIN
--     ON CLUSTER FAILURE -> DO LOCALLY

local config = WorkerDaemon.config;

local sip    = ngx.var.server_addr;
local sport  = tonumber(ngx.var.server_port);

local cinfo  = Cluster:GetStickyServer(skv);
if (cinfo == nil) then
  ngx.log(ngx.ERR, "failed to get sticky-server");
  return ngx.exit(500)
end
local cip   = cinfo.ip;
local cport = tonumber(cinfo.server_port);

if (cip ~= sip or cport ~= sport) then
  ngx.log(ngx.DEBUG, "LOAD_BALANCE: SK: " .. skv .. 
                    " CLUSTER: " .. cip .. ":" .. cport);

  local ok, err = balancer.set_current_peer(cip, cport);
  if not ok then
    ngx.log(ngx.ERR, "failed to set the current peer: ", err)
    return ngx.exit(500)
  end
else
  local sus, ism, err = KeyPartition:GetStickyUnixSocketShardKey(config, skv);
  if (err ~= nil) then
    ngx.log(ngx.ERR, "failed to get sticky-worker: ERROR: " .. err);
    return ngx.exit(500)
  end

  ngx.log(ngx.DEBUG, "LOAD_BALANCE: SK: " .. skv .. " LOCAL_PATH: " .. sus);

  local ok, err = balancer.set_current_peer(sus)
  if not ok then
    ngx.log(ngx.ERR, "failed to set the current peer: ", err)
    return ngx.exit(500)
  end
end

