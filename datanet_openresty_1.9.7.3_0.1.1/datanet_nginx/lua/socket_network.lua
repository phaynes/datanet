
local Network   = require "network";

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- HELPERS -------------------------------------------------------------------

local function num_to_le_uint(n)
  local b  = {};
  for i = 1, 4 do
    b[i] = n % 256;
    n    = math.floor(n / 256);
  end
  return string.char(unpack(b));
end

local function le_uint_to_num(s)
  local b = {string.byte(s, 1, 4)};
  local n = 0;
  for i = #b, 1, -1 do
    n = (n * 256) + b[i];
  end
  return n;
end

local function parse_response(rbody)
  local v = cjson.decode(rbody);
  if (v == nil) then      -- PARSE ERROR
    return nil, "PARSE ERROR";
  elseif (v.error) then   -- MESSAGE ERROR
    return nil, v.error.message;
  else                    -- MESSAGE OK
    return v.result, nil;
  end
end

local function network_encode(jbody)
  local jlen = string.len(jbody)
  return num_to_le_uint(jlen) .. jbody;
end

local function socket_connect(sock, ip, port)
  local ok, err = sock:connect(ip, port);
  if (not ok) then
    ngx.say("failed to connect to IP: " .. ip .. " PORT: " .. port, err);
    return false;
  else
    return true;
  end
end

local function network_socket_send(sock, jbody)
  local nbody      = network_encode(jbody);
  local bytes, err = sock:send(nbody);
  if (err ~= nil) then
    ngx.say('ERROR: SOCK:SEND(): ' .. err);
    return false;
  else
    return true;
  end
end

local function network_socket_read(sock)
  local line, err, p = sock:receive(4);
  if (not line) then
    ngx.say("ERROR: sock:receive(): FAILED: ", err);
    return false, nil;
  else
    local mlen   = le_uint_to_num(line);
    line, err, p = sock:receive(mlen);
    if (not line) then
      ngx.say("ERROR: sock:receive(#): FAILED: ", err);
      return false, nil;
    else
      return true, line;
    end
  end
end

local function connect_to_primary_port(config)
  local pport  = config.primary.port;
  local sock   = ngx.socket.tcp();
  return socket_connect(sock, "127.0.0.1", pport), sock;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _SN   = {};
_SN.__index = _SN;

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- NETWORKING ---------------------------------------------------------------

function _SN.ConnectToPrimaryWorkerPort(self)
  local wport = c_get_primary_worker_port();
  local sock  = ngx.socket.tcp();
  return socket_connect(sock, "127.0.0.1", wport), sock;
end

function _SN.ConnectToNormalWorkerPort(self, kqk)
  local wport = c_get_key_worker_port(kqk);
  local sock  = ngx.socket.tcp();
  return socket_connect(sock, "127.0.0.1", wport), sock;
end

function _SN.NetworkRequest(self, sock, jbody)
  local ok        = network_socket_send(sock, jbody);
  if (not ok) then return nil, "NETWORK CONNECTION"; end
  local ok, rbody = network_socket_read(sock);
  if (not ok) then return nil, "NETWORK CONNECTION"; end
  return parse_response(rbody);
end

function _SN.RunProxyToCentralMethod(self, config, id, method, qbody)
  LT('BEG: Network:RunProxyToCentralMethod');
  local ok, sock = connect_to_primary_port(config);
  if (not ok) then
    return Network:FormatAgentErrorResponse(id, "NETWORK CONNECTION");
  end
  local ares, err = self:NetworkRequest(sock, qbody);
  if (err ~= nil) then return Network:FormatAgentErrorResponse(id, err); end
  local rbody     = cjson.encode(ares);
  local err       = c_handle_central_response(method, qbody, rbody);
  LT('DONE: Network:RunProxyToCentralMethod');
  return Network:FormatAgentResponse(id, ares, err);
end

return _SN;
