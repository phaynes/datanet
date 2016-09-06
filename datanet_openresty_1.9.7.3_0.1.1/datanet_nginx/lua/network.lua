
local http  = require "datanet.http";
local Debug = require "datanet.debug";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _N   = {};
_N.__index = _N;

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- SAFE PARSE JSON RPC ------------------------------------------------------

function _N.SafeDecode(self, sresp)
  local ok, ret = pcall(cjson.decode, sresp);
  if (not ok) then return nil, ret; -- ret is ERROR
  else             return ret, nil;
  end
end

function safe_decode_json_rpc_body(sresp)
  local ok, ret = pcall(cjson.decode, sresp);
  if (not ok) then return nil,        ret; -- ret is ERROR
  else
    local res = ret.result;
    local err = ret.error and ret.error.message or nil;
    return res, err;
  end
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- FORMAT AGENT RESPONSE ----------------------------------------------------

function format_agent_error_response(id, err)
  local etxt  = (type(err) == "string") and err or err.message;
  local resp  = {jsonrpc = "2.0",
                 id      = id,
                 error   = {
                   code    = -32007,
                   message = etxt
                 }
                };
  local sresp = cjson.encode(resp);
  return sresp, nil;
end

function format_agent_ok_resonse(id, res)
  local resp  = {jsonrpc = "2.0",
                 id      = id,
                 result  = res
                };
  local sresp = cjson.encode(resp);
  return sresp, nil;
end

function _N.FormatAgentResponse(self, id, res, err)
  if (err ~= nil) then return format_agent_error_response(id, err);
  else                 return format_agent_ok_resonse    (id, res);
  end
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- HTTP NETWORKING ----------------------------------------------------------

local function set_keep_alive(httpc, desc)
  local ok, err = httpc:set_keepalive(0);
  if (not ok) then
    ngx.log(ngx.ERR, desc .. " set_keepalive() ERROR: " .. err);
  end
end

function _N.LocalHttpRequest(self, sus, jbody)
  if (Debug.FullNetwork) then
    LD('Network:LocalHttpRequest: URI: ' .. sus .. ' BODY: ' .. jbody);
  end
  local options = {
    path    = "/agent";
    method  = "POST",
    body    = jbody
  };
  local httpc   = http.new()
  local ok, err = httpc:connect(sus);
  if (err ~= nil) then
    return nil, err;
  else
    local ret, err = httpc:request(options);
    if (err ~= nil) then return nil, err; end
    if (not ret) then
      local err = "REQUEST FAILED";
      return nil, err;
    end
    local sresp = ret:read_body();
    if (Debug.FullNetwork) then
      LD('Network:LocalHttpRequest: RESPONSE: ' .. sresp);
    end
    set_keep_alive(httpc, "UnixSocket");
    return safe_decode_json_rpc_body(sresp);
  end
end

local function send_https_request(self, url, body)
  local options = {
    method     = "POST",
    body       = body,
    ssl_verify = false
  };
  if (Debug.FullNetwork) then
    LD('send_https_request: URL: ' .. url .. ' BODY: ' .. body);
  end
  local httpc     = http.new()
  local hres, err = httpc:request_uri(url, options);
  if (err) then return nil,       err;
  else          return hres.body, nil;
  end
end

function _N.SendUrlHttpsRequest(self, url, qbody)
  return send_https_request(self, url, qbody);
end

function get_central_url(config)
  local host = config.central.https.hostname;
  local port = config.central.https.port;
  return "https://" .. host .. ":" .. port .. "/agent";
end

function _N.SendCentralHttpsRequest(self, config, qbody)
  local url        = get_central_url(config);
  local rbody, err = send_https_request(self, url, qbody);
  if (err ~= nil) then
    LE('Network:SendCentralHttpsRequest: ERROR: ' .. err);
    c_broken_central_connection();
    return nil, err;
  end
  if (Debug.FullNetwork) then
    LD('Network:SendCentralHttpsRequest: RESPONSE: ' .. rbody);
  end
  local err = c_handle_central_response(qbody, rbody);
  if (err ~= nil) then return nil, err; end
  return safe_decode_json_rpc_body(rbody);
end

return _N;
