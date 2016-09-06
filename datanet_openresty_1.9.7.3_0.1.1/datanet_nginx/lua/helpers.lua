
local KeyPartition = require "datanet.key_partition";
local Network      = require "datanet.network";
local SHM          = require "datanet.shm";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _H   = {};
_H.__index = _H;

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- SIMPLE HELPER FUNCTIONS --------------------------------------------------

local MaxInt32 = 4294967296;

function _H.ReadFile(self, fname)
  local f = io.open(fname, "rb");
  if (f == nil) then return nil; end
  local contents = f:read("*all");
  f:close();
  return contents;
end

function _H.WriteFile(self, fname, txt)
  local f = io.open(fname, "wb");
  if (f == nil) then return false; end
  f:write(txt);
  f:close();
  return true;
end

local function __string_split(inputstr, sep)
  local t   = {};
  local i   = 1;
  for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
    t[i] = str
    i    = i + 1
  end
  return t
end

function _H.DotNotationStringSplit(self, field)
  return __string_split(field, ".");
end

function _H.Copy(self, orig)
  local copy;
  local otype = type(orig);
  if (otype == 'table') then
    copy = {};
    for okey, ovalue in next, orig, nil do
      copy[self:Copy(okey)] = self:Copy(ovalue);
    end
    setmetatable(copy, self:Copy(getmetatable(orig)));
  else -- number, string, boolean, etc
    copy = orig;
  end
  return copy;
end

function _H.LookupByDotNotation(self, json, field)
  local j    = json;
  local path = self:DotNotationStringSplit(field);
  local np   = table.getn(path);
  for i = 1, np do
    local p = path[i];
    j       = j[p];
    if (j == nil) then return nil; end
  end
  return j;
end

function _H.LookupParentByDotNotation(self, json, field)
  local j    = json;
  local par  = j;
  local path = self:DotNotationStringSplit(field);
  local np   = table.getn(path);
  for i = 1, np do
    if (j == nil) then return nil; end
    local p = path[i];
    par     = j;
    if (type(j) ~= "table") then return nil; end
    j       = j[p];
  end
  return par;
end

function _H.CountDictionary(self, cnodes)
  if (cnodes == nil) then return 0; end
  local nnodes  = 0;
  for uuid, cinfo in pairs(cnodes) do
    nnodes = nnodes + 1;
  end
  return nnodes;
end

function _H.EncodeCLog(self, prfx, t)
  local txt = cjson.encode(t);
  LD(prfx .. txt);
end

function _H.ConvertStringToJson(self, sval)
  local ok, ret = pcall(cjson.decode, sval);
  if (not ok) then return sval; -- DECODE FAILED -> USE STRING VALUE
  else             return ret;
  end
end

function _H.GetUniqueRequestID(self)
 return ngx.worker.pid() .. '_' .. math.random(MaxInt32);
end

function _H.CmpDependencies(self, depa, depb)
  for auuid, avrsn in pairs(depa) do
    local oavrsn = depb[auuid];
    if (avrsn ~= oavrsn) then return true; end
  end
  for auuid, avrsn in pairs(depb) do
    local oavrsn = depa[auuid];
    if (avrsn ~= oavrsn) then return true; end
  end
  return false; -- MATCH
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- BASE DOCUMENT HELPERS ----------------------------------------------------

function _H.NewDocument(self, bd, ns, cn, key, json, crdt, deps)
  local bdoc        = setmetatable({}, bd);
  bdoc.__index      = bdoc
  bdoc.namespace    = ns;
  bdoc.collection   = cn;
  bdoc.key          = key;
  bdoc.json         = json;
  bdoc.crdt         = crdt;
  bdoc.dependencies = deps;
  bdoc.oplog        = {};
  return bdoc;
end

function _H.CreateFromCrdt(self, bd, crdt, deps)
  local ns   = crdt._meta.ns;
  local cn   = crdt._meta.cn;
  local json = self:CreatePrettyJson(crdt);
  local key  = json._id;
  return self:NewDocument(bd, ns, cn, key, json, crdt, deps), nil;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- AUTHORIZATION HELPERS ----------------------------------------------------

local DefaultDocumentChannel = "0";

local function check_default_backdoor(schanid)
  return (schanid == DefaultDocumentChannel);
end

function _H.HasAllWritePermissions(self, rchans, perms)
  local uperms = {};
  for k, uperm in pairs(perms) do
    local uname = uperm.username;
    if (uname == ngx.ctx.username) then
      local chan = uperm.channel;
      uperms[chan.id] = chan.permissions;
    end
  end
  local n = table.getn(rchans);
  for k, schanid in pairs(rchans) do
    if ((n == 1) and check_default_backdoor(schanid)) then return true; end
    local perm = uperms[schanid];
    if (perm ~= "W") then return false; end
  end
  return true;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CRDT HELPER FUNCTIONS ----------------------------------------------------

_H.TableType = {Object = 1,
                Array  = 2
               };

local convert_crdt_element_to_tluat; -- NOTE LOCAL FUNCTION DEFINED BELOW
local convert_crdt_array_to_tluat;   -- NOTE LOCAL FUNCTION DEFINED BELOW

function _H.CreateTypedLuaTableArray(self)
  local cval = {}; -- EMPTY ARRAY
  return convert_crdt_array_to_tluat(cval);
end

local function convert_crdt_object_to_tluat(cval)
  local data = {__root = {dtype = _H.TableType.Object}};
  for k, v in pairs(cval) do
    data[k] = convert_crdt_element_to_tluat(v);
  end
  return data;
end

convert_crdt_array_to_tluat = function(cval) -- LOCAL FUNCTION
  local data = {__root = {dtype = _H.TableType.Array}};
  for i, v in ipairs(cval) do
    data[i] = convert_crdt_element_to_tluat(v);
  end
  return data;
end

convert_crdt_element_to_tluat = function(cval) -- NOTE LOCAL FUNCTION
  if (cval.X) then return nil; end -- TOMBSTONE
  local t = cval.T;
  if     (t == "A") then
    local cdata = cval.V;
    return convert_crdt_array_to_tluat(cdata);
  elseif (t == "O") then
    local cdata = cval.V;
    return convert_crdt_object_to_tluat(cdata);
  elseif (t == "N") then
    local val = cval.V.P - cval.V.N;
    return val;
  elseif (t == "S") then
    return cval.V;
  elseif (t == "X") then
    return "null";
  end
  ngx.log(ngx.EMERG, "convert_crdt_element_to_tluat");
end

function _H.ConvertCrdtToTypedLuaTable(self, crdt)
  local crdtdv = crdt._data.V;
  return convert_crdt_object_to_tluat(crdtdv);
end

local function convert_crdt_element_to_json(cdata)
  local ctxt = cjson.encode(cdata);
  local jtxt = c_convert_crdt_element_to_json(ctxt);
  return cjson.decode(jtxt);
end

function _H.CreatePrettyJson(self, crdt)
  if (not crdt) then return nil; end
  local meta   = crdt._meta;
  local id     = meta._id;
  local json   = convert_crdt_element_to_json(crdt._data);
  local chans  = meta.replication_channels;
  local pfname = meta.server_filter;
  local pjson  = {_id            = id,
                  _channels      = chans,
                  _server_filter = pfname};
  for k, v in pairs(json) do
    pjson[k] = v;
  end
  return pjson;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- JSON RPC REQUESTS --------------------------------------------------------

function _H.SetAuthentication(self, username, password)
  if (username ~= nil) then
    --LT('Helper:SetAuthentication: ' .. username);
    ngx.ctx          = {};
    ngx.ctx.username = username;
    ngx.ctx.password = password;
  end
end

function _H.CheckAuthentication(self)
  if (ngx.ctx.username == nil or ngx.ctx.password == nil) then
    return "ERROR: authentication is missing";
  else
    return nil;
  end
end

function _H.CreateJsonRpcResponseBody(self, id, rdata, err, edata)
  local rbody = {jsonrpc = "2.0",
                 id      = id};
  if (rdata ~= nil) then
    rbody.result        = rdata;
  else
    rbody.error         = err;
    rbody.error_details = edata;
  end
  return rbody;
end

function _H.HttpRespondData(self, id, rdata, err, data)
  local rbody = self:CreateJsonRpcResponseBody(id, rdata, err, data);
  local rtxt  = cjson.encode(rbody);
  --LT('Helper:HttpRespondData: RTXT: ' .. rtxt);
  ngx.say(rtxt);
end

local RpcCounter = 0;
function _H.GetNextRpcID(self)
  local pid  = ngx.worker.pid();
  RpcCounter = RpcCounter + 1;
  return "LAGENT_" .. pid .. "_L" .. RpcCounter;
end

local function create_json_rpc_body(self, method)
  local id = self:GetNextRpcID();
  local t  = {jsonrpc = "2.0",
              method  = method,
              id      = id,
              params  = {
                  data           = {},
                  authentication = {
                      username = ngx.ctx.username,
                      password = ngx.ctx.password
                  }
                }
              };
  return t, id;
end

local function create_central_json_rpc_body(self, method, needa)
  local uuid  = c_get_agent_uuid();
  local body  = create_json_rpc_body(self, method);
  local data  = body.params.data;
  data.device = {uuid = uuid, key = self.DeviceKey};
  if (needa) then data.agent  = {uuid = uuid}; end
  return body;
end

function _H.CreateKeyRequestJRB(self, method, ns, cn, key)
  local body, id  = create_json_rpc_body(self, method);
  local data      = body.params.data;
  data.namespace  = ns;
  data.collection = cn;
  data.key        = key;
  return body, id;
end

function _H.CreateKqk(self, ns, cn, key)
  return ns .. "|" .. cn .. "|" .. key;
end

function _H.CreateClientPingJRB(self)
  local body = create_json_rpc_body(self, "ClientPing");
  return body;
end

function _H.CreateGetClusterInfoJRB(self, ip, port)
  local body  = create_central_json_rpc_body(self, "GetClusterInfo", false);
  local data  = body.params.data;
  data.ip     = ip;
  data.port   = port;
  return cjson.encode(body);
end

function _H.CreateClusterPingJRB(self, ip, port, sport, uuid)
  local body       = create_json_rpc_body(self, "ClusterPing");
  local data       = body.params.data;
  data.device      = {uuid = uuid};
  data.ip          = ip;
  data.port        = port;
  data.server_port = sport;
  return cjson.encode(body);
end

function _H.CreateClusterStateChangeVoteJRB(self, term, vid, cnodes)
  local body         = create_json_rpc_body(self, "ClusterStateChangeVote");
  local data         = body.params.data;
  data.term_number   = term;
  data.vote_id       = vid;
  data.cluster_nodes = cnodes;
  return cjson.encode(body);
end

function _H.CreateClusterStateChangeCommitJRB(self, term, vid, cnodes, ptbl)
  local body           = create_json_rpc_body(self, "ClusterStateChangeCommit");
  local data           = body.params.data;
  data.term_number     = term;
  data.vote_id         = vid;
  data.cluster_nodes   = cnodes;
  data.partition_table = ptbl;
  return cjson.encode(body);
end

local function create_authorize_json_rpc_data(atype, kqk, rchans, schanid)
  local data = {};
  data.authorization_type   = atype;
  data.kqk                  = kqk;
  data.replication_channels = rchans;
  if (schanid ~= nil) then data.channel = {id = schanid}; end
  return data;
end

function _H.CreateLocalAuthorizeJRB(self, atype, kqk, rchans, schanid)
  local body       = create_json_rpc_body(self, "InternalClientAuthorize");
  body.params.data = create_authorize_json_rpc_data(atype, kqk, rchans,
                                                    schanid);
  return cjson.encode(body);
end

-- CLIENT CALLS
function _H.CreateGetInternalAgentSettingsJRB(self)
  local body = create_json_rpc_body(self, "GetInternalAgentSettings");
  return cjson.encode(body);
end

function _H.CreateClientFetchRequest(self, ns, cn, key)
  local body, id = self:CreateKeyRequestJRB("ClientFetch", ns, cn, key);
  return body;
end

function _H.CreateClientStoreRequest(self, ns, cn, key, sval)
  local body, id = self:CreateKeyRequestJRB("ClientStore", ns, cn, key);
  local data     = body.params.data;
  if (type(sval) == "table") then -- LUA TABLE API
    data.json  = sval;
  else                            -- JSON STRING API
    data.sjson = sval; -- AVOID: cjson.en/decode -> EMPTY-ARRAY-ISSUE
  end
  return body;
end

function _H.CreateClientRemoveRequest(self, ns, cn, key)
  local body, id = self:CreateKeyRequestJRB("ClientRemove", ns, cn, key);
  return body;
end

function _H.CreateClientCommitRequest(self, ns, cn, key, crdt, oplog)
  local body, id = self:CreateKeyRequestJRB("ClientCommit", ns, cn, key);
  local data     = body.params.data;
  data.crdt      = crdt;
  data.oplog     = oplog;
  return body;
end

function _H.CreateInternalCommitRequest(self, ns, cn, key, crdt, dentry)
  local body, id = self:CreateKeyRequestJRB("InternalCommit", ns, cn, key);
  local data     = body.params.data;
  data.crdt      = crdt;
  data.dentry    = dentry;
  data.pid       = ngx.worker.pid(); -- Used by CppHook.InternalDeltaBroadcast()
  return body;
end

function _H.CreateClientCacheRequest(self, ns, cn, key, pin, watch, sticky)
  local body, id = self:CreateKeyRequestJRB("ClientCache", ns, cn, key);
  local data     = body.params.data;
  data.pin       = pin;
  data.watch     = watch;
  data.sticky    = sticky;
  return body;
end

function _H.CreateClientEvictRequest(self, ns, cn, key)
  local body, id = self:CreateKeyRequestJRB("ClientEvict", ns, cn, key);
  return body;
end

function _H.CreateClientExpireRequest(self, ns, cn, key, expire)
  local body, id = self:CreateKeyRequestJRB("ClientExpire", ns, cn, key);
  local data     = body.params.data;
  data.expire    = expire;
  return body;
end

function _H.CreateClientIsolationRequest(self, isolate)
  local body = create_json_rpc_body(self, "ClientIsolation");
  local data = body.params.data;
  data.value = isolate;
  return body;
end

local function finalize_create_heartbeat_request(body, ns, cn, cmd, field, uuid,
                                           mlen, trim, isi, isa)
  local data        = body.params.data;
  data.command      = cmd;
  data.field        = field;
  data.uuid         = uuid;
  data.max_size     = mlen;
  data.trim         = trim;
  data.is_increment = isi;
  data.is_array     = isa;
  return body;
end

function _H.CreateClientHeartbeatRequest(self, ns, cn, cmd, field, uuid,
                                         mlen, trim, isi, isa)
  local body, id = self:CreateKeyRequestJRB("ClientHeartbeat", ns, cn, nil);
  return finalize_create_heartbeat_request(body, ns, cn, cmd, field, uuid,
                                           mlen, trim, isi, isa)
end

function _H.CreateInternalHeartbeatRequest(self, ns, cn, key, cmd, field, uuid,
                                           mlen, trim, isi, isa)
  local body, id = self:CreateKeyRequestJRB("InternalHeartbeat", ns, cn, key);
  return finalize_create_heartbeat_request(body, ns, cn, cmd, field, uuid,
                                           mlen, trim, isi, isa)
end

function _H.CreateClientGetStationedUsersRequest(self)
  local body = create_json_rpc_body(self, "ClientGetStationedUsers");
  return body;
end

function _H.CreateClientGetUserSubscriptionsRequest(self, username)
  local body    = create_json_rpc_body(self, "ClientGetUserSubscriptions");
  local data    = body.params.data;
  data.username = username
  return body;
end

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- AGENT CALLS ----------------------------------------------------------------

function _H.CreateAgentPingJRB(self)
  local body = create_central_json_rpc_body(self, "AgentPing", true);
  return cjson.encode(body);
end

function _H.CreateAdminGrantUserRequest(self, username, schanid, priv)
  local body    = create_central_json_rpc_body(self, "AdminGrantUser", false);
  local data    = body.params.data;
  data.username = username
  data.channel  = {id        = schanid,
                   privilege = priv};
  return body;
end

function _H.CreateAgentStationUserRequest(self, username, password)
  local body = create_central_json_rpc_body(self, "AgentStationUser", true);
  return body;
end

function _H.CreateAgentDestationUserRequest(self, username, password)
  local body = create_central_json_rpc_body(self, "AgentDestationUser", true);
  return body;
end

function _H.CreateAgentSubscribeRequest(self, schanid)
  local body   = create_central_json_rpc_body(self, "AgentSubscribe", true);
  local data   = body.params.data;
  data.channel = {id = schanid};
  return body;
end

function _H.CreateAgentUnsubscribeRequest(self, schanid)
  local body   = create_central_json_rpc_body(self, "AgentUnsubscribe", true);
  local data   = body.params.data;
  data.channel = {id = schanid};
  return body;
end

function _H.CreateAgentCacheJRB(self, jreq)
  local method = "AgentCache";
  local jdata  = jreq.params.data;
  local ns     = jdata.namespace;
  local cn     = jdata.collection;
  local key    = jdata.key;
  local body   = create_central_json_rpc_body(self, method, true);
  local data   = body.params.data;
  local kqk    = self:CreateKqk(ns, cn, key)
  data.ks      = {ns  = ns,
                  cn  = cn,
                  key = key,
                  kqk = kqk};
  data.pin     = jdata.pin;
  data.watch   = jdata.watch;
  data.sticky  = jdata.sticky;
  return cjson.encode(body);
end

local function create_agent_evict_jrb(self, method, ns, cn, key)
  local body = create_central_json_rpc_body(self, method, true);
  local data = body.params.data;
  local kqk  = self:CreateKqk(ns, cn, key);
  data.ks    = {ns  = ns,
                cn  = cn,
                key = key,
                kqk = kqk};
  return cjson.encode(body);
end

function _H.CreateAgentEvictJRB(self, ns, cn, key)
  local method = "AgentEvict";
  return create_agent_evict_jrb(self, method, ns, cn, key);
end

function _H.CreateAgentLocalEvictJRB(self, ns, cn, key)
  local method = "AgentLocalEvict";
  return create_agent_evict_jrb(self, method, ns, cn, key);
end

local function parse_client_authorize(params)
  local jinfo = {};
  jinfo.atype = params.data.authorization_type;
  if (params.data.kqk ~= nil) then
    jinfo.kqk = params.data.kqk;
  end
  if (params.data.channel    ~= nil and
      params.data.channel.id ~= nil) then
    jinfo.schanid = params.data.channel.id;
  end
  if (params.data.replication_channels ~= nil) then
    jinfo.rchans = params.data.replication_channels;
  end
  return jinfo;
end

function _H.CreateAgentAuthorizeRequest(self, jreq)
  local params = jreq.params;
  local jinfo  = parse_client_authorize(params);
  local atype  = jinfo.atype;
  local body   = nil;
  local method = nil;
  local cfunc  = create_central_json_rpc_body;
  if     (atype == "BASIC") then
    method = "AgentAuthenticate";
    body   = cfunc(self, method, false);
  elseif (atype == "STORE") then
    method = "AgentGetUserChannelPermissions";
    body   = cfunc(self, "AgentGetUserChannelPermissions", false);
    body.params.data.replication_channels = jinfo.rchans;
  elseif (atype == "SUBSCRIBE") then
    method = "AgentHasSubscribePermissions";
    body   = cfunc(self, "AgentHasSubscribePermissions", false);
    body.params.data.channel = {id = jinfo.schanid};
  else
    --TODO FATAL LOGIC ERROR
    return nil;
  end
  return cjson.encode(body);
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- AGENT CALL PARSER --------------------------------------------------------

function _H.CreateKqkFromWorkerCall(self, config, jreq)
  local method = jreq.method;
  local ns     = jreq.params.data.namespace;
  local cn     = jreq.params.data.collection
  local key    = jreq.params.data.key;   -- COMMIT,REMOVE
  local sjson  = jreq.params.data.sjson; -- STORE (LUA)
  local json   = jreq.params.data.json;  -- STORE (JS)
  local query  = jreq.params.data.query; -- FIND
  if (method == "ClientStore") then
    if (sjson == nil) then sjson = cjson.encode(json); end
    local key, rchans, nsjson, err =
                               internal_check_client_store(self, config, sjson);
    if (err) then return nil, nil, err; end
    return self:CreateKqk(ns, cn, key), rchans, nil;
  elseif (method == "ClientFind") then
    local jquery, err = Network:SafeDecode(query);
    if (err) then return nil, nil, err; end
    local key         = jquery._id;
    if (key == nil) then 
      local err = "ERROR: ClientFind -> NO KEY";
      return nil, nil, err;
    end
    return self:CreateKqk(ns, cn, key), nil, nil;
  else
    return self:CreateKqk(ns, cn, key), nil, nil;
  end
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- DATA CHECKS --------------------------------------------------------------

local function error_store_parse()
  return '-ERROR: Command: STORE failed to parse JSON data-structure';
end

local function error_store_no_key()
  return '-ERROR: Command: STORE: JSON requires "_id"';
end

local function error_store_no_rchans()
  return '-ERROR: Command: STORE: JSON requires "_channels"';
end

local function error_channels_format()
  return '-ERROR: "_channels" must be an array w/ a single string channel (e.g. ["1"])';
end

-- NOTE: return KEY, RCHANS, NSVAL, ERR
function internal_check_client_store(self, config, sval)
  local iss;
  local v;
  if (type(sval) == "table") then -- LUA TABLE API
    v   = sval;
    iss = false;
  else                            -- JSON STRING API
    local ok, ret = pcall(cjson.decode, sval);
    if (not ok) then return nil, nil, nil, ret; end -- ret is ERROR
    v   = ret;
    iss = true;
  end
  local err;
  if (v == nil or type(v) ~= "table") then err = error_store_parse();  end
  if (v._id == nil) then                   err = error_store_no_key(); end
  if (err ~= nil) then return nil, nil, nil, err; end
  local nsval;
  if (v._channels == nil) then -- NOTE: DEFAULT CHANNEL
    v._channels = {config.default.channel};
    if (iss) then -- ADD _channels to JSON STRING
      nsval = cjson.encode(v);
    end
  else
    local t = type(v._channels);
    if (t ~= "table") then err = error_channels_format();
    else
      local s = 0;
      for k, v in pairs(v._channels) do
        if (type(k) ~= "number") then err = error_channels_format(); end
        if (type(v) ~= "string") then err = error_channels_format(); end
        s = s + 1;
      end
      if (s ~= 1) then err = error_channels_format(); end
    end
    if (err ~= nil) then return nil, nil, nil, err; end
  end
  return v._id, v._channels, nsval, err;
end

local function error_store_no_json()
  return '-ERROR: Command: STORE missing field "json"';
end

-- NOTE: return KEY, NSVAL, ERR
function _H.CheckClientStore(self, config, sval)
  if (sval == nil) then
    local err = error_store_no_json();
    return nil, err;
  end
  local key, r, nsval, err = internal_check_client_store(self, config, sval);
  if (err ~= nil) then return nil, nil,   err;
  else                 return key, nsval, nil;
  end
end

function _H.DebugOpLog(self, oplog)
  for k, op in pairs(oplog) do
    local v = tostring(op.args[1]);
    if (op.name == "insert") then
      local spot = v;
      v          = tostring(op.args[2]);
      LT('OPLOG: OP: ' .. op.name .. ' PATH: ' .. op.path ..
            ' SPOT: ' .. spot .. ' V: ' .. v);
    elseif (op.name == "delete") then
      LT('OPLOG: OP: ' .. op.name .. ' PATH: ' .. op.path);
    else -- [SET, INCREMENT]
      LT('OPLOG: OP: ' .. op.name .. ' PATH: ' .. op.path .. ' V: ' .. v);
    end
  end
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- RUN PRIMARY/LOCAL CLIENT-METHOD ------------------------------------------

local function get_primary_unix_socket(self, config)
  local uwild     = config.sticky.unix_socket_wildcard;
  local sms       = SHM.GetDictionaryName("STARTUP");
  local pwid, err = sms:get("primary_worker_uuid");
  if (err) then         return nil; end
  if (pwid == nil) then return nil; end
  local ppid      = pwid;
  if (err) then return nil;
  else          return KeyPartition:CreateStickySocketName(config, ppid);
  end
end

function _H.RunPrimaryClientMethod(self, config, jbody)
  local sus = get_primary_unix_socket(self, config);
  return Network:LocalHttpRequest(sus, jbody);
end

function _H.RunLocalClientMethod(self, jbody)
  LT('Helper:RunLocalClientMethod');
  local sresp = c_local_client_method(jbody);
  local ret   = cjson.decode(sresp);
  local res   = ret.result;
  local err   = ret.error and ret.error.message or nil;
  return res, err;
end

function _H.GetBroadcastUnixSockets(self, config, xpid)
  local smp    = SHM.GetDictionaryName("WORKER_PARTITION_TABLE");
  local nwrkrs = ngx.worker.count();
  local suses  = {};
  for i = 0, (nwrkrs - 1) do
    local pid, err = smp:get(i);
    if (err) then return; end
    if (pid ~= nil) then -- NIL PID -> NOT YET INITIALIZAED (ON STARTUP)
      if (pid ~= xpid) then
        local sus  = KeyPartition:CreateStickySocketName(config, pid);
        suses[sus] = pid;
      end
    end
  end
  return suses;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- DEVICE KEY ---------------------------------------------------------------

_H.DeviceKey = nil;

function _H.SetDeviceKey(self, method, dkey)
  LD(method .. ': LUA-DEVICE-KEY: ' .. dkey);
  self.DeviceKey = dkey;
end

function _H.GetDeviceKey(self)
  return self.DeviceKey;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- AGENT CONNECTION ---------------------------------------------------------

function _H.ResetCentral(self, config, mhost, mport)
  LD('Helper:ResetCentral: HOST: ' .. mhost .. ' P: ' .. mport);
  config.central.https.hostname = mhost;
  config.central.https.port     = mport;
end

function _H.SendPrimaryGetInternalAgentSettingsJRB(self, config)
  LT('Helper:SendPrimaryGetInternalAgentSettingsJRB');
  local jbody    = self:CreateGetInternalAgentSettingsJRB();
  local res, err = self:RunPrimaryClientMethod(config, jbody);
  if (err) then
    ngx.log(ngx.ERR, "ERROR: SendPrimaryGetInternalAgentSettings CALL FAILED");
  end
end

local function broadcast_worker_settings(premature)
  if (premature) then return end
  c_broadcast_workers_settings(); -- PRIMARY BROADCAST SETTINGS TO ALL WORKERS
end

function _H.SendCentralAgentOnline(self, config, b)
  local nb       = b and 1 or 0; -- toint()
  local jbody    = c_get_agent_online_request_body(nb);
  local res, err = Network:SendCentralHttpsRequest(config, jbody);
  if (err ~= nil) then return nil, err;
  else
    if (res.device and res.device.key) then
      self:SetDeviceKey("AgentOnline", res.device.key);
      -- NOTIFY ALL WORKERS of DEVICE-KEY
      ngx.timer.at(0, broadcast_worker_settings);
    end
    return res, nil;
  end
end

function _H.SendCentralAgentPing(self, config)
  local jbody = self:CreateAgentPingJRB();
  return Network:SendCentralHttpsRequest(config, jbody);
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- DATA TYPE HELPERS --------------------------------------------------------

function _H.DataTypeCreateQueue(self, msize, trim)
  local d = {_data     = {},
             _type     = "LIST",
             _metadata = {}
            };
  d._metadata["MAX-SIZE"] = (msize * -1);
  d._metadata["TRIM"]     = trim;
  return d;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CONDITIONS ---------------------------------------------------------------

local ConditionSleep = 0.1; -- 1/10 of a second

function _H.BuildConditionName(self, op, path, value)
  if (value == nil) then
    return "CONDITION-" .. op .. "-" .. path;
  else
    return "CONDITION-" .. op .. "-" .. path .. "-" .. cjson.encode(value);
  end
end

function _H.ConditionInitialize(self, cname)
  LT("Helper:ConditionInitialize: " .. cname);
  local s, err, f = ngx.shared.DATANET_TEST_CONDITIONS:set(cname, true);
  return err;
end

function _H.ConditionWait(self, cname)
  LT("Helper:ConditionWait: " .. cname);
  while (true) do
    local val, flags = ngx.shared.DATANET_TEST_CONDITIONS:get(cname);
    if (val == nil) then return; end
    ngx.sleep(ConditionSleep);
  end
end

function _H.ConditionSignal(self, cname)
  LT("Helper:ConditionSignal: " .. cname);
  ngx.shared.DATANET_TEST_CONDITIONS:delete(cname);
end

return _H;
