
local Helper        = require "datanet.helpers";
local DocumentCache = require "datanet.document_cache";
local RunMethod     = require "datanet.run_method";
local DeltaCache    = require "datanet.delta_cache";
local Error         = require "datanet.error";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CHAOS --------------------------------------------------------------------

local ChaosDelaySendInternalClientCommit = false;

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- HELPERS ------------------------------------------------------------------

local function error_no_document(cmd)
  return '-ERROR: Command: ' .. cmd ..  ' requires a document to be FETCHed';
end

local function error_no_collection(cmd)
  return '-ERROR: Command: ' .. cmd .. ' requires a COLLECTION';
end

local function error_field_not_found(field)
  return '-ERROR: Field: (' .. field .. ') not found';
end

local function error_field_not_number(field)
  return '-ERROR: Field: (' .. field .. ') must be type: NUMBER';
end

local function error_insert_on_non_table(field)
  return '-ERROR: INSERT: Field: (' .. field .. ') must be type: ARRAY';
end

local function error_non_numeric_arg(cmd, argc)
  return '-ERROR: Command: ' .. cmd .. ' argument #' .. argc ..  ' NOT numeric';
end

local function build_single_element_array(pos, sval)
  local arr = "[";
  for i = 1, pos do
    arr = arr .. "null,";
  end
  arr = arr .. sval .. "]"; -- SET single element ARRAY
  return arr;
end

local function convert_string_to_oplog_value(sval)
  local n = tonumber(sval);
  if (n ~= nil) then return n;
  else               return sval;
  end
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- DOCUMENT CLASS CONSTRUCTOR & PRIVATE -------------------------------------

local _BD   = {};
_BD.__index = _BD;

local function debug(self)
  LD('NS: ' .. self.namespace ..  ' CN: ' .. self.collection ..
     ' K: ' .. self.key);
  return true;
end

local function nested_set(self, field, v)
  local p    = Helper:LookupParentByDotNotation(self.json, field);
  local path = Helper:DotNotationStringSplit(field);
  local np   = table.getn(path);
  local fp   = path[np];
  LT('nested_set: fp: ' .. fp);
  p[fp]      = v; -- UPDATE JSON MIRROR
  return true;
end

local function add_to_oplog(self, opname, field, args)
  local oentry = {name = opname,
                  path = field,
                  args = Helper:Copy(args)};
  table.insert(self.oplog, oentry);
  return true;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- PUBLIC METHODS -----------------------------------------------------------

function _BD.set(self, field, sval)
  if (field == nil) then return "ERROR: SET(field, value): NO FIELD"; end
  if (sval  == nil) then return "ERROR: SET(field, value): NO VALUE"; end
  local p = Helper:LookupParentByDotNotation(self.json, field);
  if (p == nil) then
    return error_field_not_found(field);
  end
  local v  = Helper:ConvertStringToJson(sval);
  local ov = convert_string_to_oplog_value(sval);
  nested_set(self, field, v);
  add_to_oplog(self, "set", field, {ov});
  return nil;
end

function _BD.delete(self, field)
  if (field == nil) then return "ERROR: DELETE(field): NO FIELD"; end
  local f = Helper:LookupByDotNotation(self.json, field);
  if (f == nil) then
    return error_field_not_found(field);
  end
  nested_set(self, field, nil);
  add_to_oplog(self, "delete", field);
  return nil;
end

function _BD.incr(self, field, bval)
  if (field == nil) then return "ERROR: INCR(field, byvalue): NO FIELD";   end
  if (bval  == nil) then return "ERROR: INCR(field, byvalue): NO BYVALUE"; end
  local f = Helper:LookupByDotNotation(self.json, field);
  if (f == nil) then
    return error_field_not_found(field);
  end
  if (type(f) ~= "number") then
    return error_field_not_number(field);
  end
  f = f + bval;
  nested_set(self, field, f);
  add_to_oplog(self, "increment", field, {bval});
  return nil;
end

function _BD.decr(self, field, bval)
  if (field == nil) then return "ERROR: DECR(field, byvalue): NO FIELD";   end
  if (bval  == nil) then return "ERROR: DECR(field, byvalue): NO BYVALUE"; end
  local f = Helper:LookupByDotNotation(self.json, field);
  if (f == nil) then
    return error_field_not_found(field);
  elseif (type(f) ~= "number") then
    return error_field_not_number(field);
  end
  f = f - bval;
  nested_set(self, field, f);
  add_to_oplog(self, "decrement", field, {bval});
  return nil;
end

function _BD.insert(self, field, pos, sval)
  if (field == nil) then
    return "ERROR: INSERT(field, position, value): NO FIELD";
  end
  if (pos == nil) then
    return "ERROR: INSERT(field, position, value): NO POSITION";
  end
  if (sval == nil) then
    return "ERROR: INSERT(field, position, value): NO VALUE";
  end
  if (type(pos) ~= "number") then
    return "ERROR: INSERT(field, position, value): POSITION must be NUMBER";
  end
  local f = Helper:LookupByDotNotation(self.json, field);
  if (f ~= nil) then
    if (type(f) ~= "table") then
      return error_insert_on_non_table(field);
    end
  else
    local p = Helper:LookupParentByDotNotation(self.json, field);
    if (p == nil) then
      return error_field_not_found(field);
    else
      local arr = build_single_element_array(pos, sval);
      return self:set(field, arr);
    end
  end
  local v    = Helper:ConvertStringToJson(sval);
  local ov   = convert_string_to_oplog_value(sval);
  local jpos = pos + 1; -- DATANET-API starts at ZERO, LUA starts at ONE
  table.insert(f, jpos, v); -- UPDATE JSON MIRROR
  add_to_oplog(self, "insert", field, {pos, ov});
  return nil;
end

local function run_client_commit(self, ns, cn, key, crdt, oplog, ccid)
  LT('run_client_commit');
  local kqk      = Helper:CreateKqk(ns, cn, key);
  local jreq     = Helper:CreateClientCommitRequest(ns, cn, key,
                                                    crdt, oplog, ccid);
  local res, err = RunMethod:RunWorker(kqk, jreq, nil);
  if (err ~= nil) then return nil, err; end
  return self:Create(ns, cn, res);
end

function _BD.commit(self)
  local ns    = self.namespace;
  local cn    = self.collection;
  local key   = self.key;
  local crdt  = self.crdt;
  local oplog = self.oplog;
  local ccid  = Helper:GetUniqueRequestID();
  return run_client_commit(self, ns, cn, key, crdt, oplog, ccid);
end

function on_create_error(err)
 if (type(err)   == "table" and
     err.message == Error.ReadNoDoc) then
   return nil, nil;
  else
    return nil, err;
  end
end

function _BD.Create(self, ns, cn, res)
  local crdt = res.crdt;
  local json = Helper:CreatePrettyJson(crdt);
  local deps = res.dependencies;
  local key  = json._id;
  return Helper:NewDocument(self, ns, cn, key, json, crdt, deps), nil;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- INTERNAL CLIENT COMMIT ---------------------------------------------------

local function send_internal_client_commit(premature, self, kqk, ccid, doc_root)
  if (premature) then return end
  local scrdt    = doc_root.scrdt;
  local dentry   = doc_root.dentry;
  local crdt     = cjson.decode(scrdt);
  local ns       = crdt._meta.ns;
  local cn       = crdt._meta.cn;
  local key      = crdt._meta._id;
  local jreq     = Helper:CreateInternalCommitRequest(ns, cn, key,
                                                      crdt, dentry);
  local res, err = RunMethod:RunWorker(kqk, jreq, nil);
  if (err ~= nil) then return false; end
  DeltaCache:Remove(kqk, ccid);
  local ndeps    = res.dependencies;
  DocumentCache:UpdateDependencies(kqk, ndeps);
  return true;
end

local chaos_send_internal_client_commit; -- NOTE: LOCAL FUNCION

local function do_send_internal_client_commit(self, kqk, ccid, doc_root)
  if (not ChaosDelaySendInternalClientCommit) then
    return send_internal_client_commit(nil, self, kqk, ccid, doc_root);
  else
    return chaos_send_internal_client_commit(self, kqk, ccid, doc_root);
  end
end

local function do_run_client_commit(premature, self, kqk, ccid, doc_root)
  if (premature) then return end
  LT('do_run_client_commit');
  do_send_internal_client_commit(self, kqk, ccid, doc_root);
end

function _BD.SendInternalClientCommit(self, kqk, ccid, doc_root)
  ngx.timer.at(0, do_run_client_commit, self, kqk, ccid, doc_root)
  return true;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CHAOS TESTING ------------------------------------------------------------

--NOTE: LOCAL FUNCTION
chaos_send_internal_client_commit = function(self, kqk, ccid, doc_root)
  local to = 30;
  LT('ChaosDelaySendInternalClientCommit: TO: ' .. to);
  ngx.timer.at(to, send_internal_client_commit, self, kqk, ccid, doc_root);
  local deps  = doc_root.dependencies;
  if (deps == nil) then deps = {}; end
  local uuid  = c_get_agent_uuid();
  local mydep = deps[uuid];
  if (mydep == nil) then mydep = 0; end
  mydep       = mydep + 1; -- FAKE DEPENDENCIES[] UPDATE
  LD('FAKE MY-DEPENDENCY: ' .. mydep);
  deps[uuid]  = mydep;
  DocumentCache:UpdateDependencies(kqk, deps);
end

return _BD;
