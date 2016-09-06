
local Helper        = require "datanet.helpers";
local DocumentCache = require "datanet.document_cache";
local DirtyKeys     = require "datanet.dirty_keys";
local Debug         = require "datanet.debug";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _D = {};

local LuaDocReservedFields = {__index   = 1,
                              __root    = 2,
                              _id       = 3,
                              _channels = 3,
                             };

local CallType = {Increment = 1,
                  Insert    = 2,
                 };


local convert_table_to_document; -- LOCAL FUNCTION DEFINED BELOW
local create_document_metatable; -- LOCAL FUNCTION DEFINED BELOW
local init_document_root;        -- LOCAL FUNCTION DEFINED BELOW

local function is_typed_lua_table_object(__data)
  if (__data.__root == nil) then return false; end
  return (__data.__root.dtype == Helper.TableType.Object);
end

local function is_typed_lua_table_array(__data)
  if (__data.__root == nil) then return false; end
  return (__data.__root.dtype == Helper.TableType.Array);
end

local function is_typed_lua_table(__data)
  if     (is_typed_lua_table_object(__data)) then return true;
  elseif (is_typed_lua_table_array(__data))  then return true;
  else                                            return false;
  end
end

local function smart_pairs(self)
  if (self.pairs) then return self:pairs();
  else                 return pairs(self);
  end
end

local function __document_print(self, nested)
  local ptxt   = '';
  if (not nested) then
    ptxt       = ptxt .. 'KQK: ' .. self.__root.info.kqk .. ' {';
    local rtxt = '';
    for k, r in pairs(self.__root.info.chans) do
      if (string.len(rtxt) > 0) then
        rtxt = rtxt .. ', ';
      end
      rtxt = rtxt .. r;
    end
    ptxt = ptxt .. '_channels = [' .. rtxt .. '], ';
  end
  local etxt = '';
  for k, v in smart_pairs(self) do
    if (not LuaDocReservedFields[k]) then
      if (string.len(etxt) > 0) then
        etxt = etxt .. ', ';
      end
      if (type(v) == "table") then
        local isa  = is_typed_lua_table_array(v);
        if (isa) then
          etxt = etxt .. k .. ' = [' .. __document_print(v, true) .. ']';
        else
          etxt = etxt .. k .. ' = {' .. __document_print(v, true) .. '}';
        end
      elseif (type(v) == "string") then
        etxt = etxt .. k .. ' = "' .. tostring(v) .. '"';
      else
        etxt = etxt .. k .. ' = ' .. tostring(v);
      end
    end
  end
  ptxt = ptxt .. etxt;
  if (not nested) then
    ptxt = ptxt .. '}';
  end
  return ptxt;
end

function _D.Print(self, doc)
  return __document_print(doc, false);
end

local function print_typed_lua_table(v)
  local sv;
  local isa = is_typed_lua_table_array(v);
  if (isa) then sv = '[' .. __document_print(v, true) .. ']';
  else          sv = '{' .. __document_print(v, true) .. '}';
  end
  return sv;
end

function _D.Debug(self, doc)
  Helper:DebugOpLog(self.__root.oplog);
end

function _D.Size(self, doc)
  local i = 0;
  for k, v in doc:pairs() do
    i = i + 1;
  end
  return i;
end

local function undirty_key(__data)
  if (__data.__root.key ~= nil) then
    local _pd = __data.__root.data;
    return undirty_key(_pd);
  else
    local kqk = __data.__root.base_kqk;
    DirtyKeys:UnsetDirty(kqk);
  end
end

local function document_error(__data, emsg)
  LE('DOCUMENT ERROR: ' .. emsg);
  undirty_key(__data)
  error(emsg);
end


local function clone_data(self, nested)
  local data;
  if (not nested) then
    data = {_id = self.__root.info.kqk};
  else
    data = {__root = {dtype = self.__root.dtype}};
  end
  for k, v in self:pairs() do
    if (not LuaDocReservedFields[k]) then
      if (type(v) == "table") then
        data[k] = clone_data(v, true);
      else
        data[k] = v;
      end
    end
  end
  return data;
end

local function document_clone(self)
  local kqk   = self.__root.info.kqk;
  LT('document_clone: K: ' .. kqk);
  local scrdt = self.__root.scrdt;
  local chans = Helper:Copy(self.__root.info.chans);
  local deps  = Helper:Copy(self.__root.dependencies);
  local dvrsn = self.__root.delta_version;
  local ctbl  = clone_data(self, false);
  local ndoc  = convert_table_to_document(kqk, ctbl, nil, nil);
  return init_document_root(ndoc, kqk, scrdt, chans, deps, dvrsn);
end

local function __dnext(t, k)
  local m = getmetatable(t);
  local n = m and m.__next or next;
  return n(t,k);
end

local function dnext(t, k)
  local vv = nil;
  local kk = k;
  repeat
    kk, vv = __dnext(t, kk);
  until (not LuaDocReservedFields[kk]);
  return kk, vv;
end

local function document_pairs(self)
  return dnext, self, nil;
end

local function _ipairs(t, var)
  local nvar  = var + 1;
  local value = t[nvar];
  if value == nil then return; end
  return nvar, value;
end

local function document_ipairs(self)
  return _ipairs, self, 0;
end

local function document_cache_copy_on_write(kqk)
  local doc = DocumentCache:RawGet(kqk);
  if (doc == nil) then return true; -- NOT YET CACHED
  else
    LT('document_cache_copy_on_write: K: ' .. kqk);
    local ndoc = doc:clone();
    return DocumentCache:Set(kqk, ndoc, false, true);
  end
end

local function dirty_key(kqk, __data)
  local cherry = DirtyKeys:SetDirty(kqk, __data);
  if (not cherry) then return true;
  else                 return document_cache_copy_on_write(kqk);
  end
end

local function __add_oplog_entry(__data, oentry)
  if (__data.__root.initialized) then
    local kqk = __data.__root.base_kqk;
    dirty_key(kqk, __data);
  end
  table.insert(__data.__root.oplog, oentry);
  return true;
end

-- NOTE: OPLOG MUST BE CREATED BEFORE MODIFICATION ON __data[] IS PERFORMED
local function add_oplog_entry(__data, oentry)
  if (__data.__root.key ~= nil) then
    local _pd = __data.__root.data;
    return add_oplog_entry(_pd, oentry);
  else
    return __add_oplog_entry(__data, oentry);
  end
end

local function get_full_key(__data, k)
  if (__data.__root.key == nil) then return k;
  else
    local _pd = __data.__root.data;
    local pk  = __data.__root.key;
    local fk  = k and pk .. '.' .. k or pk;
    return get_full_key(_pd, fk);
  end
end

local function do_increment(__data, k, byval)
  local v = __data[k];
  if      (v == nil) then
    v = 0;
  elseif (type(v) ~= "number") then
    local err = "ERROR: INCREMENT on NON-NUMBER";
    LE(err);
    return nil, err;
  end
  local fk   = get_full_key(__data, k);
  if (Debug.OpLog) then
    LD('-------->INCREMENT: ' .. fk .. ' ' .. tostring(byval));
  end
  local oentry = {name = 'increment',
                  path = fk,
                  args = {byval}
                 };
  add_oplog_entry(__data, oentry); -- FIRST OPLOG -> THEN MODIFICATION
  local nval   = v + byval;
  __data[k]    = nval;
  return nval, nil;
end

local function do_insert(__data, spot, v)
  if (not is_typed_lua_table_array(__data)) then
    document_error(__data, "ERROR: INSERT ONLY ALLOWED ON ARRAYS");
  end
  local fk     = get_full_key(__data, nil);
  if (Debug.OpLog) then
    LD('-------->INSERT: ' .. fk .. ' ' .. spot .. ' ' .. tostring(v));
  end
  local oentry = {name = 'insert',
                  path = fk,
                  args = {spot, v}
                 };
  add_oplog_entry(__data, oentry); -- FIRST OPLOG -> THEN MODIFICATION
  local rspot  = spot + 1;  -- LUA ARRAYS START AT 1
  table.insert(__data, rspot, v);
  return v, nil;
end

local function document_increment(self, k, byval)
  return self(CallType.Increment, k, byval);
end

-- USAGE: doc:insert(2, 'abc');
local function document_insert(self, spot, v)
  return self(CallType.Insert, spot, v);
end

local function document_right_push(self, v)
  local spot = _D.Size(self, self);
  return document_insert(self, spot, v);
end

local function document_left_push(self, v)
  local spot = 0;
  return document_insert(self, spot, v);
end


local function document_new_array(self, name)
  local ctbl = Helper:CreateTypedLuaTableArray();
  self[name] = ctbl;
  return true;
end

-- NOTE: BASE-CLASS contains all DOCUMENT methods
local function get_base_class()
  local bc  = {pairs      = document_pairs,
               ipairs     = document_ipairs,
               clone      = document_clone,
               array      = document_new_array,
               increment  = document_increment,
               insert     = document_insert,
               right_push = document_right_push,
               left_push  = document_left_push,
              };
  return bc;
end

local function internal_clone_table(__data, k, v)
  if (Debug.OpLog) then
    LD('-------->COPY-ON-ASSIGNMENT: K: ' .. k);
  end
  for kk, vv in v:pairs() do
    __data[k][kk] = vv;
  end
end

convert_table_to_document = function(kqk, ctbl, rk, rd) -- NOTE: LOCAL FUNCTION
  local bc  = get_base_class();
  local doc = setmetatable(bc, create_document_metatable(kqk, ctbl, rk, rd));
  for k, v in pairs(ctbl) do
    if (not LuaDocReservedFields[k]) then
      doc[k] = v;
    end
  end
  return doc;
end

create_document_metatable = function(kqk, ctbl, rk, rd) -- NOTE: LOCAL FUNCTION
  local __data = {__root = {}};
  if (kqk ~= nil) then
    __data.__root.base_kqk = kqk;
    __data.__root.oplog    = {};
  else
     __data.__root.key   = rk;
     __data.__root.data  = rd;
     __data.__root.dtype = ctbl.__root.dtype;
  end
  local mt = {
    __next  = function(t, k)
      return dnext(__data, k)
    end,
    __index = function(table, k)
      return __data[k];
    end,
    __newindex = function(table, k, v)
      local del = (v == nil);
      if (del and __data[k] == nil) then return nil; end -- NO-OP
      if (is_typed_lua_table_array(__data) and (type(k) ~= "number")) then
        document_error(__data, "INVALID OPERATION: SET A FIELD IN AN ARRAY");
      end
      local fk = get_full_key(__data, k);
      local oentry;
      if (del) then oentry = {name = 'delete', path = fk, args = {}};
      else
        if (type(v) == "table" and is_typed_lua_table(v)) then
          local sv = print_typed_lua_table(v);
          oentry   = {name = 'set', path = fk, sargs = sv};
        else
          oentry   = {name = 'set', path = fk, args = {v}};
        end
      end
      add_oplog_entry(__data, oentry); -- FIRST OPLOG -> THEN MODIFICATION
      if (type(v) ~= "table") then
        if (Debug.OpLog) then
          if (del) then LD('-------->DELETE: ' .. fk);
          else          LD('-------->SET: ' .. fk .. ' ' .. v);
          end
        end
        __data[k] = v;
      else
        if (Debug.OpLog) then LD('-------->SET: ' .. fk .. ' TABLE'); end
        __data[k] = convert_table_to_document(nil, v, k, __data);
        if (v.increment == document_increment) then
          internal_clone_table(__data, k, v);
        end
      end
    end,
    __call = function(table, ct, ...) -- REPURPOSE _call() FOR INCREMENT
      if     (ct == CallType.Increment) then
        return do_increment(__data, ...);
      elseif (ct == CallType.Insert) then
        return do_insert(__data, ...);
      end
    end
  }
  return mt;
end

-- NOTE: LOCAL FUNCTION
init_document_root = function(doc, kqk, scrdt, chans, deps, dvrsn)
  doc.__root.oplog         = {};  -- RESET CONVERSION OPLOG
  doc.__root.scrdt         = scrdt;
  doc.__root.dependencies  = deps;
  doc.__root.delta_version = dvrsn;
  doc.__root.info          = {kqk   = kqk,
                              chans = chans,
                              auth  = {}}; -- NOTE: AUTH filled in later
  doc.__root.initialized   = true;
  return doc;
end

local function get_unique_delta_version(crdt)
  local auuid = string.format("%.0f", crdt._meta["_"]);
  local duuid = string.format("%.0f", crdt._meta.delta_version);
  return auuid .. "_" .. duuid;
end

function _D.New(self, bdoc)
  local ns    = bdoc.namespace;
  local cn    = bdoc.collection;
  local key   = bdoc.key;
  local kqk   = Helper:CreateKqk(ns, cn, key);
  LT('Document:New: K: ' .. kqk);
  local scrdt = cjson.encode(bdoc.crdt);
  local chans = bdoc.json._channels;
  local deps  = bdoc.dependencies;
  local dvrsn = get_unique_delta_version(bdoc.crdt);
  local ctbl  = Helper:ConvertCrdtToTypedLuaTable(bdoc.crdt);
  local doc   = convert_table_to_document(kqk, ctbl, nil, nil);
  return init_document_root(doc, kqk, scrdt, chans, deps, dvrsn);
end

return _D;

