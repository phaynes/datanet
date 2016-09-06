
local CollectionName = "testdata";
local Channel        = "0";
local Username       = "default";
local Password       = "password";

local args       = ngx.req.get_uri_args()
local machine_id = args['id'];       -- REQUIRED
local cmd        = args['cmd'];      -- REQUIRED
local field      = args['field'];    -- SET,INCREMENT,INSERT,DELETE
local value      = args['value'];    -- SET,INCREMENT,INSERT
local byval      = args['byvalue'];  -- INCREMENT
local pos        = args['position']; -- INSERT

if (cmd ~= nil) then cmd = string.upper(cmd); end

local ValidCommands = {INITIALIZE  =  0,
                       STATION     =  0,
                       DESTATION   =  0,
                       SUBSCRIBE   =  0,
                       UNSUBSCRIBE =  0,
                       GET         = -1,  -- ARGS: [field] (optional)
                       REMOVE      =  0,
                       CACHE       =  0,
                       EVICT       =  0,
                       SET         =  2,  -- ARGS: [field,value]
                       INCREMENT   =  2,  -- ARGS: [field,byvalue]
                       INSERT      =  3,  -- ARGS: [field,position,value]
                       DELETE      =  1}; -- ARGS: [field]

local nargs = ValidCommands[cmd];
if (nargs == nil) then
  ngx.say('ERROR: SUPPORTED COMMANDS:' .. 
          ' [INITIALIZE,STATION,DESTATION,SUBSCRIBE,UNSUBSCRIBE,' .. 
            'GET,REMOVE,CACHE,EVICT,' ..
            'SET,INCREMENT,INSERT,DELETE]');
  return;
end

local function handle_error(err)
  if (type(err) ~= "table") then ngx.say('ERROR: ' .. tostring(err));
  else                           ngx.say('ERROR: ' .. cjson.encode(err));
  end
end

local function argument_required(cmd, aname, arg)
  if (arg == nil) then
    ngx.say("ERROR: COMMAND " .. cmd .. " REQUIRES ARGUMENT '" .. aname .. "'");
    return false;
  end
  return true;
end

local function argument_json(cmd, value)
  local dval, err = Datanet.Network:SafeDecode(value);
  if (err ~= nil) then
    ngx.say("ERROR: COMMAND " .. cmd .. " REQUIRES JSON: (" .. value .. ")");
    return false;
  end
  return true;
end

local function argument_is_number(aname, num)
  num = tonumber(num);
  if ((num == nil) or (num < 0)) then
    ngx.say("ERROR: ARGUMENT: '" .. aname .. "' must be an integer > 0");
    return false
  end
  return true;
end

local function initialize_machine_document(collection)
  local now  = ngx.time();
  local mdoc = {_id       = machine_id,
                events    = Datanet.Helper:DataTypeCreateQueue(10, 2),
                ok        = 1,
                num       = 0,
                ts        = now
               };
  return collection:store(mdoc);
end

local AdminUsername = "admin";     -- NOTE: of course this is INSANE
local AdminPassword = "apassword"; --       putting admin passwords in scripts

local function grant_user_write()
  Datanet:switch_user(AdminUsername, AdminPassword);
  Datanet:grant_user(Username, Channel, "WRITE");
  Datanet:switch_user(Username, Password);
end

local function do_subscribe()
  local hit      = false;
  local iss, err = Datanet:is_subscribed(Channel);
  if (err) then return nil, err; end
  ngx.say('INFO: do_subscribe: IS_SUBSCRIBED: ' .. tostring(iss));
  if (not iss) then
    local ok, err = grant_user_write();
    if (err) then return nil, err; end
    local ok, err = Datanet:subscribe(Channel);
    if (err) then return nil, err; end
    ngx.say('INFO: SUBSCRIBED: ' .. Channel);
    hit = true;
  end
  return hit, nil;
end

local function do_unsubscribe()
  local hit      = false;
  local iss, err = Datanet:is_subscribed(Channel);
  if (err) then return nil, err; end
  ngx.say('INFO: do_unsubscribe: IS_SUBSCRIBED: ' .. tostring(iss));
  if (iss) then
    local ok, err = Datanet:unsubscribe(Channel);
    if (err) then return nil, err; end
    ngx.say('INFO: UNSUBSCRIBED: ' .. Channel);
    hit = true;
  end
  return hit, nil;
end

local function do_station()
  local hit      = false;
  local ist, err = Datanet:is_user_stationed(Username);
  if (err) then return nil, err; end
  ngx.say('INFO: do_station: IS_STATIONED: ' .. tostring(ist));
  if (not ist) then
    local ok, err = Datanet:station_user();
    if (err) then return nil, err; end
    ngx.say('INFO: STATIONED: ' .. Username);
    hit = true;
  end
  return hit, nil;
end

local function do_destation()
  local hit      = false;
  local ist, err = Datanet:is_user_stationed(Username);
  if (err) then return nil, err; end
  ngx.say('INFO: do_destation: IS_STATIONED: ' .. tostring(ist));
  if (ist) then
    local ok, err = Datanet:destation_user();
    if (err) then return nil, err; end
    ngx.say('INFO: DESTATIONED: ' .. Username);
    hit = true;
  end
  return hit, nil;
end

if (nargs > 0) then -- SET,INCREMENT,INSERT,DELETE
  if (not argument_required(cmd, "field", field)) then return; end
end
if (cmd == "SET" or cmd == "INSERT") then
  if (not argument_required(cmd, "value", value)) then return; end
  if (not argument_json(cmd, value)) then return; end
  if (field == "_id" or field == "_channels") then
    ngx.say("ERROR: SET on KEYWORD [_id, _channels]");
  end
end
if (cmd == "INCREMENT") then
  if (not argument_required(cmd, "byvalue", byval)) then return; end
  if (not argument_is_number("byvalue", byval)) then return; end
  byval = tonumber(byval);
end
if (cmd == "INSERT") then
  if (not argument_required(cmd, "position", pos)) then return; end
  if (not argument_is_number("position", pos)) then return; end
  pos = tonumber(pos);
end

if (cmd == "STATION") then
  local stahit, err = do_station();
  if (err) then return handle_error(err); end
  return;
elseif (cmd == "DESTATION") then
  local stahit, err = do_destation();
  if (err) then return handle_error(err); end
  return;
elseif (cmd == "SUBSCRIBE") then
  local subhit, err = do_subscribe();
  if (err) then return handle_error(err); end
  return;
elseif (cmd == "UNSUBSCRIBE") then
  local subhit, err = do_unsubscribe();
  if (err) then return handle_error(err); end
  return;
end

local collection = Datanet:collection(CollectionName);
local doc, err;
if (cmd == "INITIALIZE") then
  local stahit, err = do_station();
  if (err) then return handle_error(err); end
  local subhit, err = do_subscribe();
  if (err) then return handle_error(err); end
  if (stahit or subhit) then
    ngx.say('INFO: SLEEP 2 seconds -> CENTRAL CONFIRMS: INITIALIZATION');
    ngx.sleep(2);
  end
elseif (cmd == "REMOVE") then
  local ok, err = collection:remove(machine_id);
  if (err) then return handle_error(err); end
  ngx.say('INFO: REMOVE: KEY: ' .. machine_id);
  return;
elseif (cmd == "EVICT") then
  local ok, err = collection:evict(machine_id);
  if (err) then return handle_error(err); end
  ngx.say('INFO: EVICT: KEY: ' .. machine_id);
  return;
else
  if (cmd == "CACHE") then doc, err = collection:fetch(machine_id, true);
  else                     doc, err = collection:fetch(machine_id, false);
  end
  if (err) then return handle_error(err); end
  if (doc == nil) then
    doc, err = initialize_machine_document(collection);
    if (err) then return handle_error(err); end
  end
end

if (cmd == "GET") then
  local j = doc.json;
  if (field == nil) then
    ngx.say('DOCUMENT: ' .. cjson.encode(j));
  else
    local v = j[field];
    if (type(v) ~= "table") then
      ngx.say('GET: VALUE: ' .. tostring(v));
    else
      ngx.say('GET: VALUE: ' .. cjson.encode(v));
    end
  end
elseif (cmd == "SET") then
  local err      = doc:set(field, value);
  if (err) then return handle_error(err); end
  local doc, err = doc:commit();
  if (err) then return handle_error(err); end
  if (type(value) ~= "table") then
    ngx.say("SET: FIELD: " .. field .. " VALUE: " .. value);
  else
    ngx.say("SET: FIELD: " .. field .. " VALUE: " .. cjson.encode(value));
  end
elseif (cmd == "DELETE") then
  local err      = doc:delete(field);
  if (err) then return handle_error(err); end
  local doc, err = doc:commit();
  if (err) then return handle_error(err); end
  ngx.say("DELETE: FIELD: " .. field);
elseif (cmd == "INCREMENT") then
  local err      = doc:incr(field, byval);
  if (err) then return handle_error(err); end
  local doc, err = doc:commit();
  if (err) then return handle_error(err); end
  ngx.say("INCREMENT: FIELD: " .. field .. " BYVALUE: " .. byval);
elseif (cmd == "INSERT") then
  local err      = doc:insert(field, pos, value);
  if (err) then return handle_error(err); end
  local doc, err = doc:commit();
  if (err) then return handle_error(err); end
  if (type(value) ~= "table") then
    ngx.say("INSERT: FIELD: " .. field .. " POSITION: " .. pos ..
            " VALUE: " .. value);
  else
    ngx.say("INSERT: FIELD: " .. field .. " POSITION: " .. pos ..
            " VALUE: " .. cjson.encode(value));
  end 
end

