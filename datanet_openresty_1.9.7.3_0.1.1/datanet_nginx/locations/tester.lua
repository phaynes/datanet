
local http = require "datanet.http";

-- TEST USE-CASE SETTINGS
local CollectionName = "testdata";
local Key            = "unit_test";

-- TEST DEFAULT SETTINGS
local Channel        = "0";
local Username       = "default";
local Password       = "password";

-- ERROR USE-CASE SETTINGS
local MissingKey     = "abcdefghijklmnopqrstuvwxyz";
local MissingField   = "abc.efg.ijk.mno.qrs.uvw.yz";
local BogusUsername  = "BogusUsername";
local BogusPassword  = "BogusPassword";
local BogusChannel   = "BogusChannel";

local function ngx_say_flush(txt)
  ngx.say(txt);
  ngx.flush();
end

local function build_client_notify_body(url)
  local jreq = {jsonrpc = "2.0",
                method  = "ClientNotify",
                id      = 1,
                params = {
                  data = {
                    command = "ADD",
                    url     = url
                  },
                  authentication = {
                    username = "admin",
                    password = "apassword"
                  }
                }
               };
  return cjson.encode(jreq);
end

local function send_https_request(url, jbody)
  local options;
  if (jbody == nil) then
    options = {ssl_verify = false};
  else
    options = {
      path       = "/agent";
      method     = "POST",
      body       = jbody,
      ssl_verify = false
    };
  end
  local httpc     = http.new()
  local hres, err = httpc:request_uri(url, options);
  if (err) then return nil,       err;
  else          return hres.body, nil;
  end
end

local function build_url(ip, port, location, args)
  local url = "https://" .. ip .. ":" .. port .. "/" .. location;
  if (args ~= nil) then url = url .. "?" .. args; end
  return url;
end

local function register_notify(sip, sport)
  local nurl     = build_url(sip, sport, "notify");
  local nbody    = build_client_notify_body(nurl);
  local url      = build_url(sip, sport, "agent");
  local res, err = send_https_request(url, nbody);
  if (err) then return err; end
  ngx_say_flush('PASS: NOTIFY: HTTP RESULT: ' .. res);
  return nil;
end

local function remove_document(sip, sport)
  local args     = "id=" .. Key .. "&cmd=REMOVE";
  local url      = build_url(sip, sport, "client", args);
  local res, err = send_https_request(url);
  if (err) then return err; end
  -- NOTE: do NOT check status -> OK: REMOVE on NON-existent key
  ngx_say_flush('PASS: REMOVE: HTTP RESULT: ' .. res);
  return nil;
end

local function get_client_error(res)
  return string.sub(res, 8);
end

local function check_client_status(res)
  local status   = string.match(res, '[A-Z]*:')
  if (status == "ERROR:") then return get_client_error(res); end
  return nil;
end

local function do_run_operation(url, desc)
  local res, err = send_https_request(url);
  if (err) then return err; end
  local err      = check_client_status(res);
  if (err) then return err; end
  ngx_say_flush('PASS: ' .. desc .. ': HTTP RESULT: ' .. res);
  return nil;
end

local function run_command(sip, sport, cmd)
  local args = "id=" .. Key .. "&cmd=" .. cmd;
  local url  = build_url(sip, sport, "client", args);
  return do_run_operation(url, cmd);
end

local function run_operation(sip, sport, op, path, value, byvalue, pos)
  local args = "id=" .. Key .. "&cmd=" .. op .. "&field=" .. path;
  if (value   ~= nil) then args = args .. "&value="    .. value;   end
  if (byvalue ~= nil) then args = args .. "&byvalue="  .. byvalue; end
  if (pos     ~= nil) then args = args .. "&position=" .. pos; end
  local url  = build_url(sip, sport, "client", args);
  return do_run_operation(url, "run_operation");
end

local function run_and_validate_operation(sip, sport, op, path,
                                          value, byvalue, pos)
  local cvalue = (value ~= nil) and value              or byvalue;
  local cpath  = (pos ~= nil)   and path .. "." .. pos or path;
  local cname  = Datanet.Helper:BuildConditionName(op, cpath, cvalue);
  local err    = Datanet.Helper:ConditionInitialize(cname);
  if (err) then return err; end
  local err    = run_operation(sip, sport, op, path, value, byvalue, pos);
  if (err) then return err; end
  Datanet.Helper:ConditionWait(cname);
  ngx_say_flush('PASS: CONDITION MET: ' .. cname);
  return nil;
end

local function run_set_TS_command(sip, sport)
  local op    = "set";
  local path  = "ts";
  local value = ngx.time();
  return run_and_validate_operation(sip, sport, op, path, value, nil, nil);
end

local function cache_modify_evict(sip, sport)
  local err   = run_command(sip, sport, "CACHE");
  if (err) then return err; end
  local err   = run_set_TS_command(sip, sport);
  if (err) then return err; end
  ngx_say_flush('PASS: cache_modify_evict: SET');
  local err   = run_command(sip, sport, "EVICT");
  if (err) then return err; end
  return nil;
end

function setup_tests(sip, sport)
  local err = register_notify(sip, sport);
  if (err) then return err; end

  local err = run_command(sip, sport, "INITIALIZE"); -- INITIALIZE TO REMOVE
  if (err) then return err; end

  local err = remove_document(sip, sport);
  if (err) then return err; end

  local err = run_command(sip, sport, "INITIALIZE"); -- INITIALIZE AND STORE
  if (err) then return err; end

  local err = run_set_TS_command(sip, sport);
  if (err) then return err; end
  return nil;
end

function test_increment_insert_delete(sip, sport)
  local op      = "increment";
  local path    = "num";
  local byvalue = 11;
  local err     = run_and_validate_operation(sip, sport, op, path,
                                             nil, byvalue, nil);
  if (err) then return err; end
  ngx_say_flush('PASS: test_increment_insert_delete: INCREMENT');

  local op    = "insert";
  local path  = "events";
  local value = '"BIG OLE EVENT at ' .. ngx.time() .. '"';
  local pos   = 0;
  local err   = run_and_validate_operation(sip, sport, op, path,
                                           value, nil, pos);
  if (err) then return err; end
  ngx_say_flush('PASS: test_increment_insert_delete: INSERT');

  local op    = "delete";
  local path  = "ts";
  local err   = run_and_validate_operation(sip, sport, op, path,
                                           nil, nil, nil);
  if (err) then return err; end
  ngx_say_flush('PASS: test_increment_insert_delete: DELETE');

  return nil;
end

function unsubcribe_cache_subscribe_test(sip, sport)
  local err = run_command(sip, sport, "UNSUBSCRIBE");
  if (err) then return err; end
  ngx_say_flush('INFO: SLEEP 2 seconds: CENTRAL CONFIRMS: UNSUBSCRIBE');
  ngx.sleep(2);

  local err = cache_modify_evict(sip, sport);
  if (err) then return err; end
  ngx_say_flush('INFO: SLEEP 2 seconds: CENTRAL CONFIRMS: EVICT');
  ngx.sleep(2);

  local err = cache_modify_evict(sip, sport);
  if (err) then return err; end

  local err = run_command(sip, sport, "SUBSCRIBE");
  if (err) then return err; end
  ngx_say_flush('INFO: SLEEP 2 seconds: CENTRAL CONFIRMS: SUBSCRIBE');
  ngx.sleep(2);

  local err   = run_set_TS_command(sip, sport);
  if (err) then return err; end
  return nil;
end

function test_cache_modify_evict_cache(collection)
  local ok, err = Datanet:unsubscribe(Channel);
  if (err) then return err; end
  ngx_say_flush('INFO: SLEEP 2 seconds: CENTRAL CONFIRMS: UNSUBSCRIBE');
  ngx.sleep(2);

  local doc, err = collection:fetch(Key, true);
  if (err) then return err; end

  -- TEST: C-M-E-C(SET)
  local field    = "ts";
  local value    = ngx.time();
  local err      = doc:set(field, value);
  if (err) then return err; end
  local doc, err = doc:commit();
  if (err) then return err; end

  local ok, err = collection:evict(Key);
  if (err) then return err; end
  ngx_say_flush('INFO: SLEEP 2 seconds: CENTRAL CONFIRMS: EVICT');
  ngx.sleep(2);

  local doc, err = collection:fetch(Key, true);
  if (err) then return err; end
  local j        = doc.json; -- GET DOCUMENT's JSON
  local ts       = j.ts;     -- GET FIELD(TS)
  if (ts == value) then
    ngx_say_flush('PASS: C-M-E-C(SET): TS: ' .. ts ..
                  ' CORRECT(TS): ' .. value);
  else
    return 'ERROR: C-M-E-C(SET): TS: ' .. ts .. ' CORRECT(TS): ' .. value;
  end

  -- TEST: C-M-E-C(INSERT)
  local field    = "events";
  local pos      = 0;
  local value    = '"BIG OLE EVENT at ' .. ngx.time() .. '"';
  local err      = doc:insert(field, pos, value);
  if (err) then return err; end
  local doc, err = doc:commit();
  if (err) then return err; end

  local ok, err = collection:evict(Key);
  if (err) then return err; end
  ngx_say_flush('INFO: SLEEP 2 seconds: CENTRAL CONFIRMS: EVICT');
  ngx.sleep(2);

  local doc, err = collection:fetch(Key, true);
  if (err) then return err; end
  local j        = doc.json; -- GET DOCUMENT's JSON
  local events   = j.events; -- GET FIELD(EVENTS)
  local fev      = events[1];
  if (fev == value) then
    ngx_say_flush('PASS: C-M-E-C(INSERT): EV: ' .. fev ..
                  ' CORRECT(EV): ' .. value);
  else
    return 'ERROR: C-M-E-C(INSERT): EV: ' .. fev .. ' CORRECT(EV): ' .. value;
  end

  return nil;
end

function test_retrieve_missing_key(collection)
  local doc, err   = collection:fetch(MissingKey, false);
  if (err) then return 'ERROR: FETCH: MISSING_KEY: ' .. err;        end
  if (doc) then return 'ERROR: FETCH: MISSING_KEY: FOUND DOCUMENT'; end
  ngx_say_flush('PASS: FETCH: MISSING_KEY: MISS');

  local doc, err = collection:fetch(MissingKey, true);
  if (err) then return 'ERROR: FETCH: MISSING_KEY: ' .. err;        end
  if (doc) then return 'ERROR: CACHE: MISSING_KEY: FOUND DOCUMENT'; end
  ngx_say_flush('PASS: CACHE: MISSING_KEY: MISS');
  return nil;
end

function test_remove_missing_key(collection)
  local ok, err = collection:evict(MissingKey);
  if (err) then return 'ERROR: EVICT: ' .. err; end
  ngx_say_flush('PASS: EVICT: MISSING_KEY: NO ERROR: OK');
  
  local ok, err = collection:remove(MissingKey);
  if (err == nil) then return 'ERROR: REMOVE: MISSING-KEY: WORKED'; end
  ngx_say_flush('PASS: REMOVE: MISSING_KEY: GOT ERROR: ' .. err);
  return nil;
end

function test_bogus_username_channel()
  Datanet:switch_user(BogusUsername, BogusPassword); -- SWITCH TO BOGUS-NAME

  local ok, err = Datanet:station_user();
  if (not err) then return 'ERROR: STATION-USER: BogusUsername: NO ERROR'; end
  ngx_say_flush('PASS: STATION-USER: BogusUsername: GOT ERROR: ' .. err);

  local ok, err = Datanet:destation_user();
  if (not err) then return 'ERROR: DESTATION-USER: BogusUsername: NO ERROR'; end
  ngx_say_flush('PASS: DESTATION-USER: BogusUsername: GOT ERROR: ' .. err);

  local ok, err = Datanet:subscribe(Channel);
  if (not err) then return 'ERROR: SUBSCRIBE: BogusUsername: NO ERROR'; end
  ngx_say_flush('PASS: SUBSCRIBE: BogusUsername: GOT ERROR: ' .. err);

  Datanet:switch_user(Username, Password);           -- REVERT BACK TO DEFAULT

  local ok, err = Datanet:unsubscribe(BogusChannel);
  if (not err) then return 'ERROR: UNSUBSCRIBE: BogusChannel: NO ERROR'; end
  ngx_say_flush('PASS: UNSUBSCRIBE: BogusChannel: GOT ERROR: ' .. err);
  return nil;
end

function test_store_bad_input(collection)
  local mdoc     = {num = 1}; -- NO ID
  local doc, err = collection:store(mdoc);
  if (not err) then
    return 'ERROR: STORE: BAD INPUT(LUA: NO ID): NO ERROR';
  end
  ngx_say_flush('PASS: STORE: BAD INPUT(LUA: NO ID): GOT ERROR: ' .. err);

  local mtxt     = '{"num" : 1}'; -- NO ID
  local doc, err = collection:store(mtxt);
  if (not err) then
   return 'ERROR: STORE: BAD INPUT(JSON: NO ID): NO ERROR';
  end
  ngx_say_flush('PASS: STORE: BAD INPUT(JSON: NO ID): GOT ERROR: ' .. err);

  local mtxt     = '{INVALID}'; -- INVALID
  local doc, err = collection:store(mtxt);
  if (not err) then
   return 'ERROR: STORE: BAD INPUT(JSON: INVALID): NO ERROR';
  end
  ngx_say_flush('PASS: STORE: BAD INPUT(JSON: INVALID): GOT ERROR: ' .. err);
  return nil;
end

function test_store_bad_operations(collection)
  local mtxt     = '{"_id" : "OP_TEST_2", "n" : 2}';
  local doc, err = collection:store(mtxt);
  if (err) then return err; end
  ngx_say_flush('PASS: STORE: JSON-STRING');

  local tkey     = "OP_TEST_1";
  local mdoc     = {_id = tkey,
                    n   = 1,
                    s   = "AM-STRING-1",
                    a   = {1},
                    o   = {x=11}
                   };
  local doc, err = collection:store(mdoc);
  if (err) then return err; end
  ngx_say_flush('PASS: STORE: LUA-TABLE');

  local err = doc:set(MissingField, 111);
  if (not err) then return 'ERROR: SET: MissingField: WORKED'; end
  ngx_say_flush('PASS: SET: MissingField: ERROR: ' .. err);
  local err = doc:delete(MissingField);
  if (not err) then return 'ERROR: DELETE: MissingField: WORKED'; end
  ngx_say_flush('PASS: DELETE: MissingField: ERROR: ' .. err);
  local err = doc:incr(MissingField, 111);
  if (not err) then return 'ERROR: INCREMENT: MissingField: WORKED'; end
  ngx_say_flush('PASS: INCREMENT: MissingField: ERROR: ' .. err);
  local err = doc:insert(MissingField, 0, 111);
  if (not err) then return 'ERROR: INSERT: MissingField: WORKED'; end
  ngx_say_flush('PASS: INSERT: MissingField: ERROR: ' .. err);

  local err = doc:set("n.x", 111);
  if (not err) then return "ERROR: NESTED-SET ON NON-OBJECT: NO ERROR"; end
  ngx_say_flush('PASS: NESTED-SET ON NON-OBJECT: ERROR: ' .. err);

  local err = doc:set();
  if (not err) then return 'ERROR: SET MISSING KEY: NO ERROR'; end
  ngx_say_flush('PASS: SET MISSING KEY: ERROR: ' .. err);
  local err = doc:set("o.y");
  if (not err) then return 'ERROR: SET MISSING VALUE: NO ERROR'; end
  ngx_say_flush('PASS: SET MISSING VALUE: ERROR: ' .. err);
  local err = doc:delete();
  if (not err) then return 'ERROR: DELETE MISSING KEY: NO ERROR'; end
  ngx_say_flush('PASS: DELETE MISSING KEY: ERROR: ' .. err);
  local err = doc:incr();
  if (not err) then return 'ERROR: INCR MISSING KEY: NO ERROR'; end
  ngx_say_flush('PASS: INCR MISSING KEY: ERROR: ' .. err);
  local err = doc:incr("n");
  if (not err) then return 'ERROR: INCR MISSING BYVAL: NO ERROR'; end
  ngx_say_flush('PASS: INCR MISSING BYVAL: ERROR: ' .. err);
  local err = doc:insert();
  if (not err) then return 'ERROR: INSERT MISSING KEY: NO ERROR'; end
  ngx_say_flush('PASS: INSERT MISSING KEY: ERROR: ' .. err);
  local err = doc:insert("a");
  if (not err) then return 'ERROR: INSERT MISSING POSITION: NO ERROR'; end
  ngx_say_flush('PASS: INSERT MISSING POSITION: ERROR: ' .. err);
  local err = doc:insert("a", 0);
  if (not err) then return 'ERROR: INSERT MISSING VALUE: NO ERROR'; end
  ngx_say_flush('PASS: INSERT MISSING VALUE: ERROR: ' .. err);

  local err = doc:incr("s", 111);
  if (not err) then return 'ERROR: INCR NON NUMERICAL FIELD(S): NO ERROR'; end
  ngx_say_flush('PASS: INCR NON NUMERICAL FIELD(S): ERROR: ' .. err);
  local err = doc:incr("a", 111);
  if (not err) then return 'ERROR: INCR NON NUMERICAL FIELD(A): NO ERROR'; end
  ngx_say_flush('PASS: INCR NON NUMERICAL FIELD(A): ERROR: ' .. err);
  local err = doc:incr("o", 111);
  if (not err) then return 'ERROR: INCR NON NUMERICAL FIELD(O): NO ERROR'; end
  ngx_say_flush('PASS: INCR NON NUMERICAL FIELD(O): ERROR: ' .. err);

  local err      = doc:insert("n", 0, 111);
  if (not err) then return 'ERROR: INSERT NON ARRAY FIELD(N): NO ERROR'; end
  ngx_say_flush('PASS: INSERT NON ARRAY FIELD(N): ERROR: ' .. err);
  local err      = doc:insert("s", 0, 111);
  if (not err) then return 'ERROR: INSERT NON ARRAY FIELD(S): NO ERROR'; end
  ngx_say_flush('PASS: INSERT NON ARRAY FIELD(S): ERROR: ' .. err);

  local err = doc:insert("a", "BAD", 111);
  if (not err) then return 'ERROR: INSERT POSITION NOT NUMBER: NO ERROR'; end
  ngx_say_flush('PASS: INSERT POSITION NOT NUMBER: ERROR: ' .. err);

  local doc, err = collection:fetch(tkey, false);
  local err      = doc:set("a.x", 111); -- NOTE: caught on COMMIT
  local doc, err = doc:commit();
  if (not err) then return "ERROR: ARRAY-SET NON NUMERICAL INDEX: NO ERROR"; end
  ngx_say_flush('PASS: ARRAY-SET NON NUMERICAL INDEX: ERROR: ' .. err);

  local doc, err = collection:fetch(tkey, false);
  local err      = doc:set("_id",       111); -- NOTE: caught on COMMIT
  local doc, err = doc:commit();
  if (not err) then return 'ERROR: SET RESERVED FIELD(ID): NO ERROR'; end
  ngx_say_flush('PASS: SET RESERVED FIELD(ID): ERROR: ' .. err);

  local doc, err = collection:fetch(tkey, false);
  local err      = doc:set("_channels", 111); -- NOTE: caught on COMMIT
  local doc, err = doc:commit();
  if (not err) then return 'ERROR:  SET RESERVED FIELD(CHANNELS): NO ERROR'; end
  ngx_say_flush('PASS: SET RESERVED FIELD(CHANNELS): ERROR: ' .. err);

  local doc, err = collection:fetch(tkey, false);
  local err      = doc:set("_meta",     111); -- NOTE: caught on COMMIT
  local doc, err = doc:commit();
  if (not err) then return 'ERROR: SET RESERVED FIELD(META): NO ERROR'; end
  ngx_say_flush('PASS: SET RESERVED FIELD(META): ERROR: ' .. err);

  local doc, err = collection:fetch(tkey, false);
  local err      = doc:insert("o", 0, 111); -- NOTE: caught on COMMIT
  local doc, err = doc:commit();
  if (not err) then return 'ERROR: INSERT NON ARRAY FIELD(O): NO ERROR'; end
  ngx_say_flush('PASS: INSERT NON ARRAY FIELD(O): ERROR: ' .. err);

  return nil;
end

function exit_tests(sip, sport)
  local err = run_command(sip, sport, "UNSUBSCRIBE");
  if (err) then return err; end

  local err = run_command(sip, sport, "DESTATION");
  if (err) then return err; end
  return nil;
end

local sip   = ngx.var.server_addr;
-- NOTE: HTTP/S CONFIG DEPENDENCY (e.g. 8080->4000 or 8081->4001)
local sport = ngx.var.server_port - 4080;

local err   = setup_tests(sip, sport);
if (err) then return ngx_say_flush('ERROR: setup_tests: ' .. err); end

local err   = test_increment_insert_delete(sip, sport)
if (err) then
  return ngx_say_flush('ERROR: test_increment_insert_delete: ' .. err);
end

local err   = unsubcribe_cache_subscribe_test(sip, sport);
if (err) then
  return ngx_say_flush('ERROR: unsubcribe_cache_subscribe_test: ' .. err);
end

-- DIRECT API TESTS
local collection = Datanet:collection(CollectionName);
local err        = test_cache_modify_evict_cache(collection);
if (err) then
  return ngx_say_flush('ERROR: test_cache_modify_evict_cache: ' .. err);
end

local err = test_retrieve_missing_key(collection);
if (err) then
  return ngx_say_flush('ERROR: test_retrieve_missing_key: ' .. err);
end

local err = test_remove_missing_key(collection);
if (err) then
  return ngx_say_flush('ERROR: test_remove_missing_key: ' .. err);
end

local err = test_bogus_username_channel();
if (err) then
  return ngx_say_flush('ERROR: test_bogus_username_channel: ' .. err);
end

local err = test_store_bad_input(collection);
if (err) then
  return ngx_say_flush('ERROR: test_store_bad_input: ' .. err);
end

local err = test_store_bad_operations(collection);
if (err) then
  return ngx_say_flush('ERROR: test_store_bad_operations: ' .. err);
end

local err = exit_tests(sip, sport);
if (err) then return ngx_say_flush('ERROR: exit_tests: ' .. err); end

ngx_say_flush('TESTS SUCCEEDED');
