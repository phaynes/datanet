
Datanet:switch_user("hbuser", "password");

local cname      = "users";
local key        = "BDOC";
local collection = Datanet:collection(cname);

ngx.say('FETCH: K: ' .. key);
local doc, err = collection:fetch(key);
if (err ~= nil) then
  Datanet:generic_handle_response(nil, err);
  return;
end

ngx.say('INCR: F: b V: 22');
doc:incr("b", 22);

ngx.say('COMMIT: K: ' .. key);
local ret, err = doc:commit();
Datanet:generic_handle_response(ret, err);

c_log('RequestUUID: ' .. ngx.var.RequestUUID);
