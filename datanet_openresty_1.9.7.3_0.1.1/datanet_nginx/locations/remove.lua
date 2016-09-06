
local cname      = "users";
local collection = Datanet:collection(cname);

local atext = '{"_id" : "ADOC", "_channels" : [1], "a" : 1, "purpose" : "DELETE-ME"}';
ngx.say('STORE: ' .. atext);
local ret, err = collection:store(atext);
Datanet:generic_handle_response(ret, err);

local akey = "ADOC";
ngx.say('REMOVE: ' .. akey);
local ret, err = collection:remove(akey);
Datanet:generic_handle_response(ret, err);

