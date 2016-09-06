
local pin        = false;
local watch      = false;
local sticky     = false;

local cname      = "users";
local collection = Datanet:collection(cname);

local ckey = "CDOC";
pin        = true;
watch      = false;
ngx.say('CACHE: K: ' .. ckey);
local doc, err = collection:cache(ckey, pin, watch, sticky);
Datanet:generic_handle_response(doc, err);
ngx.sleep(2);

local dkey = "DDOC";
pin        = false;
watch      = true;
ngx.say('CACHE: K: ' .. dkey);
local doc, err = collection:cache(dkey, pin, watch, sticky);
Datanet:generic_handle_response(doc, err);
ngx.sleep(2);

local ekey = "EDOC";
pin        = false;
watch      = false;
ngx.say('CACHE: K: ' .. ekey);
local doc, err = collection:cache(ekey, pin, watch, sticky);
Datanet:generic_handle_response(doc, err);
ngx.sleep(2);

ngx.say('EVICT: K: ' .. ckey);
local res, err = collection:evict(ckey);
Datanet:generic_handle_response(res, err);

--TODO: CACHE MISS: XDOC
