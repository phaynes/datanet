
local args   = ngx.req.get_uri_args()
local user   = args['user'];
ngx.say('USER-INFO: USER: ' .. user);

local cname      = "users";
local collection = Datanet:collection(cname);
local kuinfo     = "UserInfo_" .. user;
local doc, err   = collection:get(kuinfo);
if (err) then
  ngx.say("ERROR: collection:get: " .. err);
  return;
end
if (doc == nil) then
  ngx.say("NO USER INFO");
  return;
end

ngx.say('TEST: doc.actions:insert(2, 22);');
local res, err = doc.actions:insert(2, 22);
if (err) then ngx.say(err);
else          ngx.say("->PASSED");
end

ngx.say('TEST: doc.attributes:insert(2, 22);');
local status, err = pcall(function ()
  doc.attributes:insert(2, 22);
end
);
if (err) then ngx.say(err);
else          ngx.say("->PASSED");
end

ngx.say('TEST: doc:increment("height", 44);');
local res, err = doc:increment("height", 44);
if (err) then ngx.say(err);
else          ngx.say("->PASSED");
end

ngx.say('TEST: doc:increment("lastname", 44);');
local res, err = doc:increment("lastname", 44);
if (err) then ngx.say(err);
else          ngx.say("->PASSED");
end

local status, err = pcall(function ()
  ngx.say('TEST: doc.actions.FIELD = 33;');
  doc.actions.FIELD = 33; -- THROWS ERROR
end
);
if (err) then ngx.say(err);
else          ngx.say("->PASSED");
end

