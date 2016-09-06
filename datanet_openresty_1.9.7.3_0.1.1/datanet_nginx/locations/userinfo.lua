
local config = Datanet:get_config();
local args   = ngx.req.get_uri_args()
local user   = args['user'];
ngx.say('USER-INFO: USER: ' .. user);

local cname      = "users";
local collection = Datanet:collection(cname);
local kuinfo     = "UserInfo_" .. user;
local doc, err   = collection:get(kuinfo);
if (err ~= nil) then
  ngx.say('ERROR: ' .. tostring(err));
  return;
end
if (doc == nil) then
  ngx.say('NO USER INFO');
  return;
end

doc.new_field = 7;
doc:increment("number_access", 1);
doc.age = doc.age + 1;
local w = doc.weight;
if (w) then doc.weight = nil; -- DELETE FIELD
else        doc.weight = math.random(100, 150);
end

local ts      = ngx.time();
local uaction = "got user info at: " .. ts;
local faction = "FIRST ACTION: " .. ts;
doc.actions:right_push(uaction);
doc.actions:left_push (faction);

if (doc.attributes ~= nil) then
  doc.attributes:array("narr");
end

local dtxt = Datanet.Document:Print(doc)
ngx.say('DOC: ' .. dtxt);


local cname      = "statistics";
local collection = Datanet:collection(cname);
local pc         = "page_counts";
local doc, err   = collection:get(pc);
if (err ~= nil) then
  ngx.say('ERROR: ' .. tostring(err));
  return;
end
if (doc == nil) then
  ngx.say('NO PAGE COUNTS');
  return;
end
doc:increment("total", 1);
doc.counts:increment("user_info", 1);


local suri = config.cluster.ip .. ':' .. config.cluster.port;
ngx.say('SERVED FROM CLUSTER-NODE: ' .. suri .. ' PID: ' .. ngx.worker.pid());

