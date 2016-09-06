
local _C = {};
_C.__index = _C;

_C.config = {
   name    = "SERVER_1",                   -- UNIQUE DORA NAME
   central = {
     name     = "D1",                      -- CENTRAL NAME
     https    = {
       hostname = "127.0.0.1",             -- CENTRAL HOSTNAME (OUTGOING)
       port     = 10101                    -- CENTRAL PORT (OUTGOING)
     },
     callback = {
       hostname = "127.0.0.1",             -- DORA HOSTNAME (INCOMING)
       port     = 10080,                   -- DORA PORT (INCOMING)
       key      = "UNIQUE KEY PER AGENT 1" -- UNIQUE DORA KEY
     }
   }
 };

ngx.log(ngx.NOTICE, 'LOADED CONFIG FILE: ' .. _C.config.name);

return _C;
