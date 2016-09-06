
local _C = {};
_C.__index = _C;

_C.config = {name    = "SERVER_1",
             central = {
               name     = "D1",
               https    = {
                 hostname = "127.0.0.1",
                 port     = 10100
               },
               callback = {
                 hostname = "localhost",
                 port     = 10080,
                 key      = "UNIQUE KEY PER AGENT 1"
               }
             },
             sticky = {
               shard_key            = 'user',
               port                 = 8080,
               unix_socket_wildcard = "unix:/tmp/sticky_nginx_socket_"
             },
             cluster = {
               ip   = "localhost",
               port = 9080
             },
             cache  = { max_bytes       = 100000 },
             deltas = { max_bytes       = 30000 },
             log    = { level           = "TRACE" },
             debug  = { pid_file_prefix = "/tmp/NGINX_PID"; }
            };

ngx.log(ngx.NOTICE, 'LOADED CONFIG FILE: ' .. _C.config.name);

return _C;
