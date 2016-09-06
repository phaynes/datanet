
local _C = {};
_C.__index = _C;

_C.config = {
            name    = "SERVER_2",
            central = {
              name     = "D1",
              hostname = "USA",
              port     = 10000,
              https    = {
                hostname = "127.0.0.1", --USA
                port     = 10100
              },
              callback = {
                hostname = "localhost",
                port     = 10081,
                key      = "UNIQUE KEY PER AGENT 2"
              }
            },
            cache = {
              max_bytes = 10000
            },
            sticky = {
              shard_key            = 'user',
              port                 = 8081,
              unix_socket_wildcard = "unix:/tmp/sticky_nginx_socket_"
            },
            cluster = {
              ip        = "127.0.0.1",
              port      = 9081,
              discovery = { -- POINTS to FIRST NGINX_AGENT
                ip   = "127.0.0.1",
                port = 9080
              }
            },
            debug = {
              pid_file_prefix = "/tmp/NGINX_PID";
            }
           };

ngx.log(ngx.NOTICE, 'LOADED CONFIG FILE: ' .. _C.config.name);

return _C;
