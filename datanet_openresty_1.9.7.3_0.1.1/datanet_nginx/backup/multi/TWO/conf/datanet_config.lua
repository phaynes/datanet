
local _C = {};
_C.__index = _C;

_C.config = {worker = {
              uuid           = 2000000,
              port           = 25000,
              data_directory = "./data/"
            },
            cluster = {
              ip        = "127.0.0.1",
              port      = 28080,
              discovery = {
                ip   = "127.0.0.1",
                port = 18080
              }
            }
           };

return _C;
