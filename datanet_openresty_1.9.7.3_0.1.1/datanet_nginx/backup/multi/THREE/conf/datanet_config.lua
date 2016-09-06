
local _C = {};
_C.__index = _C;

_C.config = {worker = {
              uuid           = 3000000,
              port           = 35000,
              data_directory = "./data/"
            },
            cluster = {
              ip        = "127.0.0.1",
              port      = 38080,
              discovery = {
                ip   = "127.0.0.1",
                port = 18080
              }
            }
           };

return _C;
