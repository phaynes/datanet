
local _C = {};
_C.__index = _C;

_C.config = {worker = {
              uuid           = 1000000,
              port           = 15000,
              data_directory = "./data/"
            },
            cluster = {
              ip   = "127.0.0.1",
              port = 18080
            }
           };

return _C;
