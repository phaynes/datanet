
local _C = {};
_C.__index = _C;

_C.config = {
   name    = "SERVER_2",
   central = {
     name     = "D1",
     https    = {
       hostname = "127.0.0.1",
       port     = 10101
     },
     callback = {
       hostname = "localhost",
       port     = 10081,
       key      = "UNIQUE KEY PER AGENT 2"
     }
   },
   default = {
     log_configuration_file = "./conf/two_easylogging.conf",
     ldb_data_directory     = "./TWO/LDB/";
   }
 };

ngx.log(ngx.NOTICE, 'LOADED CONFIG FILE: ' .. _C.config.name);

return _C;
