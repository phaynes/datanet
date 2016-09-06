
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _E  = {};
_E.__index = _E;

_E.BasicAuthFail     = "-ERROR: BASIC AUTHORIZATION FAILED";
_E.ReadAuthFail      = "-ERROR: READ AUTHORIZATION FAILED";
-- Error.ReadNoDoc: MUST MATCH shared.cpp ZS_Errors["NoDataFound"]
_E.ReadNoDoc         = "-ERROR: No Data Found";
_E.WriteAuthFail     = "-ERROR: WRITE AUTHORIZATION FAILED";
_E.StoreAuthFail     = "-ERROR: STORE AUTHORIZATION FAILED";
_E.SubscribeAuthFail = "-ERROR: SUBSCRIBE AUTHORIZATION FAILED";

return _E;
