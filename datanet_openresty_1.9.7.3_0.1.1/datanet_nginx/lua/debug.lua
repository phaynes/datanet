
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _DG  = {};
_DG.__index = _DG;

_DG.FullAgent                = true;
_DG.FullFrontend             = true;
_DG.FullSubscriber           = true;
_DG.FullNetwork              = true;

_DG.OpLog                    = true;
_DG.DocumentCache            = true;

_DG.CreateDiffPartitionTable = false;
_DG.FinalDiffPartitionTable  = false;

_DG.SHM                      = false;

_DG.AllowFrontendMethods     = true;
_DG.AllowAdminMethods        = true;

return _DG;
