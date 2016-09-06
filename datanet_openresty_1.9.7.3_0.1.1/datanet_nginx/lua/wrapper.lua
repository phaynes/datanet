
-- NOTE: ALL GLOBAL FUNCTIONS/VARIABLES

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- LOG WRAPPERS --------------------------------------------------------------

LogNumber = 0; -- DEFAULT: TRACE

function LT(txt)
  if (LogNumber == 0) then
    c_log(txt, 0);
  end
end

function LD(txt)
  if (LogNumber ~= 2) then
    c_log(txt, 1);
  end
end

function LE(txt)
  c_log(txt, 2);
end

