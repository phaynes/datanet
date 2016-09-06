[
  {"comment" : "START: INIT MERGE TEST"},
  {"client" : "two", "command" : "cond_signal", "args" : ["W", "x", "=", 1]},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},
  {"comment" : "FINISH: INIT MERGE TEST"}
]
