[
  {"comment" : "START: MERGE TEST A,L"},
  {"client" : "two", "command" : "cond_signal", "args" : ["W", "x", "=", 1]},
  {"client" : "two", "command" : "isolation", "args" : [true]},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},
  {"comment" : "FINISH: MERGE TEST L"}
]
