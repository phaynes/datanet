[
  {"comment" : "START: MERGE TEST C,M"},
  {"client" : "one", "command" : "cond_signal", "args" : ["W", "x", "=", 2]},
  {"file" : "./test/conf/Create_TWO_Document_W_V2.cmd"},
  {"client" : "one", "command" : "cond_wait", "args" : ["W"]},


  {"client" : "two", "command" : "isolation", "args" : [true]},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "two", "command" : "remove", "args" : ["W"]},
  {"client" : "two", "command" : "cond_signal", "args" : ["W", "x", "=", 1]},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},
  {"comment" : "FINISH: MERGE TEST C,M"}
]
