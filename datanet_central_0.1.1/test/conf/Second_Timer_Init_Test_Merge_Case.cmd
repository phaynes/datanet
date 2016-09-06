[
  {"comment" : "START: INIT MERGE TEST"},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "second_timer", "args" : [true]},
  {"client" : "two", "command" : "cond_signal", "args" : ["W", "x", "=", 1]},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "one", "command" : "second_timer", "args" : [false]},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},
  {"comment" : "FINISH: INIT MERGE TEST"}
]
