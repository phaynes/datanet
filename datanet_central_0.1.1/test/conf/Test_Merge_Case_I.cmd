[
  {"comment" : "START: MERGE TEST I"},
  {"client" : "two", "command" : "delta_reaper", "args" : [false]},
  {"client" : "one", "command" : "second_timer", "args" : [true]},
  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"client" : "two", "command" : "second_timer", "args" : [true]},
  {"client" : "two", "command" : "isolation", "args" : [true]},

  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"file" : "./test/conf/Create_TWO_Document_W_V2.cmd"},

  {"client" : "one", "command" : "second_timer", "args" : [false]},
  {"client" : "two", "command" : "second_timer", "args" : [false]},
  {"client" : "two", "command" : "cond_signal", "args" : ["W", "", "merge_case", "I"]},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},

  {"client" : "one", "command" : "sleep", "args" : [2000]},
  {"client" : "two", "command" : "delta_reaper", "args" : [true]},
  {"comment" : "FINISH: MERGE TEST I"}
]
