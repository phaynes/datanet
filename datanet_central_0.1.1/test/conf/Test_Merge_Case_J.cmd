[
  {"comment" : "START: MERGE TEST J"},
  {"file" : "./test/conf/Second_Timer_Init_Test_Merge_Case.cmd"},

  {"client" : "two", "command" : "cond_signal", "args" : ["W", "", "removed"]},
  {"client" : "one", "command" : "remove", "args" : ["W"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},


  {"client" : "two", "command" : "delta_reaper", "args" : [false]},
  {"client" : "one", "command" : "second_timer", "args" : [true]},
  {"client" : "two", "command" : "second_timer", "args" : [true]},

  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"client" : "two", "command" : "isolation", "args" : [true]},
  {"client" : "three", "command" : "isolation", "args" : [true]},

  {"file" : "./test/conf/Create_TWO_Document_W_V2.cmd"},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},

  {"client" : "one", "command" : "second_timer", "args" : [false]},
  {"client" : "two", "command" : "second_timer", "args" : [false]},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "three", "command" : "cond_signal", "args" : ["W", "", "merge_case", "J"]},
  {"client" : "three", "command" : "isolation", "args" : [false]},
  {"client" : "three", "command" : "cond_wait", "args" : ["W"]},

  {"client" : "two", "command" : "delta_reaper", "args" : [true]},
  {"comment" : "FINISH: MERGE TEST J"}
]
