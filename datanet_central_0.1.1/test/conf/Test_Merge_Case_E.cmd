[
  {"comment" : "START: MERGE TEST E"},
  {"file" : "./test/conf/Second_Timer_Init_Test_Merge_Case.cmd"},

  {"client" : "one", "command" : "second_timer", "args" : [true]},
  {"client" : "two", "command" : "second_timer", "args" : [true]},

  {"client" : "two", "command" : "cond_signal", "args" : ["W", "x", "=", 1]},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},


  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"file" : "./test/conf/Create_TWO_Document_W_V2.cmd"},
  {"client" : "one", "command" : "remove", "args" : ["W"]},
  {"client" : "one", "command" : "cond_signal", "args" : ["W", "", "merge_case", "E"]},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "cond_wait", "args" : ["W"]},


  {"client" : "one", "command" : "second_timer", "args" : [false]},
  {"client" : "two", "command" : "second_timer", "args" : [false]},
  {"comment" : "FINISH: MERGE TEST E"}
]
