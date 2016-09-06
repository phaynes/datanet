[
  {"comment" : "START: MERGE TEST B"},
  {"file" : "./test/conf/Init_Test_Merge_Case.cmd"},

  {"client" : "one", "command" : "delta_reaper", "args" : [false]},
  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"client" : "two", "command" : "isolation", "args" : [true]},

  {"file" : "./test/conf/Create_TWO_Document_W_V2.cmd"},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "one", "command" : "remove", "args" : ["W"]},

  {"client" : "three", "command" : "cond_signal", "args" : ["W", "", "removed"]},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "sleep", "args" : [2000]},
  {"client" : "one", "command" : "cond_signal", "args" : ["W", "", "merge_case", "B"]},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "cond_wait", "args" : ["W"]},

  {"client" : "one", "command" : "sleep", "args" : [2000]},
  {"client" : "one", "command" : "delta_reaper", "args" : [true]},
  {"client" : "three", "command" : "cond_wait", "args" : ["W"]},
  {"comment" : "FINISH: MERGE TEST B"}
]
