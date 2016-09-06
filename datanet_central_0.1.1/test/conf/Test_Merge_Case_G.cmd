[
  {"comment" : "START: MERGE TEST G"},
  {"file" : "./test/conf/Init_Test_Merge_Case.cmd"},

  {"client" : "one", "command" : "cond_signal", "args" : ["W", "", "merge_case", "G"]},
  {"client" : "one", "command" : "delta_reaper", "args" : [false]},
  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"file" : "./test/conf/Create_TWO_Document_W_V2.cmd"},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "cond_wait", "args" : ["W"]},

  {"client" : "one", "command" : "delta_reaper", "args" : [true]},
  {"comment" : "FINISH: MERGE TEST G"}
]
