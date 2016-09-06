[
  {"comment" : "START: MERGE TEST F"},
  {"file" : "./test/conf/Init_Test_Merge_Case.cmd"},

  {"client" : "two", "command" : "cond_signal", "args" : ["W", "", "removed"]},
  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"file" : "./test/conf/Create_TWO_Document_W_V2.cmd"},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "one", "command" : "remove", "args" : ["W"]},
  {"client" : "one", "command" : "cond_signal", "args" : ["W", "", "merge_case", "F"]},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "cond_wait", "args" : ["W"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},
  {"comment" : "FINISH: MERGE TEST F"}
]
