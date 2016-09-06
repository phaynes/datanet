[
  {"comment" : "START: MERGE TEST H"},
  {"client" : "two", "command" : "cond_signal", "args" : ["W", "x", "=", 1]},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},

  {"client" : "two", "command" : "to_sync_keys", "args" : [false]},
  {"client" : "two", "command" : "isolation", "args" : [true]},
  {"client" : "two", "command" : "cond_signal", "args" : ["W", "", "merge_case", "H"]},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "remove", "args" : ["W"]},
  {"client" : "two", "command" : "sleep", "args" : [2000]},
  {"client" : "two", "command" : "to_sync_keys", "args" : [true]},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},
  {"comment" : "FINISH: MERGE TEST H"}
]
