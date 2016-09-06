[
  {"comment" : "START: MERGE TEST K"},
  {"client" : "two", "command" : "cond_signal", "args" : ["W", "x", "=", 1]},
  {"file" : "./test/conf/Create_ONE_Document_W_V1.cmd"},
  {"client" : "two", "command" : "cond_wait", "args" : ["W"]},

  {"client" : "one", "command" : "to_sync_keys", "args" : [false]},
  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"client" : "two", "command" : "incr", "args" : ["W", "x", 1],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "cond_signal", "args" : ["W", "", "merge_case", "K"]},
  {"client" : "one", "command" : "isolation", "args" : [false]},

  {"client" : "three", "command" : "remove", "args" : ["W"]},
  {"client" : "one", "command" : "sleep", "args" : [2000]},
  {"client" : "one", "command" : "to_sync_keys", "args" : [true]},
  {"client" : "one", "command" : "cond_wait", "args" : ["W"]},
  {"comment" : "FINISH: MERGE TEST K"}
]
