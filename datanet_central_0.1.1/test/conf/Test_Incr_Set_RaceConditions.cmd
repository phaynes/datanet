[
  {"client" : "two", "command" : "fetch", "args" : ["X"]},
  {"client" : "one", "command" : "fetch", "args" : ["X"]},



  {"client" : "one", "command" : "cond_signal", "args" : ["X", "n", "=", 1]},
  {"client" : "two", "command" : "cond_signal", "args" : ["X", "n", "=", 1]},
  {"client" : "two", "command" : "set", "args" : ["X", "n", 1],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "cond_wait", "args" : ["X"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["X"]},



  {"comment" : "TEST: Set + Incr Race Conditions"},
  {"client" : "two", "command" : "isolation", "args" : [true]},
  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"client" : "two", "command" : "cond_signal", "args" : ["X", "n", "=", 8]},
  {"client" : "one", "command" : "cond_signal", "args" : ["X", "n", "=", 8]},
  {"client" : "one", "command" : "set", "args" : ["X", "n", 11],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "set", "args" : ["X", "n", 4],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "incr", "args" : ["X", "n", 4],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "incr", "args" : ["X", "n", 100],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "cond_wait", "args" : ["X"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["X"]}

]
