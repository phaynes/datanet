[
  {"client" : "two", "command" : "fetch", "args" : ["X"]},
  {"client" : "one", "command" : "fetch", "args" : ["X"]},



  {"comment" : "TEST: Concurrent Sets"},
  {"client" : "two", "command" : "cond_signal", "args" :
                                             ["X", "s", "=", "FROM_ONE"]},
  {"client" : "one", "command" : "cond_signal", "args" :
                                             ["X", "s", "=", "FROM_ONE"]},
  {"client" : "two", "command" : "set", "args" : ["X", "s", "FROM_TWO"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "set", "args" : ["X", "s", "FROM_ONE"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "cond_wait", "args" : ["X"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["X"]},



  {"client" : "two", "command" : "cond_signal", "args" : ["X", "s", "deleted"]},
  {"client" : "one", "command" : "cond_signal", "args" : ["X", "s", "deleted"]},
  {"client" : "one", "command" : "delete", "args" : ["X", "s"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "cond_wait", "args" : ["X"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["X"]},



  {"comment" : "TEST: Concurrent Adds"},
  {"client" : "two", "command" : "isolation", "args" : [true]},
  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"client" : "two", "command" : "cond_signal", "args" :
                                             ["X", "s", "=", "1.2"]},
  {"client" : "one", "command" : "cond_signal", "args" :
                                             ["X", "s", "=", "1.2"]},
  {"client" : "one", "command" : "set", "args" : ["X", "s", "1.1"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "set", "args" : ["X", "s", "2.1"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "set", "args" : ["X", "s", "1.2"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "cond_wait", "args" : ["X"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["X"]}

]
