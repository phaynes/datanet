[
                                 
  {"thread" : "tid_one_incr",
   "client" : "one", "command" : "cond_signal", "args" : ["X", "n", "+", 88],
                                 "flags" : {"fetch" : true}},
  {"thread" : "tid_one_incr",
   "client" : "one", "command" : "incr", "args" : ["X", "n", 11],
                                 "loops" : 4, "sleep" : 1000,
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"thread" : "tid_one_incr",
   "client" : "one", "command" : "cond_wait", "args" : ["X"]},


  {"comment" : "MAIN: COMMENT ONE"},


  {"thread" : "tid_two_incr",
   "client" : "two", "command" : "cond_signal", "args" : ["X", "n", "+", 88],
                                 "flags" : {"fetch" : true}},
  {"thread" : "tid_two_incr",
   "client" : "two", "command" : "incr", "args" : ["X", "n", 11],
                                 "loops" : 4, "sleep" : 1000,
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"thread" : "tid_two_incr",
   "client" : "two", "command" : "cond_wait", "args" : ["X"]},

  {"comment" : "MAIN: COMMENT TWO"},


  {"command" : "wait", "args" : []},
  {"comment" : "MAIN: COMMENT THREE"}

]
