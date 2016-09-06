[
  {"client" : "two", "command" : "cond_signal", "args" : ["X", "n", "+", 99],
                                 "flags" : {"fetch" : true}},
  {"client" : "one", "command" : "cond_signal", "args" : ["X", "n", "+", 99],
                                 "flags" : {"fetch" : true}},
  {"client" : "one", "command" : "incr", "args" : ["X", "n", 11],
                                 "loops" : 5,
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "incr", "args" : ["X", "n", 22],
                                 "loops" : 2,
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "cond_wait", "args" : ["X"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["X"]}
]
