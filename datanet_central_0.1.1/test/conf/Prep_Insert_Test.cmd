[
  {"comment" : "PREP INSERT TEST"},
  {"client" : "one", "command" : "cond_signal", "args" : ["Y", "a", "=", [1,2]]},
  {"client" : "two", "command" : "set", "args" : ["Y", "a", [1,2]],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "cond_wait", "args" : ["Y"]}
]
