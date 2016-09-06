[
  {"client" : "one", "command" : "isolation", "args" : [true]},
  {"client" : "one", "command" : "insert", "args" : ["Y", "a", 1, 101],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "insert", "args" : ["Y", "a", 2, 102],
                                 "flags" : {"fetch" : true, "commit" : true}}
]
