[
  {"client" : "two", "command" : "isolation", "args" : [true]},
  {"client" : "two", "command" : "insert", "args" : ["Y", "a", 1, 201],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "insert", "args" : ["Y", "a", 2, 202],
                                 "flags" : {"fetch" : true, "commit" : true}}
]
