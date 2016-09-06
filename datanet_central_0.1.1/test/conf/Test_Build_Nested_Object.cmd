[
  {"client" : "two", "command" : "fetch", "args" : ["Y"]},
  {"client" : "one", "command" : "fetch", "args" : ["Y"]},




  {"client" : "two", "command" : "cond_signal", "args" : ["Y", "o", "=", {}]},
  {"client" : "one", "command" : "set", "args" : ["Y", "o", {}],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "cond_wait", "args" : ["Y"]},




  {"client" : "one", "command" : "cond_signal", "args" :
                                                ["Y", "o", "=", {"first":{}}]},
  {"client" : "two", "command" : "set", "args" : ["Y", "o.first", {}],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "cond_wait", "args" : ["Y"]},




  {"client" : "two", "command" : "cond_signal", "args" :
                            ["Y", "o.first.second", "=", {"x":1,"y":2,"z":3}]},
  {"client" : "one", "command" : "set", "args" :
                            ["Y", "o.first.second", {"x":1,"y":2,"z":3}],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "cond_wait", "args" : ["Y"]},




  {"client" : "one", "command" : "cond_signal", "args" :
                                        ["Y", "o.first.second", "=", {"z":3}]},
  {"client" : "two", "command" : "cond_signal", "args" :
                                        ["Y", "o.first.second", "=", {"z":3}]},
  {"client" : "two", "command" : "delete", "args" : ["Y", "o.first.second.y"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "delete", "args" : ["Y", "o.first.second.x"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "cond_wait", "args" : ["Y"]},
  {"client" : "one", "command" : "cond_wait", "args" : ["Y"]},



  {"client" : "one", "command" : "cond_signal", "args" : ["Y", "o", "=", {}]},
  {"client" : "two", "command" : "cond_signal", "args" : ["Y", "o", "=", {}]},
  {"client" : "two", "command" : "incr", "args" : ["Y", "o.first.second.z", 1],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "delete", "args" : ["Y", "o.first"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "cond_wait", "args" : ["Y"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["Y"]}
]
