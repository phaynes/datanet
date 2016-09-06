[
  {"client" : "one", "command" : "store", "args" : ["SET_TEST",
                                          {"_channels":["TEST_CHANNEL"]
                                          }]},


  {"client" : "one", "command" : "set", "args" : ["SET_TEST", "s", "ABC"],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "set", "args" : ["SET_TEST", "n", 1],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "set", "args" : ["SET_TEST", "o", {"x":1}],
                                 "flags" : {"fetch" : true, "commit" : true}},


  {"client" : "one", "command" : "set", "args" : ["SET_TEST", 4, 0],
                                 "error" : "-ERROR: KEY must be a STRING",
                                 "flags" : {"fetch" : true, "commit" : true}},

  {"client" : "one", "command" : "set", "args" : ["SET_TEST", "_ID", 0],
     "error" :
       "-ERROR: Changing reserved fields (_id, _meta,_channels) prohibited",
                                 "flags" : {"fetch" : true, "commit" : true}},


  {"client" : "one", "command" : "set", "args" : ["SET_TEST", "a", [1,2,3]],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "set", "args" : ["SET_TEST", "a.x", 0],
      "error" : "-ERROR: SET() on an array invalid format, use SET(num, val)",
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "insert", "args" : ["SET_TEST", "a", "x", 0],
      "error" : "-ERROR: USAGE: insert(key, index, value) - \"index\" must be a positive integer", "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "insert", "args" : ["SET_TEST", "a", -1, 0],
      "error" : "-ERROR: USAGE: insert(key, index, value) - \"index\" must be a positive integer", "flags" : {"fetch" : true, "commit" : true}},


  {"client" : "one", "command" : "set", "args" : ["SET_TEST", "s.1", 0],
    "error" : "-ERROR: Treating primitive (number,string) as nestable(object,array)", "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "insert", "args" : ["SET_TEST", "s", 1, 1],
      "error_prefix" : "-ERROR: INSERT is an array command, does not work on type: ", "flags" : {"fetch" : true, "commit" : true}},

  {"client" : "one", "command" : "incr", "args" : ["SET_TEST", "c", 1],
                                 "error" : "-ERROR: Field not Found",
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "incr", "args" : ["SET_TEST", "s", 1],
                            "error" : "-ERROR: INCR only supported on Numbers",
                            "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "incr", "args" : ["SET_TEST", "n", "XYZ"],
                            "error" : "-ERROR: INCR field by a NaN",
                            "flags" : {"fetch" : true, "commit" : true}},


  {"client" : "one", "command" : "delete", "args" : ["SET_TEST", "q"],
                                 "error" : "-ERROR: Field not Found",
                                 "flags" : {"fetch" : true, "commit" : true}},



  {"client" : "one", "command" : "set", "args" : ["SET_TEST", "o.x", 4],
                            "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "set", "args" : ["SET_TEST", "o.a.b", 1],
          "error_prefix" : "-ERROR: Nested field does not exist, field_name: ",
          "flags" : {"fetch" : true, "commit" : true}}
]
