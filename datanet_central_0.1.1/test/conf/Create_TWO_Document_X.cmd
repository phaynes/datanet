[
  {"file" : "./test/conf/Switch_TWO_tester_production_TEST.cmd"},
  {"client" : "two", "command" : "remove", "args" : ["X"]},
  {"client" : "two", "command" : "store",  "args" : ["X",
                                           {"_channels":["TEST_CHANNEL"],
                                            "s" : "hello",
                                            "n" : 44,
                                            "o" : {"x" : 1, "y" : 2, "z" : 3},
                                            "a" : [1,2,3,4]
                                           }]},
  {"client" : "two", "command" : "sleep", "args" : [2000]}
]
