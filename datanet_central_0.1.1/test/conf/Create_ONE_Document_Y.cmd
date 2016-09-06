[
  {"file" : "./test/conf/Switch_ONE_tester_production_TEST.cmd"},
  {"client" : "one", "command" : "remove", "args" : ["Y"]},
  {"client" : "one", "command" : "store",  "args" : ["Y",
                                           {"_channels":["TEST_CHANNEL"],
                                            "s" : "string",
                                            "n" : 12345,
                                            "a" : [77,88,99]
                                           }]},
  {"client" : "one", "command" : "sleep", "args" : [2000]}
]
