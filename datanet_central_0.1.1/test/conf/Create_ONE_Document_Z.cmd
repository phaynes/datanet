[
  {"file" : "./test/conf/Switch_ONE_tester_production_TEST.cmd"},
  {"client" : "one", "command" : "remove", "args" : ["Z"]},
  {"client" : "one", "command" : "store",  "args" : ["Z",
                              {"_channels" : ["TEST_CHANNEL"],
                              "events" : { "_type"     : "LIST",
                                           "_metadata" : {"MAX-SIZE" : 20,
                                                          "TRIM"     : 5},
                                           "_data" : []
                                         }
                              }]},
  {"client" : "one", "command" : "sleep", "args" : [2000]}
]
