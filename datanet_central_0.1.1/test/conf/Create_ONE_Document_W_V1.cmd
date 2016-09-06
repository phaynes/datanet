[
  {"comment" : "CREATING W VERSION 1 FROM ONE"},
  {"client" : "one", "command" : "remove", "args" : ["W"]},
  {"client" : "one", "command" : "store", "args" : ["W",
                                          {"_channels":["TEST_CHANNEL"],
                                           "x" : 1
                                          }]},
  {"client" : "one", "command" : "sleep", "args" : [2000]}
]
