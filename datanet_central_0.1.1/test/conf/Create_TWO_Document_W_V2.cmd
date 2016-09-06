[
  {"comment" : "CREATING W VERSION 2 FROM TWO"},
  {"client" : "two", "command" : "remove", "args" : ["W"]},
  {"client" : "two", "command" : "store", "args" : ["W",
                                          {"_channels":["TEST_CHANNEL"],
                                           "x" : 2
                                          }]},
  {"client" : "two", "command" : "sleep", "args" : [2000]}
]
