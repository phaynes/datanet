[
  {"comment" : "START: INIT AGENTS"},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "to_sync_keys", "args" : [true]},
  {"client" : "one", "command" : "delta_reaper", "args" : [true]},
  {"client" : "one", "command" : "second_timer", "args" : [false]},

  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "to_sync_keys", "args" : [true]},
  {"client" : "two", "command" : "delta_reaper", "args" : [true]},
  {"client" : "two", "command" : "second_timer", "args" : [false]},
  {"comment" : "FINISH: INIT AGENTS"}
]
