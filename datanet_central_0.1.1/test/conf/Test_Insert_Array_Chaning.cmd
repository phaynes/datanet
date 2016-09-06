[
  {"client" : "two", "command" : "fetch", "args" : ["Y"]},
  {"client" : "one", "command" : "fetch", "args" : ["Y"]},


  {"comment" : "DO FIRST INSERT TEST"},
  {"file" : "./test/conf/Prep_Insert_Test.cmd"},
  {"client" : "two", "command" : "cond_signal", "args" :
                                              ["Y", "a", "=", [111,1,2,222]]},
  {"client" : "one", "command" : "insert", "args" : ["Y", "a", 0, 111],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "one", "command" : "insert", "args" : ["Y", "a", 3, 222],
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"client" : "two", "command" : "cond_wait", "args" : ["Y"]},



  {"comment" : "DO SECOND INSERT TEST - DEISOLATE TWO->ONE"},
  {"file" : "./test/conf/Prep_Insert_Test.cmd"},
  {"file" : "./test/conf/Insert_Test2_Signal.cmd"},
  {"file" : "./test/conf/Insert_Test2_CRUD_ONE.cmd"},
  {"file" : "./test/conf/Insert_Test2_CRUD_TWO.cmd"},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "cond_wait", "args" : ["Y"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["Y"]},



  {"comment" : "DO THIRD INSERT TEST - DEISOLATE ONE->TWO"},
  {"file" : "./test/conf/Prep_Insert_Test.cmd"},
  {"file" : "./test/conf/Insert_Test2_Signal.cmd"},
  {"file" : "./test/conf/Insert_Test2_CRUD_ONE.cmd"},
  {"file" : "./test/conf/Insert_Test2_CRUD_TWO.cmd"},
  {"client" : "one", "command" : "isolation", "args" : [false]},
  {"client" : "two", "command" : "isolation", "args" : [false]},
  {"client" : "one", "command" : "cond_wait", "args" : ["Y"]},
  {"client" : "two", "command" : "cond_wait", "args" : ["Y"]}

]
