[
  {"file" : "./test/conf/Create_ONE_Document_Z.cmd"},

                                 

  {"client" : "one",   "command" : "cond_signal", "args" :
                                     ["Z", "events", "shift", "one.${NUM}",
                                      "validate_capped_list"],
                                   "flags" : {"fetch" : true}},
  {"client" : "two",   "command" : "cond_signal", "args" :
                                     ["Z", "events", "shift", "two.${NUM}",
                                      "validate_capped_list"],
                                   "flags" : {"fetch" : true}},
  {"client" : "three", "command" : "cond_signal", "args" :
                                     ["Z", "events", "shift", "three.${NUM}",
                                      "validate_capped_list"],
                                   "flags" : {"fetch" : true}},



  {"thread" : "tid_one_CL",
   "client" : "one", "command" : "insert", "args" : 
                                   ["Z", "events", 0, "${name}.${next}"],
                                 "loops" : ${NUM},
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"thread" : "tid_one_CL",
   "client" : "one", "command" : "cond_wait", "args" : ["Z"]},



  {"thread" : "tid_two_CL",
   "client" : "two", "command" : "insert", "args" :
                                   ["Z", "events", 0, "${name}.${next}"],
                                 "loops" : ${NUM},
                                 "flags" : {"fetch" : true, "commit" : true}},
  {"thread" : "tid_two_CL",
   "client" : "two", "command" : "cond_wait", "args" : ["Z"]},



  {"thread" : "tid_three_CL",
   "client" : "three", "command" : "insert", "args" :
                                     ["Z", "events", 0, "${name}.${next}"],
                                   "loops" : ${NUM},
                                   "flags" : {"fetch" : true, "commit" : true}},
  {"thread" : "tid_three_CL",
   "client" : "three", "command" : "cond_wait", "args" : ["Z"]},



  {"command" : "wait", "args" : []},
  {"comment" : "MAIN: (Test_Capped_List) COMMENT #4"}

]
