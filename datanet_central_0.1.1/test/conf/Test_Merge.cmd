{
  "agents" : [
    { "file" : "test/conf/AGENT_NODE_ONE.cfg"   },
    { "file" : "test/conf/AGENT_NODE_TWO.cfg"   },
    { "file" : "test/conf/AGENT_NODE_THREE.cfg" }

  ],
  "actions" : [
    {"file" : "./test/conf/Set_Notify_ONE.cmd"},
    {"file" : "./test/conf/Switch_ONE_tester_production_TEST.cmd"},

    {"file" : "./test/conf/Set_Notify_TWO.cmd"},
    {"file" : "./test/conf/Switch_TWO_tester_production_TEST.cmd"},

    {"file" : "./test/conf/Set_Notify_THREE.cmd"},
    {"file" : "./test/conf/Switch_THREE_tester_production_TEST.cmd"},

    {"comment" : "START: MERGE TESTS"},

    {"file" : "./test/conf/Init_Agents.cmd"},

    {"file" : "./test/conf/Test_Merge_Case_A_L.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_B.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_C_M.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_D.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_E.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_F.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_G.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_H.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_I.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_J.cmd"},
    {"file" : "./test/conf/Test_Merge_Case_K.cmd"},

    {"comment" : "FINISH: MERGE TESTS "}
  ]
}
