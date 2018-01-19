# create the target
add_pdb_integration_test(Test79)

# add a build dependency to build-tests target
add_dependencies(build-tests Test79)

# add build dependencies to shared libraries it uses
add_dependencies(Test79 SimpleJoin)
add_dependencies(Test79 ScanIntSet)
add_dependencies(Test79 ScanStringIntPairSet)
add_dependencies(Test79 ScanStringSet)
add_dependencies(Test79 IntSelectionOfStringIntPair)
add_dependencies(Test79 WriteIntSet)
add_dependencies(Test79 WriteStringSet)
