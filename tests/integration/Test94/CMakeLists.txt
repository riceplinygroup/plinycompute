# create the target
add_pdb_integration_test(Test94)

# add a build dependency to build-tests target
add_dependencies(build-tests Test94)

# add build dependencies to shared libraries it uses
add_dependencies(Test94 OptimizedMethodJoin)
add_dependencies(Test94 ScanStringIntPairSet)
add_dependencies(Test94 StringSelectionOfStringIntPair)
add_dependencies(Test94 WriteStringSet)