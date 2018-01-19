# create the target
add_pdb_integration_test(Test93)

# add a build dependency to build-tests target
add_dependencies(build-tests Test93)

# add build dependencies to shared libraries it uses
add_dependencies(Test93 OptimizedMethodJoin)
add_dependencies(Test93 ScanStringIntPairSet)
add_dependencies(Test93 StringSelectionOfStringIntPair)
add_dependencies(Test93 WriteStringSet)