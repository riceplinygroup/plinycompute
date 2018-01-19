# create the target
add_pdb_integration_test(Test95)

# add a build dependency to build-tests target
add_dependencies(build-tests Test95)

# add build dependencies to shared libraries it uses
add_dependencies(Test95 OptimizedMethodJoin)
add_dependencies(Test95 ScanStringIntPairSet)
add_dependencies(Test95 StringSelectionOfStringIntPair)
add_dependencies(Test95 StringIntPairMultiSelection)
add_dependencies(Test95 WriteStringSet)