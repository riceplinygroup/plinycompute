# create the target
add_pdb_integration_test(Test97)

# add a build dependency to build-tests target
add_dependencies(build-tests Test97)

# add build dependencies to shared libraries it uses
add_dependencies(Test97 OptimizedMethodJoin)
add_dependencies(Test97 ScanStringIntPairSet)
add_dependencies(Test97 StringSelectionOfStringIntPair)
add_dependencies(Test97 WriteStringSet)