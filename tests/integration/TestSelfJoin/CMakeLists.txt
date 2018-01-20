# create the target
add_pdb_integration_test(SelfJoin)

# add a build dependency to build-tests target
add_dependencies(build-tests SelfJoin)

# add build dependencies to shared libraries it uses
add_dependencies(SelfJoin OptimizedMethodJoin)
add_dependencies(SelfJoin ScanStringIntPairSet)
add_dependencies(SelfJoin StringSelectionOfStringIntPair)
add_dependencies(SelfJoin WriteStringSet)