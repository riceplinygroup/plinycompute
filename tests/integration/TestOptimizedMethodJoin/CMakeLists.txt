# create the target
add_pdb_integration_test(TestOptimizedMethodJoin)

# add a build dependency to build-tests target
add_dependencies(build-integration-tests TestOptimizedMethodJoin)

# add build dependencies to shared libraries it uses
add_dependencies(TestOptimizedMethodJoin OptimizedMethodJoin)
add_dependencies(TestOptimizedMethodJoin ScanStringIntPairSet)
add_dependencies(TestOptimizedMethodJoin StringSelectionOfStringIntPair)
add_dependencies(TestOptimizedMethodJoin WriteStringSet)