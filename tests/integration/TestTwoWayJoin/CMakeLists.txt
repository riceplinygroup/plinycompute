# create the target
add_pdb_integration_test(TestTwoWayJoin)

# add a build dependency to build-tests target
add_dependencies(build-integration-tests TestTwoWayJoin)

# add build dependencies to shared libraries it uses
add_dependencies(TestTwoWayJoin CartesianJoin)
add_dependencies(TestTwoWayJoin ScanIntSet)
add_dependencies(TestTwoWayJoin ScanStringSet)
add_dependencies(TestTwoWayJoin WriteStringIntPairSet)