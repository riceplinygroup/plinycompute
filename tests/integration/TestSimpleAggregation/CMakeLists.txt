# create the target
add_pdb_integration_test(TestSimpleAggregation)

# add a build dependency to build-tests target
add_dependencies(build-integration-tests TestSimpleAggregation)

# add build dependencies to shared libraries it uses
add_dependencies(TestSimpleAggregation SimpleSelection)
add_dependencies(TestSimpleAggregation ScanSupervisorSet)
add_dependencies(TestSimpleAggregation SimpleAggregation)