# create the target
add_pdb_integration_test(TestIterativeAggregation)

# add a build dependency to build-tests target
add_dependencies(build-integration-tests TestIterativeAggregation)

# add build dependencies to shared libraries it uses
add_dependencies(TestIterativeAggregation ScanSupervisorSet)
add_dependencies(TestIterativeAggregation SimpleGroupBy)