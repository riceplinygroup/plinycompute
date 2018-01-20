# create the target
add_pdb_integration_test(TestGroupByAggregation)

# add a build dependency to build-tests target
add_dependencies(build-tests TestGroupByAggregation)

# add build dependencies to shared libraries it uses
add_dependencies(TestGroupByAggregation ScanSupervisorSet)
add_dependencies(TestGroupByAggregation SimpleGroupBy)