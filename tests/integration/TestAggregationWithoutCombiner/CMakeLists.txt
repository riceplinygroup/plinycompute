# create the target
add_pdb_integration_test(TestAggregationWithoutCombiner)

# add a build dependency to build-tests target
add_dependencies(build-tests TestAggregationWithoutCombiner)

# add build dependencies to shared libraries it uses
add_dependencies(TestAggregationWithoutCombiner ScanSupervisorSet)
add_dependencies(TestAggregationWithoutCombiner EmployeeGroupBy)