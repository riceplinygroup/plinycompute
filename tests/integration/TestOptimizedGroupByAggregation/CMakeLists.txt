# create the target
add_pdb_integration_test(TestOptimizedGroupByAggregation)

# add a build dependency to build-tests target
add_dependencies(build-tests TestOptimizedGroupByAggregation)

# add build dependencies to shared libraries it uses
add_dependencies(TestOptimizedGroupByAggregation ScanOptimizedSupervisorSet)
add_dependencies(TestOptimizedGroupByAggregation OptimizedEmployeeGroupBy)