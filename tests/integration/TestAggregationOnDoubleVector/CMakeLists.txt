# create the target
add_pdb_integration_test(TestAggregationOnDoubleVector)

# add a build dependency to build-tests target
add_dependencies(build-integration-tests TestAggregationOnDoubleVector)

# add build dependencies to shared libraries it uses
add_dependencies(TestAggregationOnDoubleVector DoubleVectorAggregation)