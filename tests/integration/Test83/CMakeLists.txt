# create the target
add_pdb_integration_test(Test83)

# add a build dependency to build-tests target
add_dependencies(build-tests Test83)

# add build dependencies to shared libraries it uses
add_dependencies(Test83 DoubleVectorAggregation)