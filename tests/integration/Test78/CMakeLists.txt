# create the target
add_pdb_integration_test(Test78)

# add a build dependency to build-tests target
add_dependencies(build-tests Test78)

# add build dependencies to shared libraries it uses
add_dependencies(Test78 IntSimpleJoin)
add_dependencies(Test78 ScanIntSet)
add_dependencies(Test78 ScanStringIntPairSet)
add_dependencies(Test78 StringSelectionOfStringIntPair)
add_dependencies(Test78 IntAggregation)
add_dependencies(Test78 WriteSumResultSet)