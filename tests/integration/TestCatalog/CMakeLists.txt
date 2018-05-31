# create the target
add_pdb_integration_test(TestCatalog)

# add a build dependency to build-tests target
add_dependencies(build-integration-tests TestCatalog)

# add build dependencies to shared libraries it uses
add_dependencies(TestCatalog DoubleVectorAggregation)
add_dependencies(TestCatalog SimpleJoin)
add_dependencies(TestCatalog ScanIntSet)
add_dependencies(TestCatalog ScanStringIntPairSet)
add_dependencies(TestCatalog ScanStringSet)
add_dependencies(TestCatalog WriteStringSet)
