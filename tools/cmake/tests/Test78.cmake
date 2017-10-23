# add the test
add_test(selection-and-join-mixed ${CTEST_BINARY_DIRECTORY}/Test78 Y Y 1024 master Y)

# add a build dependency to build-tests target
add_dependencies(build-tests Test78)

# add build dependencies to shared libraries it uses
add_dependencies(Test78 IntSillyJoin)
add_dependencies(Test78 ScanIntSet)
add_dependencies(Test78 ScanStringIntPairSet)
add_dependencies(Test78 StringSelectionOfStringIntPair)
add_dependencies(Test78 IntAggregation)
add_dependencies(Test78 WriteSumResultSet)