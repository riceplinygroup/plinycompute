# add the test
add_test(aggregation-and-selection-mixed ${CTEST_BINARY_DIRECTORY}/Test74 Y Y 1024 master Y)

# add a build dependency to build-tests target
add_dependencies(build-tests Test74)

# add build dependencies to shared libraries it uses
add_dependencies(Test74 SimpleSelection)
add_dependencies(Test74 ScanSupervisorSet)
add_dependencies(Test74 SimpleAggregation)
add_dependencies(Test74 FinalSelection)
add_dependencies(Test74 WriteDoubleSet)
