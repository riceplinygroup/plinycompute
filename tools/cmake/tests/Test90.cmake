# add the test
add_test(selection-and-join-mixed ${CTEST_BINARY_DIRECTORY}/Test90 Y Y 1024 master Y)

# add a build dependency to build-tests target
add_dependencies(build-tests Test90)

# add build dependencies to shared libraries it uses
add_dependencies(Test90 ScanOptimizedSupervisorSet)
add_dependencies(Test90 OptimizedEmployeeGroupBy)