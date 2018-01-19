# create the target
add_pdb_integration_test(Test90)

# add a build dependency to build-tests target
add_dependencies(build-tests Test90)

# add build dependencies to shared libraries it uses
add_dependencies(Test90 ScanOptimizedSupervisorSet)
add_dependencies(Test90 OptimizedEmployeeGroupBy)