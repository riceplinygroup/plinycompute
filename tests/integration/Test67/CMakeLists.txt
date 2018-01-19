# create the target
add_pdb_integration_test(Test67)

# add a build dependency to build-tests target
add_dependencies(build-tests Test67)

# add build dependencies to shared libraries it uses
add_dependencies(Test67 SimpleSelection)
add_dependencies(Test67 ScanSupervisorSet)
add_dependencies(Test67 SimpleAggregation)