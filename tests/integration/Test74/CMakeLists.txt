# create the target
add_pdb_integration_test(Test74)

# add a build dependency to build-tests target
add_dependencies(build-tests Test74)

# add build dependencies to shared libraries it uses
add_dependencies(Test74 SimpleSelection)
add_dependencies(Test74 ScanSupervisorSet)
add_dependencies(Test74 SimpleAggregation)
add_dependencies(Test74 FinalSelection)
add_dependencies(Test74 WriteDoubleSet)