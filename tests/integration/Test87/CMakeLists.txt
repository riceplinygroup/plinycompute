# create the target
add_pdb_integration_test(Test87)

# add a build dependency to build-tests target
add_dependencies(build-tests Test87)

# add build dependencies to shared libraries it uses
add_dependencies(Test87 ScanSupervisorSet)
add_dependencies(Test87 SimpleGroupBy)