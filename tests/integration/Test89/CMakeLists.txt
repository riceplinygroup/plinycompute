# create the target
add_pdb_integration_test(Test89)

# add a build dependency to build-tests target
add_dependencies(build-tests Test89)

# add build dependencies to shared libraries it uses
add_dependencies(Test89 ScanSupervisorSet)
add_dependencies(Test89 SimpleGroupBy)