# create the target
add_pdb_integration_test(Test88)

# add a build dependency to build-tests target
add_dependencies(build-tests Test88)

# add build dependencies to shared libraries it uses
add_dependencies(Test88 ScanSupervisorSet)
add_dependencies(Test88 EmployeeGroupBy)