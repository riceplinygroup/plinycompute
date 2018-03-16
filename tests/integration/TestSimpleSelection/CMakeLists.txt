# create the target
add_pdb_integration_test(TestSimpleSelection)

# add a build dependency to build-tests target
add_dependencies(build-integration-tests TestSimpleSelection)

# add build dependencies to shared libraries it uses
add_dependencies(TestSimpleSelection SharedEmployee)
add_dependencies(TestSimpleSelection EmployeeSelection)
add_dependencies(TestSimpleSelection ScanEmployeeSet)
add_dependencies(TestSimpleSelection WriteStringSet)