# create the target
add_pdb_integration_test(Test66)

# add a build dependency to build-tests target
add_dependencies(build-tests Test66)

# add build dependencies to shared libraries it uses
add_dependencies(Test66 SharedEmployee)
add_dependencies(Test66 EmployeeSelection)
add_dependencies(Test66 ScanEmployeeSet)
add_dependencies(Test66 WriteStringSet)