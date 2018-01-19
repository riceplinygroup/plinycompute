# create the target
add_pdb_integration_test(Test69)

# add a build dependency to build-tests target
add_dependencies(build-tests Test69)

# add build dependencies to shared libraries it uses
add_dependencies(Test69 SharedEmployee)
add_dependencies(Test69 EmployeeSelection)
add_dependencies(Test69 ScanEmployeeSet)
add_dependencies(Test69 WriteStringSet)