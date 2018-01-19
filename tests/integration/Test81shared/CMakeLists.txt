# create the target
add_pdb_integration_test(Test81shared)

# add a build dependency to build-tests target
add_dependencies(build-tests Test81shared)

# add build dependencies to shared libraries it uses
add_dependencies(Test81shared SharedEmployee)
add_dependencies(Test81shared ScanEmployeeSet)
add_dependencies(Test81shared EmployeeIdentitySelection)
add_dependencies(Test81shared WriteEmployeeSet)