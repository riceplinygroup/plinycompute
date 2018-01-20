# create the target
add_pdb_integration_test(TestAllSelectionShared)

# add a build dependency to build-tests target
add_dependencies(build-tests TestAllSelectionShared)

# add build dependencies to shared libraries it uses
add_dependencies(TestAllSelectionShared SharedEmployee)
add_dependencies(TestAllSelectionShared ScanEmployeeSet)
add_dependencies(TestAllSelectionShared EmployeeIdentitySelection)
add_dependencies(TestAllSelectionShared WriteEmployeeSet)