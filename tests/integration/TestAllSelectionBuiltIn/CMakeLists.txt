# create the target
add_pdb_integration_test(TestAllSelectionBuiltIn)

# add a build dependency to build-tests target
add_dependencies(build-integration-tests TestAllSelectionBuiltIn)

# add build dependencies to shared libraries it uses
add_dependencies(TestAllSelectionBuiltIn SharedEmployee)
add_dependencies(TestAllSelectionBuiltIn ScanEmployeeSet)
add_dependencies(TestAllSelectionBuiltIn EmployeeBuiltInIdentitySelection)
add_dependencies(TestAllSelectionBuiltIn WriteBuiltinEmployeeSet)