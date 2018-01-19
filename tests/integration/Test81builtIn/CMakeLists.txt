# create the target
add_pdb_integration_test(Test81builtIn)

# add a build dependency to build-tests target
add_dependencies(build-tests Test81builtIn)

# add build dependencies to shared libraries it uses
add_dependencies(Test81builtIn SharedEmployee)
add_dependencies(Test81builtIn ScanEmployeeSet)
add_dependencies(Test81builtIn EmployeeBuiltInIdentitySelection)
add_dependencies(Test81builtIn WriteBuiltinEmployeeSet)