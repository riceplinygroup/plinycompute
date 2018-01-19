# create the target
add_pdb_integration_test(Test80)

# add a build dependency to build-tests target
add_dependencies(build-tests Test80)

# add build dependencies to shared libraries it uses
add_dependencies(Test80 AllSelectionWithCreation)
add_dependencies(Test80 ScanBuiltinEmployeeSet)
add_dependencies(Test80 WriteBuiltinEmployeeSet)