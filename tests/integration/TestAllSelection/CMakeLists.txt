# create the target
add_pdb_integration_test(TestAllSelection)

# add a build dependency to build-tests target
add_dependencies(build-tests TestAllSelection)

# add build dependencies to shared libraries it uses
add_dependencies(TestAllSelection AllSelectionWithCreation)
add_dependencies(TestAllSelection ScanBuiltinEmployeeSet)
add_dependencies(TestAllSelection WriteBuiltinEmployeeSet)