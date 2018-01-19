# create the target
add_pdb_integration_test(Test84)

# add a build dependency to build-tests target
add_dependencies(build-tests Test84)

# add build dependencies to shared libraries it uses
add_dependencies(Test84 WriteBuiltinEmployeeSet)
add_dependencies(Test84 ScanSupervisorSet)
add_dependencies(Test84 SupervisorMultiSelection)