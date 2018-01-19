# create the target
add_pdb_integration_test(Test77)

# add a build dependency to build-tests target
add_dependencies(build-tests Test77)

# add build dependencies to shared libraries it uses
add_dependencies(Test77 CartesianJoin)
add_dependencies(Test77 ScanIntSet)
add_dependencies(Test77 ScanStringSet)
add_dependencies(Test77 WriteStringIntPairSet)