# create the target
add_pdb_integration_test(TestDuplicate)

# add a build dependency to build-tests target
add_dependencies(build-tests TestDuplicate)
