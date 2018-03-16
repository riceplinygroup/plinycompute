# create the target
add_pdb_integration_test(TestTopK)

# add a build dependency to build-tests target
add_dependencies(build-integration-tests TestTopK)

# add build dependencies to shared libraries it uses
add_dependencies(TestTopK EmpWithVector)
add_dependencies(TestTopK TopKTest)
add_dependencies(TestTopK ScanEmpWithVector)
add_dependencies(TestTopK WriteEmpWithVector)