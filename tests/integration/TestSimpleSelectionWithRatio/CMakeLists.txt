# create the target
add_pdb_integration_test(TestSimpleSelectionWithRatio)

# add a build dependency to build-tests target
add_dependencies(build-tests TestSimpleSelectionWithRatio)

# add build dependencies to shared libraries it uses
add_dependencies(TestSimpleSelectionWithRatio SharedEmployee)
add_dependencies(TestSimpleSelectionWithRatio EmployeeSelection)
add_dependencies(TestSimpleSelectionWithRatio ScanEmployeeSet)
add_dependencies(TestSimpleSelectionWithRatio WriteStringSet)