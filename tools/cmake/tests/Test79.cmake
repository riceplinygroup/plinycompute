# add the test
add_test(join ${CTEST_BINARY_DIRECTORY}/Test79 Y Y 1024 master Y)

# add a build dependency to build-tests target
add_dependencies(build-tests Test79)

# add build dependencies to shared libraries it uses
add_dependencies(Test79 SillyJoin)
add_dependencies(Test79 ScanIntSet)
add_dependencies(Test79 ScanStringIntPairSet)
add_dependencies(Test79 ScanStringSet)
add_dependencies(Test79 IntSelectionOfStringIntPair)
add_dependencies(Test79 WriteIntSet)
add_dependencies(Test79 WriteStringSet)
