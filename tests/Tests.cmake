# enable tests
enable_testing()

# add the costum target for building tests
add_custom_target(build-tests)

# adds one pdb integration test to CMAKE
function(add_pdb_integration_test test-name)
    # get the directory
    get_filename_component(test-path ${CMAKE_CURRENT_LIST_FILE} DIRECTORY)

    # create the target
    add_executable(${test-name} "${test-path}/${test-name}.cc"
            $<TARGET_OBJECTS:logical-plan-parser>
            $<TARGET_OBJECTS:linear-algebra-parser>
            $<TARGET_OBJECTS:pdb-client>
            $<TARGET_OBJECTS:linear-algebra-parser>)

    # link it to the required libraries
    target_link_libraries(${test-name} pdb-tests-common)
    target_link_libraries(${test-name} ${GSL_LIBRARIES})

    # add a costum target to run this integration test
    add_custom_target("RunLocal${test-name}"
                       python ${PROJECT_SOURCE_DIR}/scripts/integratedTests.py ${test-name}
                       WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

    # add the dependency to the test target
    add_dependencies("RunLocal${test-name}" ${test-name})

    # we also need to build the pdb-server and pdb-cluster
    add_dependencies("RunLocal${test-name}" pdb-server pdb-cluster)

endfunction(add_pdb_integration_test)

# include all the integration tests
file(GLOB sub-dir tests/integration/*)
foreach(test-dir ${sub-dir})
    include(${test-dir}/CMakeLists.txt)
endforeach()

# include the unit tests
include(tests/unit/CMakeLists.txt)

# add a custom target to run the integration tests
add_custom_target(run-integration-tests
                  python ${PROJECT_SOURCE_DIR}/scripts/integratedTests.py int
                  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

add_custom_target(run-tpch-tests
                  python ${PROJECT_SOURCE_DIR}/scripts/integratedTests.py tpch
                  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

add_custom_target(run-la-tests
                  python ${PROJECT_SOURCE_DIR}/scripts/integratedTests.py la
                  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

add_custom_target(run-ml-tests
                  python ${PROJECT_SOURCE_DIR}/scripts/integratedTests.py ml
                  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

# add a costum target to run the integration tests
add_custom_target(run-docker-integration-tests
                  scripts/runDockerTests.sh
                  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

# add the dependency for building tests
add_dependencies(run-integration-tests build-tests shared-libraries pdb-server pdb-cluster)
add_dependencies(run-tpch-tests build-tests shared-libraries pdb-server pdb-cluster)
add_dependencies(run-la-tests build-tests shared-libraries pdb-server pdb-cluster)
add_dependencies(run-ml-tests build-tests shared-libraries pdb-server pdb-cluster)

# clean integration tests target
add_custom_target(clean-integration-tests
                  scripts/cleanupNode.sh
                  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})