# get the current directory
get_filename_component(unit-test-path ${CMAKE_CURRENT_LIST_FILE} DIRECTORY)

# compile all the objects
file(GLOB files "${unit-test-path}/*.cc")
add_custom_target(unit-tests)
foreach(file ${files})
    get_filename_component(fileName "${file}" NAME_WE)
    add_executable(${fileName} ${file}
                               $<TARGET_OBJECTS:logical-plan-parser>
                               $<TARGET_OBJECTS:linear-algebra-parser>
                               $<TARGET_OBJECTS:pdb-client>
                               $<TARGET_OBJECTS:linear-algebra-parser>)
    target_link_libraries(${fileName} pdb-tests-common)
    target_link_libraries(${fileName} ${GSL_LIBRARIES})
    add_test("Unit${fileName}" ${PROJECT_SOURCE_DIR}/bin/${fileName})
    add_dependencies(unit-tests ${fileName})
endforeach()