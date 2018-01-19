# create the target
add_pdb_application(JaccardMain)

# add a build dependency to build-tests target
add_dependencies(build-tests JaccardMain)

# compile all the objects
file(GLOB files "${PROJECT_SOURCE_DIR}/applications/TPCHBench/sharedLibraries/source/*.cc")
foreach(file ${files})
    get_filename_component(fileName "${file}" NAME_WE)
    add_library(${fileName} MODULE ${file})
    target_link_libraries(${fileName} pdb-shared-common)
    target_link_libraries(${fileName} ${GSL_LIBRARIES})
    add_dependencies(shared-libraries ${fileName})
endforeach()

# add build dependencies to shared libraries it uses
add_dependencies(JaccardMain Customer)
add_dependencies(JaccardMain LineItem)
add_dependencies(JaccardMain Order)
add_dependencies(JaccardMain Part)
add_dependencies(JaccardMain Supplier)
add_dependencies(JaccardMain ScanCustomerSet)
add_dependencies(JaccardMain TopJaccard)
add_dependencies(JaccardMain AllParts)
add_dependencies(JaccardMain JaccardResultWriter)