###
### SharedObjects
###

# compile all the objects
file(GLOB files "${PROJECT_SOURCE_DIR}/applications/TestGMM/sharedLibraries/source/*.cc")
foreach(file ${files})
    get_filename_component(fileName "${file}" NAME_WE)
    add_library(${fileName} MODULE ${file})
    target_link_libraries(${fileName} pdb-shared-common)
    target_link_libraries(${fileName} ${GSL_LIBRARIES})
    add_dependencies(shared-libraries ${fileName})
endforeach()

###
### TestKMeans
###

# create the target
add_pdb_application(TestGmmLazy)

# add a build dependency to build-tests target
add_dependencies(build-ml-tests TestGmmLazy)

# add build dependencies to shared libraries it uses
add_dependencies(TestGmmLazy GmmAggregateLazy)
add_dependencies(TestGmmLazy GmmModel)
add_dependencies(TestGmmLazy GmmAggregateOutputLazy)
add_dependencies(TestGmmLazy GmmAggregateDatapoint)
add_dependencies(TestGmmLazy GmmAggregateNewComp)
add_dependencies(TestGmmLazy GmmSampleSelection)

###
### TestGmmLoadData
###

# create the target
add_pdb_application(TestGmmLoadData)

# add a build dependency to build-tests target
add_dependencies(build-ml-tests TestGmmLoadData)