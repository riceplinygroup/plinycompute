cmake_minimum_required(VERSION 3.5.1)

# set clang as the default compiler
set(CMAKE_C_COMPILER "clang")
set(CMAKE_CXX_COMPILER "clang++")

project(pdb)

# set the output directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY  "${PROJECT_SOURCE_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/libraries")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/libraries")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/tools/cmake")
set(CTEST_BINARY_DIRECTORY "${PROJECT_SOURCE_DIR}/bin")

# set the standard
set(CMAKE_CXX_STANDARD 14)

# check the operating system and add the appropriate flags
if (APPLE)
    SET(CMAKE_CXX_FLAGS "-g -fPIC -Wno-deprecated-declarations")
    SET(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -undefined dynamic_lookup")
elseif (UNIX AND NOT APPLE)
    SET(CMAKE_CXX_FLAGS "-g -fPIC -Wno-deprecated-declarations")
endif ()

# set the definitions for the compiler
ADD_DEFINITIONS(-DINITIALIZE_ALLOCATOR_BLOCK)
ADD_DEFINITIONS(-DENABLE_SHALLOW_COPY)
ADD_DEFINITIONS(-DDEFAULT_BATCH_SIZE=100)
ADD_DEFINITIONS(-DREMOVE_SET_WITH_EVICTION)
ADD_DEFINITIONS(-DAUTO_TUNING)
ADD_DEFINITIONS(-DENABLE_LARGE_GRAPH)
ADD_DEFINITIONS(-DJOIN_HASH_TABLE_SIZE_RATIO=1.5)
ADD_DEFINITIONS(-DEVICT_STOP_THRESHOLD=0.95)
ADD_DEFINITIONS(-DEVICT_STOP_THRESHOLD=0.95)
ADD_DEFINITIONS(-DENABLE_COMPRESSION)
ADD_DEFINITIONS(-DPROFILING)

# used to enable debug
if (USE_DEBUG)
   message("PDB_DEBUG is ON")
   ADD_DEFINITIONS(-DPDB_DEBUG)
elseif (NOT USE_DEBUG)
   REMOVE_DEFINITIONS(-DPDB_DEBUG)
   message("PDB_DEBUG is OFF")
endif (USE_DEBUG)

# set the directories with the common header files
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/memory/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/utilities/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/optimizerBenchmark/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/pdbServer/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/queryPlanning/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/qunit/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/dispatcher/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/bufferMgr/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/lambdas/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/lambdas/headers/lambdas")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/lambdas/headers/creationFunctions")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/tpchBench/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/work/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/distributionManager/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/communication/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/logicalPlan/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/queries/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/queriesTests/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/serverFunctionalities/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/catalog/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/builtInPDBObjects/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/conf/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/dataPartition/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/mainClient/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/storage/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/objectModel/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/sharedLibraries/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/queryExecution/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/computations/headers")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/computations/headers/base")
include_directories("${PROJECT_SOURCE_DIR}/pdb/src/pipeline/headers")

# add the third party libraries as includes
include_directories("${PROJECT_SOURCE_DIR}/third-party/mustache/headers")
include_directories("${PROJECT_SOURCE_DIR}/third-party/sqlite/headers")


# specify the source files
file(GLOB SERVER_FUNCTIONALITIES_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/serverFunctionalities/source/*.cc")
file(GLOB COMMUNICATION_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/communication/source/*.cc")
file(GLOB CATALOG_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/catalog/source/*.cc")
file(GLOB DISPATCHER_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/dispatcher/source/*.cc")
file(GLOB DATA_PARTITION_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/dataPartition/source/*.cc")
file(GLOB PDB_SERVER_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/pdbServer/source/*.cc")
file(GLOB OBJECT_MODEL_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/objectModel/source/*.cc")
file(GLOB QUERY_EXECUTION_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/queryExecution/source/*.cc")
file(GLOB QUERY_PANNING_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/queryPlanning/source/**/*.cc" "${PROJECT_SOURCE_DIR}/pdb/src/queryPlanning/source/*.cc")
file(GLOB WORK_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/work/source/*.cc")
file(GLOB MEMORY_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/memory/source/*.cc")
file(GLOB STORAGE_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/storage/source/*.cc")
file(GLOB DISTRIBUTION_MANAGER_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/distributionManager/source/*.cc")
file(GLOB LAMBDAS_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/lambdas/source/*.cc")
file(GLOB LOGICAL_PLAN_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/logicalPlan/source/*.cc")
file(GLOB PDB_CLIENT_SOURCE "${PROJECT_SOURCE_DIR}/pdb/src/mainClient/source/*.cc")
set(SOURCE_FILES_SQL_LITE ${PROJECT_SOURCE_DIR}/third-party/sqlite/source/sqlite3.c)

# specify the hearder files
file(GLOB PDB_HEADERS "${PROJECT_SOURCE_DIR}/pdb/src/*/headers/*.h"
                      "${PROJECT_SOURCE_DIR}/pdb/src/*/headers/**/*.h")


# specify the third party headers
file(GLOB THIRD_PARTY_HEADERS "${PROJECT_SOURCE_DIR}/third-party/*/headers/*.h"
                              "${PROJECT_SOURCE_DIR}/third-party/*/headers/**/*.h")

add_custom_target(HEADER_ONLY_TARGET SOURCES "${PDB_HEADERS}" "${THIRD_PARTY_HEADERS}")

# generate lexers
FIND_PACKAGE(FLEX REQUIRED)

# set the output paths
SET(LogicalPlanFlexOutput ${CMAKE_SOURCE_DIR}/pdb/src/logicalPlan/source/Lexer.c)

# find the UUID package
FIND_PACKAGE(UUID REQUIRED)

# find the boost package
FIND_PACKAGE(Boost COMPONENTS program_options filesystem system REQUIRED)
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})

# setup the commands
IF (FLEX_FOUND)
    # logical plan lexer commands
    ADD_CUSTOM_COMMAND(
            OUTPUT ${LogicalPlanFlexOutput}
            COMMAND ${FLEX_EXECUTABLE}
            --outfile=${LogicalPlanFlexOutput}
            ${CMAKE_SOURCE_DIR}/pdb/src/logicalPlan/source/Lexer.l
            COMMENT "Generating Logical Plan Lexer!"
            BUILD_BYPRODUCTS ${LogicalPlanFlexOutput}
    )
ENDIF ()

# generate the parser
FIND_PACKAGE(BISON REQUIRED)

# set the output paths
SET(LogicalPlanParserOutput ${CMAKE_SOURCE_DIR}/pdb/src/logicalPlan/source/Parser.c)

# setup the commands
IF (BISON_FOUND)
    # logical plan parser commands
    ADD_CUSTOM_COMMAND(
            OUTPUT ${LogicalPlanParserOutput}
            COMMAND ${BISON_EXECUTABLE}
            --output=${LogicalPlanParserOutput}
            ${CMAKE_SOURCE_DIR}/pdb/src/logicalPlan/source/Parser.y
            COMMENT "Generating Logical Plan Parser"
            BUILD_BYPRODUCTS ${LogicalPlanParserOutput}
    )
ENDIF ()

# find gsl
FIND_PACKAGE(GSL REQUIRED)
include_directories(${GSL_INCLUDE_DIRS} ${GSLCBLAS_INCLUDE_DIRS})

#find snappy
FIND_PACKAGE(Snappy REQUIRED)

# the files generated from the type codes
SET(BUILT_IN_OBJECT_TYPE_ID        ${CMAKE_SOURCE_DIR}/pdb/src/objectModel/headers/BuiltInObjectTypeIDs.h)
SET(BUILT_IN_PDB_OBJECTS           ${CMAKE_SOURCE_DIR}/pdb/src/objectModel/headers/BuiltinPDBObjects.h)
SET(BUILT_IN_PDB_OBJECTS_GENERATED ${CMAKE_SOURCE_DIR}/pdb/src/objectModel/headers/BuiltinPDBObjects.h
                                   ${CMAKE_SOURCE_DIR}/pdb/src/objectModel/headers/BuiltinPDBObjects.cc)

# mark the files as generated
SET_SOURCE_FILES_PROPERTIES(${BUILT_IN_PDB_OBJECTS_GENERATED} PROPERTIES GENERATED TRUE)
SET_SOURCE_FILES_PROPERTIES(${BUILT_IN_OBJECT_TYPE_ID} PROPERTIES GENERATED TRUE)

# add the commands to generate the type ids
add_custom_command (
        OUTPUT  object-type-ids-command
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/type-id-data
        COMMAND "${CMAKE_SOURCE_DIR}/scripts/generateTypeCodes.py" BuiltInObjectTypeIDs ${CMAKE_CURRENT_BINARY_DIR}/type-id-data
        COMMAND "${CMAKE_SOURCE_DIR}/scripts/generateTypeCodes.py" BuiltinPDBObjects ${CMAKE_CURRENT_BINARY_DIR}/type-id-data
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/pdb"
        COMMENT "Generating type codes!"
)

# create a target to run the command and make every other target depend on it so it gets to run first
add_custom_target(object-type-ids ALL
        DEPENDS object-type-ids-command
)

# include the directory we just created in the clean target
set_directory_properties(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_CURRENT_BINARY_DIR}/type-id-data)

# find the threads library
find_package(Threads)

# setup the libraries
add_library(sql-lite OBJECT ${SOURCE_FILES_SQL_LITE})
add_library(server-functionalities OBJECT ${SERVER_FUNCTIONALITIES_SOURCE})
add_library(communication OBJECT ${COMMUNICATION_SOURCE})
add_library(catalog OBJECT ${CATALOG_SOURCE})
add_library(dispatcher OBJECT ${DISPATCHER_SOURCE})
add_library(lib-pdb-server OBJECT ${PDB_SERVER_SOURCE})
add_library(query-execution OBJECT ${QUERY_EXECUTION_SOURCE})
add_library(query-planning OBJECT ${QUERY_PANNING_SOURCE})
add_library(work OBJECT ${WORK_SOURCE})
add_library(memory OBJECT ${MEMORY_SOURCE})
add_library(storage OBJECT ${STORAGE_SOURCE})
add_library(distribution-manager OBJECT ${DISTRIBUTION_MANAGER_SOURCE})
add_library(lambdas OBJECT ${LAMBDAS_SOURCE})
add_library(logical-plan OBJECT ${LOGICAL_PLAN_SOURCE})
add_library(pdb-client OBJECT ${PDB_CLIENT_SOURCE})
add_library(object-model OBJECT ${OBJECT_MODEL_SOURCE})

# add the dependencies to the object type generationg
add_dependencies(server-functionalities object-type-ids)
add_dependencies(communication object-type-ids)
add_dependencies(object-model object-type-ids)
add_dependencies(pdb-client object-type-ids)
add_dependencies(logical-plan object-type-ids)
add_dependencies(lib-pdb-server object-type-ids)
add_dependencies(distribution-manager object-type-ids)

# used to build the pdb-manager and pdb-worker
add_library(pdb-server-common $<TARGET_OBJECTS:catalog>
        $<TARGET_OBJECTS:dispatcher>
        $<TARGET_OBJECTS:distribution-manager>
        $<TARGET_OBJECTS:communication>
        $<TARGET_OBJECTS:lambdas>
        $<TARGET_OBJECTS:logical-plan>
        $<TARGET_OBJECTS:linear-algebra>
        $<TARGET_OBJECTS:lib-pdb-server>
        $<TARGET_OBJECTS:memory>
        $<TARGET_OBJECTS:object-model>
        $<TARGET_OBJECTS:query-execution>
        $<TARGET_OBJECTS:query-planning>
        $<TARGET_OBJECTS:sql-lite>
        $<TARGET_OBJECTS:server-functionalities>
        $<TARGET_OBJECTS:storage>
        $<TARGET_OBJECTS:work>)

# used to link the shared libraries that the shared librarires use
add_library(pdb-shared-common $<TARGET_OBJECTS:catalog>
        $<TARGET_OBJECTS:dispatcher>
        $<TARGET_OBJECTS:distribution-manager>
        $<TARGET_OBJECTS:communication>
        $<TARGET_OBJECTS:lambdas>
        $<TARGET_OBJECTS:logical-plan>
        $<TARGET_OBJECTS:linear-algebra>
        $<TARGET_OBJECTS:lib-pdb-server>
        $<TARGET_OBJECTS:memory>
        $<TARGET_OBJECTS:object-model>
        $<TARGET_OBJECTS:query-execution>
        $<TARGET_OBJECTS:query-planning>
        $<TARGET_OBJECTS:sql-lite>
        $<TARGET_OBJECTS:server-functionalities>
        $<TARGET_OBJECTS:storage>
        $<TARGET_OBJECTS:work>)

# used to link the shared libraries that the test use
add_library(pdb-tests-common $<TARGET_OBJECTS:catalog>
        $<TARGET_OBJECTS:dispatcher>
        $<TARGET_OBJECTS:distribution-manager>
        $<TARGET_OBJECTS:communication>
        $<TARGET_OBJECTS:lambdas>
        $<TARGET_OBJECTS:logical-plan>
        $<TARGET_OBJECTS:linear-algebra>
        $<TARGET_OBJECTS:lib-pdb-server>
        $<TARGET_OBJECTS:memory>
        $<TARGET_OBJECTS:object-model>
        $<TARGET_OBJECTS:query-execution>
        $<TARGET_OBJECTS:query-planning>
        $<TARGET_OBJECTS:sql-lite>
        $<TARGET_OBJECTS:server-functionalities>
        $<TARGET_OBJECTS:storage>
        $<TARGET_OBJECTS:work>)

# link the dependent libraries so that they are made of the public interface
target_link_libraries(pdb-server-common PRIVATE ${SNAPPY_LIBRARY})
target_link_libraries(pdb-server-common PUBLIC ${UUID_LIBRARY})
target_link_libraries(pdb-server-common PRIVATE ${CMAKE_DL_LIBS})
target_link_libraries(pdb-server-common PRIVATE ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(pdb-server-common PRIVATE ${Boost_LIBRARIES})

target_link_libraries(pdb-tests-common PRIVATE ${SNAPPY_LIBRARY})
target_link_libraries(pdb-tests-common PUBLIC ${UUID_LIBRARY})
target_link_libraries(pdb-tests-common PRIVATE ${CMAKE_DL_LIBS})
target_link_libraries(pdb-tests-common PRIVATE ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(pdb-tests-common PRIVATE ${Boost_LIBRARIES})

# form the parsers for the logical plan
add_library(logical-plan-parser OBJECT ${LogicalPlanFlexOutput} ${LogicalPlanParserOutput})

# set the pdb-manager build target
set(SOURCE_FILES_PDB_CLUSTER ${PROJECT_SOURCE_DIR}/pdb/src/mainServer/source/ManagerMain.cc)
add_executable(pdb-manager ${SOURCE_FILES_PDB_CLUSTER} $<TARGET_OBJECTS:logical-plan-parser> $<TARGET_OBJECTS:linear-algebra-parser>)
target_link_libraries(pdb-manager pdb-server-common)
target_link_libraries(pdb-manager ${GSL_LIBRARIES})

# set the pdb-worker build target
set(SOURCE_FILES_PDB_CLUSTER ${PROJECT_SOURCE_DIR}/pdb/src/mainServer/source/WorkerMain.cc $<TARGET_OBJECTS:logical-plan-parser> $<TARGET_OBJECTS:linear-algebra-parser>)
add_executable(pdb-worker ${SOURCE_FILES_PDB_CLUSTER})
target_link_libraries(pdb-worker pdb-server-common)
target_link_libraries(pdb-worker ${GSL_LIBRARIES})

# compile all the objects
file(GLOB files "${PROJECT_SOURCE_DIR}/pdb/src/sharedLibraries/source/*.cc")
add_custom_target(shared-libraries)
foreach(file ${files})
    get_filename_component(fileName "${file}" NAME_WE)
    add_library(${fileName} MODULE ${file})
    target_link_libraries(${fileName} pdb-shared-common)
    add_dependencies(shared-libraries ${fileName})
endforeach()

# include tests that are in tools/cmake/tests directory
include(tests/Tests.cmake)

# include tests that are in tools/cmake/tests directory
include(applications/Applications.cmake)
