cmake_minimum_required(VERSION 3.6)
project(pdb)

find_package (Threads)

set(CMAKE_CXX_COMPILER  /usr/bin/clang++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

include_directories(
        ../../../src/bufferMgr/headers
        ../../../src/builtInPDBObjects/headers
        ../../../src/conf/headers
        ../../../src/communication/headers
        ../../../src/dispatcher/headers
        ../../../src/mocking/headers
        ../../../src/objectModel/headers
        ../../../src/pdbServer/headers
        ../../../src/queries/headers
        ../../../src/queriesTests/headers
        ../../../src/queryExecution/headers
        ../../../src/queryIntermediaryRepTests/headers
        ../../../src/qunit/headers
        ../../../src/queryIntermediaryRep/headers
        ../../../src/queryIrProcessor/headers
        ../../../src/serverFunctionalities/headers
        ../../../src/work/headers)

set(SOURCE_FILES
        ../../../src/bufferMgr/source/MyDB_BufferManager.cc
        ../../../src/bufferMgr/source/MyDB_Page.cc
        ../../../src/bufferMgr/source/MyDB_Table.cc
        ../../../src/communication/headers/SimpleRequest.cc
        ../../../src/communication/source/PDBCommunicator.cc
        ../../../src/objectModel/source/VTableMapConstructor.cc
        ../../../src/objectModel/source/VTableMapCatalogLookup.cc
        ../../../src/pdbServer/source/PDBLogger.cc
        ../../../src/queriesTests/source/SelectionTests.cc
        ../../../src/queriesTests/source/QueriesTestsRunner.cc
        ../../../src/queryIntermediaryRep/source/IrBuilder.cc
        ../../../src/queryIntermediaryRep/source/RecordPredicateIr.cc
        ../../../src/queryIntermediaryRep/source/MaterializationMode.cc
        ../../../src/queryIntermediaryRep/source/SelectionIr.cc
        ../../../src/queryIntermediaryRep/source/SetName.cc
        ../../../src/queryIntermediaryRep/source/UnarySetOperator.cc
        ../../../src/queryIntermediaryRepTests/source/BuildIrTests.cc
        ../../../src/queryIntermediaryRepTests/source/DotBuilderTests.cc
        ../../../src/queryIntermediaryRepTests/source/ProjectionIrTests.cc
        ../../../src/queryIntermediaryRepTests/source/SelectionIrTests.cc
        ../../../src/queryIntermediaryRepTests/source/QueryIntermediaryRepTestsRunner.cc
#        ../../../src/queryIrProcessorTests/source/SelectionIrPageProcessorTests.cc
        ../../../src/serverFunctionalities/source/CatalogClient.cc
#        ../../../src/tests/source/Test13a.cc
#        ../../../src/tests/source/Traverse.cc
        ../../../src/unitTestsRunner/source/AllUnitTestsRunner.cc
        ../../../src/work/source/PDBBuzzer.cc
        ../../../src/work/source/PDBWorkerQueue.cc
        ../../../src/work/source/PDBWork.cc
        ../../../src/work/source/PDBWorker.cc)

add_executable(pdb ${SOURCE_FILES})

target_link_libraries(pdb ${CMAKE_DL_LIBS})
target_link_libraries (pdb ${CMAKE_THREAD_LIBS_INIT})