cmake_minimum_required(VERSION 3.6)
project(pdb)

find_package (Threads)

set(CMAKE_CXX_COMPILER  /usr/bin/clang++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

include_directories(
        ../../../src/boost
        ../../../src/bufferMgr/headers
        ../../../src/builtInPDBObjects/headers
        ../../../src/conf/headers
        ../../../src/communication/headers
        ../../../src/dispatcher/headers
        ../../../src/logicalPlan/headers
        ../../../src/logicalPlanTests/headers
        ../../../src/lambdas/headers
        ../../../src/memory/headers
        ../../../src/mocking/headers
        ../../../src/objectModel/headers
        ../../../src/pdbServer/headers
        ../../../src/sharedLibraries/headers
        ../../../src/queries/headers
        ../../../src/queriesTests/headers
        ../../../src/queryExecution/headers
        ../../../src/queryIntermediaryRepTests/headers
        ../../../src/qunit/headers
        ../../../src/queryIntermediaryRep/headers
        ../../../src/queryIntermediaryRepTests/headers
        ../../../src/queryIrProcessor/headers
        ../../../src/serverFunctionalities/headers
        ../../../src/storage/headers
        ../../../src/tcapIntermediaryRepTests/headers
        ../../../src/tcapIntermediaryRep/headers
        ../../../src/tcapLexer/headers
        ../../../src/tcapLexerTests/headers
        ../../../src/tcapParser/headers
        ../../../src/tcapParserTests/headers
        ../../../src/work/headers)

set(SOURCE_FILES
        ../../../src/boost/libs/filesystem/src/operations.cpp
        ../../../src/boost/libs/filesystem/src/path.cpp
        ../../../src/boost/libs/program_options/src/options_description.cpp
        ../../../src/boost/libs/program_options/src/variables_map.cpp
        ../../../src/boost/libs/system/src/error_code.cpp
        ../../../src/boost/libs/program_options/src/cmdline.cpp
        ../../../src/boost/libs/program_options/src/config_file.cpp
        ../../../src/boost/libs/program_options/src/convert.cpp
        ../../../src/boost/libs/program_options/src/parsers.cpp
        ../../../src/boost/libs/program_options/src/positional_options.cpp
        ../../../src/boost/libs/program_options/src/split.cpp
        ../../../src/boost/libs/program_options/src/utf8_codecvt_facet.cpp
        ../../../src/boost/libs/program_options/src/value_semantic.cpp
        ../../../src/boost/libs/program_options/src/winmain.cpp
        ../../../src/boost/boost/program_options/config.hpp
        ../../../src/boost/boost/program_options/errors.hpp
        ../../../src/bufferMgr/source/MyDB_BufferManager.cc
        ../../../src/bufferMgr/source/MyDB_Page.cc
        ../../../src/bufferMgr/source/MyDB_Table.cc
        ../../../src/communication/headers/SimpleRequest.cc
        ../../../src/communication/source/PDBCommunicator.cc
        ../../../src/logicalPlan/source/Lexer.c
        ../../../src/logicalPlan/source/Parser.c
        ../../../src/logicalPlan/source/ParserHelperFunctions.cc
        ../../../src/logicalPlan/source/LogicalPlanBuilder.cc
        ../../../src/logicalPlanTests/source/BuildLogicalPlanTests.cc
        ../../../src/logicalPlanTests/source/LogicalPlanTestsRunner.cc
        ../../../src/objectModel/source/VTableMapConstructor.cc
        ../../../src/objectModel/source/VTableMapCatalogLookup.cc
        ../../../src/pdbServer/source/PDBLogger.cc
        ../../../src/queriesTests/source/SelectionTests.cc
        ../../../src/queriesTests/source/QueriesTestsRunner.cc
        ../../../src/queryIntermediaryRep/source/IrBuilder.cc
        ../../../src/queryIntermediaryRep/source/RecordPredicateIr.cc
        ../../../src/queryIntermediaryRep/source/MaterializationModeNamedSet.cc
        ../../../src/queryIntermediaryRep/source/MaterializationModeNone.cc
        ../../../src/queryIntermediaryRep/source/ProjectionIr.cc
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
        ../../../src/tcapLexer/source/TcapToken.cc
        ../../../src/tcapLexer/source/TcapLexer.cc
        ../../../src/tcapLexer/source/TcapTokenStream.cc
        ../../../src/tcapLexerTests/source/TcapTestsRunner.cc
        ../../../src/tcapLexerTests/source/LexTcapTests.cc
        ../../../src/tcapParser/source/TcapAttribute.cc
        ../../../src/tcapParser/source/TcapIdentifier.cc
#        ../../../src/tests/source/Test13a.cc
#        ../../../src/tests/source/Traverse.cc
#        ../../../src/unitTestsRunner/source/AllUnitTestsRunner.cc
        ../../../src/work/source/PDBBuzzer.cc
        ../../../src/work/source/PDBWorkerQueue.cc
        ../../../src/work/source/PDBWork.cc
        ../../../src/work/source/PDBWorker.cc
        ../../../src/tcapParser/source/ApplyOperation.cc
        ../../../src/tcapParser/source/BinaryOperation.cc
        ../../../src/tcapParser/source/FilterOperation.cc
        ../../../src/tcapParser/source/GreaterThanOp.cc
        ../../../src/tcapParser/source/HoistOperation.cc
        ../../../src/tcapParser/source/LoadOperation.cc
        ../../../src/tcapParser/source/RetainAllClause.cc
        ../../../src/tcapParser/source/RetainExplicitClause.cc
        ../../../src/tcapParser/source/RetainNoneClause.cc
        ../../../src/tcapParser/source/StringLiteral.cc
        ../../../src/tcapParser/source/StoreOperation.cc
        ../../../src/tcapParser/source/TableAssignment.cc
        ../../../src/tcapParser/source/TcapStatement.cc
        ../../../src/tcapParserTests/source/ParseTcapTests.cc
        ../../../src/tcapParserTests/source/TcapParserTestsRunner.cc
        ../../../src/tcapIntermediaryRepTests/source/TcapIrTestsRunner.cc
        ../../../src/tcapParser/source/TcapParser.cc
        ../../../src/tcapIntermediaryRep/source/ApplyFunction.cc
        ../../../src/tcapIntermediaryRep/source/ApplyMethod.cc
        ../../../src/tcapIntermediaryRep/source/Filter.cc
        ../../../src/tcapIntermediaryRep/source/GreaterThan.cc
        ../../../src/tcapIntermediaryRep/source/Hoist.cc
        ../../../src/tcapIntermediaryRep/source/Load.cc
        ../../../src/tcapIntermediaryRep/source/Store.cc
        ../../../src/tcapIntermediaryRep/source/TcapIrBuilder.cc
        ../../../src/tcapIntermediaryRepTests/headers/BuildTcapIrTests.h
        ../../../src/tcapIntermediaryRepTests/source/BuildTcapIrTests.cc

        ../../../src/tests/source/Test47.cc)



add_executable(pdb ${SOURCE_FILES})

target_link_libraries(pdb ${CMAKE_DL_LIBS})
target_link_libraries (pdb ${CMAKE_THREAD_LIBS_INIT})