/*****************************************************************************
 *                                                                           *
 *  Copyright 2018 Rice University                                           *
 *                                                                           *
 *  Licensed under the Apache License, Version 2.0 (the "License");          *
 *  you may not use this file except in compliance with the License.         *
 *  You may obtain a copy of the License at                                  *
 *                                                                           *
 *      http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                           *
 *  Unless required by applicable law or agreed to in writing, software      *
 *  distributed under the License is distributed on an "AS IS" BASIS,        *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *  See the License for the specific language governing permissions and      *
 *  limitations under the License.                                           *
 *                                                                           *
 *****************************************************************************/

#ifndef TEST_44_H
#define TEST_44_H

#include "Join.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "Selection.h"
#include "QueryClient.h"
#include "QueryOutput.h"
#include "StorageClient.h"
#include "ChrisSelection.h"
#include "StringSelection.h"
#include "SharedEmployee.h"
#include "QueryNodeIr.h"
#include "Selection.h"
#include "SelectionIr.h"
#include "Set.h"
#include "SourceSetNameIr.h"
#include "Supervisor.h"
#include "ProjectionIr.h"
#include "QueryGraphIr.h"
#include "QueryOutput.h"
#include "IrBuilder.h"
#include "QuerySchedulerServer.h"
using namespace pdb;
using pdb_detail::QueryGraphIr;
using pdb_detail::ProjectionIr;
using pdb_detail::RecordPredicateIr;
using pdb_detail::RecordProjectionIr;
using pdb_detail::SelectionIr;
using pdb_detail::SetExpressionIr;
using pdb_detail::SourceSetNameIr;
using pdb_detail::buildIr;
int main () {

	// for allocations
	const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};

	// register this query class
	string errMsg;
	PDBLoggerPtr myLogger = make_shared <pdb :: PDBLogger> ("clientLog");
	StorageClient temp (8108, "localhost", myLogger);
	temp.registerType ("libraries/libChrisSelection.so", errMsg);
	temp.registerType ("libraries/libStringSelection.so", errMsg);

	// connect to the query client
	QueryClient myClient (8108, "localhost", myLogger);

	// make the query graph
	Handle <Set <SharedEmployee>> myInputSet = myClient.getSet <SharedEmployee> ("chris_db", "chris_set");
	Handle <ChrisSelection> myFirstSelect = makeObject <ChrisSelection> ();
	myFirstSelect->setInput (myInputSet);
	Handle <StringSelection> mySecondSelect = makeObject <StringSelection> ();
	mySecondSelect->setInput (myFirstSelect);
	Handle <QueryOutput <String>> outputOne = makeObject <QueryOutput <String>> ("chris_db", "output_set1", myFirstSelect);
	Handle <QueryOutput <String>> outputTwo = makeObject <QueryOutput <String>> ("chris_db", "output_set2", mySecondSelect);

        Handle<Vector <Handle<QueryBase>>> queries = makeObject<Vector<Handle<QueryBase>>>();
        queries->push_back(outputOne);
        queries->push_back(outputTwo);
        pdb_detail::QueryGraphIrPtr queryGraph = buildIr(queries);

        QuerySchedulerServer server;
        server.parseOptimizedQuery(queryGraph);
        server.printCurrentPlan();
        server.schedule("localhost", 8108, myLogger); 

/*
        queryGraph = buildIr(queries);
        shared_ptr <pdb_detail::SetExpressionIr> curNode;
        for (int i = 0; i < queryGraph->getSinkNodeCount(); i ++) {
            curNode = queryGraph->getSinkNode(i);
            std :: cout << "the " << i << "-th sink:" << std :: endl;
            while (curNode->getName() != "SourceSetNameIr") {
                std :: cout << "current node is " << curNode->getName() << std :: endl;
                if (curNode->isTraversed() == false) {
                    curNode->setTraversed(true, i);
                } else {
                    std :: cout << "We have traversed this node!" << std :: endl;
                }
                shared_ptr<pdb_detail::MaterializationMode> materializationMode = curNode->getMaterializationMode();
                if(materializationMode->isNone() == false) {
                     std :: string name("");
                     std :: cout << "this is a materialization node with databaseName=" << materializationMode->tryGetDatabaseName( name )
                           << " and setName=" << materializationMode->tryGetSetName( name ) << std :: endl;
                }
                if(curNode->getName() == "SelectionIr") {
                    shared_ptr<pdb_detail::SelectionIr> selectionNode = dynamic_pointer_cast<pdb_detail::SelectionIr>(curNode);
                    curNode = selectionNode->getInputSet();
                } else if (curNode->getName() == "ProjectionIr") {
                    shared_ptr<pdb_detail::ProjectionIr> projectionNode = dynamic_pointer_cast<pdb_detail::ProjectionIr>(curNode);
                    curNode = projectionNode->getInputSet();
                }
            }
            std :: cout << "current node is " << curNode->getName() << std :: endl;
            if (curNode->getName() == "SourceSetNameIr") {
                 shared_ptr<pdb_detail::SourceSetNameIr> sourceNode = dynamic_pointer_cast<pdb_detail::SourceSetNameIr>(curNode);
                 std :: cout << "this is SourceSetName node with databaseName =" << sourceNode->getDatabaseName() << " and setName=" << sourceNode->getSetName() << std :: endl;
           }
        }
*/
/*	
	if (!myClient.execute (errMsg, outputOne, outputTwo)) {
		std :: cout << "Query failed.  Message was: " << errMsg << "\n";
		return 0;
	}
*/
	std::cout << std::endl;
	// print the resuts
	SetIterator <String> result = myClient.getSetIterator <String> ("chris_db", "output_set1");
	std :: cout << "First set of query results: ";
	for (auto a : result) 
        {
		     std :: cout << (*a) << "; ";
        }
	std :: cout << "\n\nSecond set of query results: ";
	result = myClient.getSetIterator <String> ("chris_db", "output_set2");
	for (auto a : result)
        {
		    std :: cout << (*a) << "; ";
        }
	std :: cout << "\n";
	
	// and delete the sets
	myClient.deleteSet ("chris_db", "output_set1");
	myClient.deleteSet ("chris_db", "output_set2");
        system ("scripts/cleanupSoFiles.sh");
        
}

#endif
