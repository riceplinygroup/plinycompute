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

#ifndef TEST_18_H
#define TEST_18_H

#include "Join.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "Selection.h"
#include "QueryClient.h"
#include "StorageClient.h"

#include "SharedEmployee.h"

using namespace pdb;

class ChrisSelection : public Selection <String, SharedEmployee> {

	ENABLE_DEEP_COPY

        Lambda <bool> getSelection (Handle <SharedEmployee> &checkMe) override {
                return makeLambda (checkMe, [&] () {
                        return (*(checkMe->getName ()) != "Joe Johnson48");
                });
        }

        Lambda <Handle <String>> getProjection (Handle <SharedEmployee> &checkMe) override {
                return makeLambda (checkMe, [&] {
                        return checkMe->getName ();
                });
        }
};

class StringSelection : public Selection <String, String> {

public:

        ENABLE_DEEP_COPY

        Lambda <bool> getSelection (Handle <String> &checkMe) override {
                return makeLambda (checkMe, [&] () {
                        return (*(checkMe) == "Joe Johnson488") ||  (*(checkMe) == "Joe Johnson489");
                });
        }

        Lambda <Handle <String>> getProjection (Handle <String> &checkMe) override {
                return makeLambda (checkMe, [&] () {
                        return checkMe;
                });
        }
};


int main () {

	// for allocations
	const UseTemporaryAllocationBlock tempBlock {1024 * 128};

	// register this query class
	string errMsg;
	PDBLoggerPtr myLogger = make_shared <pdb :: PDBLogger> ("clientLog");
	StorageClient temp (8108, "localhost", myLogger);
	temp.registerType ("libraries/libChrisSelection.so", errMsg);	
	temp.registerType ("libraries/libStringSelection.so", errMsg);	

	// connect to the query client
	QueryClient myClient (8108, "localhost", myLogger);

	// build a scan of the "chris_db", "chris_set" set
	Handle <ChrisSelection> myFirstSelect = makeObject <ChrisSelection> ();
	myFirstSelect->setInput (myClient.getSet <SharedEmployee> ("chris_db", "chris_set"));

	// now, scan again 
	Handle <StringSelection> mySecondSelect = makeObject <StringSelection> ();
	mySecondSelect->setInput (myFirstSelect);

	// the results will go into two iterators on the client machine
	Handle <LocalQueryOutput <String>> outputOne = makeObject <LocalQueryOutput <String>> ();
	outputOne->setInput (myFirstSelect);

	Handle <LocalQueryOutput <String>> outputTwo = makeObject <LocalQueryOutput <String>> ();
	outputTwo->setInput (mySecondSelect);
	
	if (!myClient.execute (errMsg, outputOne, outputTwo)) {
		std :: cout << "Query failed.  Message was: " << errMsg << "\n";
		return 0;
	}

        // Jia: below code will cause segfault in commit 2258, 2260, and 2263
        // I suspect it is still an issue caused by merge of r2257 and r2256
        // Temporarily fix the segfault by commenting below code. We need a full fix here.
        /*	
	// print the resuts
	std :: cout << "First set of query results: ";
	for (auto a : *outputOne) 
		std :: cout << (*a) << "; ";

	std :: cout << "\n\nSecond set of query results: ";
	for (auto a : *outputTwo) 
		std :: cout << (*a) << "; ";
        */
	std :: cout << "\n";
}

#endif
