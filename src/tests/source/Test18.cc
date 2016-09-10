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

#include "../../sharedLibraries/source/SharedLibEmployee.cc"

using namespace pdb;

namespace pdb {

class ChrisSelection : public Selection <String, SharedEmployee> {

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

}

// this is just here as an example... not actually used
class ChrisJoin : public Join <String, double, int, double> {

	virtual Lambda <bool> getSelection (Handle <double> &in1, Handle <int> &in2, Handle <double> &in3) {
		return makeLambda ([&] () {return true;});
	}

	virtual Lambda <Handle <String>> getProjection (Handle <double> &in1, Handle <int> &in2, Handle <double> &in3) {	
		return makeLambda ([&] () {Handle <String> returnVal = makeObject <String> ("Hi mom!!");
					   return returnVal;});
	}
	
};

int main () {

	// register this query class
	string errMsg;
	PDBLoggerPtr myLogger = make_shared <pdb :: PDBLogger> ("clientLog");
	StorageClient temp (8108, "localhost", myLogger);
	temp.registerType ("libraries/libChrisSelection.so", errMsg);	

	// connect to the query client
	QueryClient myClient (8108, "localhost", myLogger);

	// build a scan of the "chris_db", "chris_set" set
	Handle <ChrisSelection> mySelect = makeObject <ChrisSelection> ();
	mySelect->setInput (0, myClient.getSet <SharedEmployee> ("chris_db", "chris_set"));

	// the result will go into an iterator on the client machine
	Handle <LocalQueryOutput <String>> output = makeObject <LocalQueryOutput <String>> ();
	output->setInput (mySelect);

	if (!myClient.execute (output, errMsg)) {
		std :: cout << "Query failed.  Message was: " << errMsg << "\n";
		return 0;
	}
	
	// print the resuts
	for (auto a : *output) 
		std :: cout << (*a) << "\n";
}

#endif
