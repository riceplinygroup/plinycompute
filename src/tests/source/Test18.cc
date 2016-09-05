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
#include "DatabaseQuery.h"
#include "QueryClient.h"
#include "QueryOutput.h"

using namespace pdb;

class ChrisJoin : public Join <String, double, int, double> {

	virtual Lambda <bool> getSelection (Handle <double> &in1, Handle <int> &in2, Handle <double> &in3) {
		return makeLambda ([&] () {return true;});
	}

	virtual Lambda <Handle <String>> getProjection (Handle <double> &in1, Handle <int> &in2, Handle <double> &in3) {	
		return makeLambda ([&] () {Handle <String> returnVal = makeObject <String> ("Hi mom!!");
					   return returnVal;});
	}
	
};

class ChrisSelect : public Selection <String, String> {

	virtual Lambda <bool> getSelection (Handle <String> &in) {
		return makeLambda ([&] () {return true;});
	}

	virtual Lambda <Handle <String>> getProjection (Handle <String> &in) {
		return makeLambda (in, [&] () {return in;});
	}
};

int main () {

	// connect to the client
	QueryClient myClient (8191, "localhost");

	// ask him for a database qurey
	DatabaseQuery myQuery = myClient.makeQuery ("ChrisDB");
	
	// the join takes as input threee tables
	Handle <ChrisJoin> myJoin = makeObject <ChrisJoin> ();
	myJoin->setInput (0, myQuery.scan <double> ("BunchOfDoubles"));
	myJoin->setInput (1, myQuery.scan <int> ("BunchOfInts"));
	myJoin->setInput (2, myQuery.scan <double> ("EvenMoreDoubles"));

	// the selection takes an input the result of the join
	Handle <ChrisSelect> mySelect = makeObject <ChrisSelect> ();
	mySelect->setInput (myJoin);
	
	// the result will go into an iterator on the client machine
	Handle <QueryOutput <String>> myResult = makeObject <QueryOutput <String>> ();
	myResult->setInput (mySelect);

	// execute the query
	std :: string errMsg;
	if (!myQuery.execute (errMsg)) {
		std :: cout << "Query failed.  Message was: " << errMsg << "\n";
		return 0;
	}
	
	// print the resuts
	for (auto a : *myResult) 
		std :: cout << (*a) << "\n";

}

#endif
