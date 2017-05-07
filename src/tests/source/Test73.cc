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

#include "Handle.h"
#include "Lambda.h"
#include "Supervisor.h"
#include "Employee.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SetWriter.h"
#include "SelectionComp.h"
#include "AggregateComp.h"
#include "JoinComp.h"
#include "ScanSet.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "StringIntPair.h"
#include "QueryGraphAnalyzer.h"
// to run the aggregate, the system first passes each through the hash operation...
// then the system 
using namespace pdb;

class SillyWrite : public SetWriter <String> {

public:

	ENABLE_DEEP_COPY

	// eventually, this method should be moved into a class that works with the system to 
	// iterate through pages that are pulled from disk/RAM by the system... a programmer
	// should not provide this particular method
	ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &whichAttsToOpOn, TupleSpec &projection, ComputePlan &plan) override {
		return std :: make_shared <VectorSink <String>> (consumeMe, projection);
	}
};

// this plan has three tables: A (a: int), B (a: int, c: String), C (c: int)
// it first joins A with B, and then joins the result with C
class SillyJoin : public JoinComp <String, int, StringIntPair, String> {

public:

	ENABLE_DEEP_COPY

	Lambda <bool> getSelection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                std :: cout << "SillyJoin selection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
		return (makeLambdaFromSelf (in1) == makeLambdaFromMember (in2, myInt)) && (makeLambdaFromMember (in2, myString) == makeLambdaFromSelf (in3));
	}
	
	Lambda <Handle <String>> getProjection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                 std :: cout << "SillyJoin projection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
		return makeLambdaFromMember (in2, myString);	
	}

};


// this plan has three tables: A (a: int), B (a: int, c: String), C (c: int)
// it first joins A with B, and then joins the result with C
class SillyTrickyJoin : public JoinComp <String, int, StringIntPair, String> {

public:

        ENABLE_DEEP_COPY

        Lambda <bool> getSelection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                std :: cout << "SillyJoin selection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
                return (makeLambdaFromSelf (in1) == makeLambdaFromMember (in2, myInt)) && makeLambda (in3, [] ( Handle<String> & in3) { 
                  return true;
                  });
        }

        Lambda <Handle <String>> getProjection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                 std :: cout << "SillyJoin projection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
                return makeLambdaFromMember (in2, myString);
        }

};





// this plan has three tables: A (a: int), B (a: int, c: String), C (c: int)
// it first joins A with B, and then joins the result with C
class SillyCartesianJoin : public JoinComp <String, int, StringIntPair, String> {

public:

        ENABLE_DEEP_COPY

        Lambda <bool> getSelection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                std :: cout << "SillyCartesianJoin selection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
                return makeLambda (in1, in2, in3, [] (Handle <int> & in1,  Handle <StringIntPair> &in2, Handle <String> &in3) {
                      return true;
                });
        }

        Lambda <Handle <String>> getProjection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                 std :: cout << "SillyJoin projection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
                return makeLambdaFromMember (in2, myString);
        }

};



// this plan has three tables: A (a: int), B (a: int, c: String), C (c: int)
// it first joins A with B, and then joins the result with C
class SillyCoolJoin : public JoinComp <String, int, StringIntPair, String> {

public:

        ENABLE_DEEP_COPY

        Lambda <bool> getSelection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                std :: cout << "SillyCartesianJoin selection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
                return makeLambda (in1, in2, in3, [] (Handle <int> & in1,  Handle <StringIntPair> &in2, Handle <String> &in3) {
                      return true;
                });
        }


        Lambda <Handle <String>> getProjection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                 std :: cout << "SillyJoin projection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
                 return makeLambda (in1, in2, in3, [] (Handle <int> &in1, Handle <StringIntPair> &in2, Handle <String> &in3) {
                                std::ostringstream oss;
                                oss << "Got int " << *in1 << " and StringIntPair (" << in2->myInt << ", '" << *(in2->myString) << "') and String '" << *in3 << "'";
                                Handle <String> res = makeObject <String> (oss.str ());
                                return res;
                        });
        }

};

class SillyReadOfA : public ScanSet <int> {

	ENABLE_DEEP_COPY

	// eventually, this method should be moved into a class that works with the system to 
	// iterate through pages that are pulled from disk/RAM by the system... a programmer
	// should not provide this particular method
	ComputeSourcePtr getComputeSource (TupleSpec &schema, ComputePlan &plan) override {

		return std :: make_shared <VectorTupleSetIterator> (

			// constructs a list of data objects to iterate through
			[] () -> void * {
				
				// this implementation only serves six pages
				static int numPages = 0;
				if (numPages == 6)
					return nullptr;

				// create a page, loading it with random data
				void *myPage = malloc (1024 * 1024);
				{
	 			      	const UseTemporaryAllocationBlock tempBlock {myPage, 1024 * 1024};
	
					// write a bunch of supervisors to it
				       	Handle <Vector <Handle <int>>> data = makeObject <Vector <Handle <int>>> ();
					int i = 0;	
   				    	try {
       		    			    	for (; true; i++) {
	       	       			         	Handle <int> myInt = makeObject <int> (i);
       		          			      	data->push_back (myInt);
						}
					} catch (NotEnoughSpace &e) {
						std :: cout << "got to " << i << " when proucing data for SillyReadOfA.\n";
						getRecord (data);
					}
				}
				numPages++;
				return myPage;
			}, 

			// frees the list of data objects that have been iterated
			[] (void *freeMe) -> void {
				free (freeMe);
			},

			// and this is the chunk size, or number of items to put into each tuple set
			24);
	}
};

class SillyReadOfB : public ScanSet <StringIntPair> {

	ENABLE_DEEP_COPY

	// eventually, this method should be moved into a class that works with the system to 
	// iterate through pages that are pulled from disk/RAM by the system... a programmer
	// should not provide this particular method
	ComputeSourcePtr getComputeSource (TupleSpec &schema, ComputePlan &plan) override {

		return std :: make_shared <VectorTupleSetIterator> (

			// constructs a list of data objects to iterate through
			[] () -> void * { 
				// this implementation only serves six pages
				static int numPages = 0;
				if (numPages == 6)
					return nullptr;

				// create a page, loading it with random data
				void *myPage = malloc (1024 * 1024);
				{
	 			      	const UseTemporaryAllocationBlock tempBlock {myPage, 1024 * 1024};
	
					// write a bunch of supervisors to it
				       	Handle <Vector <Handle <StringIntPair>>> data = makeObject <Vector <Handle <StringIntPair>>> ();
	
					int i = 0;	
   				    	try {
       		    			    	for (; true; i++) {
							std::ostringstream oss; 
							oss << "My string is " << i; 
							oss.str();
	       	       			         	Handle <StringIntPair> myPair = makeObject <StringIntPair> (oss.str (), i);
       		          			      	data->push_back (myPair);
						}
					} catch (NotEnoughSpace &e) {
						std :: cout << "got to " << i << " when proucing data for SillyReadOfB.\n";
						getRecord (data);
					}
				}
				numPages++;
				return myPage;
			}, 

			// frees the list of data objects that have been iterated
			[] (void *freeMe) -> void {
				free (freeMe);
			},

			// and this is the chunk size, or number of items to put into each tuple set
			24);
	}
};

class SillyReadOfC : public ScanSet <String> {

	ENABLE_DEEP_COPY

	// eventually, this method should be moved into a class that works with the system to 
	// iterate through pages that are pulled from disk/RAM by the system... a programmer
	// should not provide this particular method
	ComputeSourcePtr getComputeSource (TupleSpec &schema, ComputePlan &plan) override {

		return std :: make_shared <VectorTupleSetIterator> (

			// constructs a list of data objects to iterate through
			[] () -> void * {
				
				// this implementation only serves six pages
				static int numPages = 0;
				if (numPages == 6)
					return nullptr;

				// create a page, loading it with random data
				void *myPage = malloc (1024 * 1024);
				{
	 			      	const UseTemporaryAllocationBlock tempBlock {myPage, 1024 * 1024};
	
					// write a bunch of supervisors to it
				       	Handle <Vector <Handle <String>>> data = makeObject <Vector <Handle <String>>> ();
	
					int j = 0;
   				    	try {
       		    			    	for (int i = 0; true; i += 3) {
							std::ostringstream oss; 
							oss << "My string is " << i; 
							oss.str();
	       	       			         	Handle <String> myString = makeObject <String> (oss.str ());
       		          			      	data->push_back (myString);
							j++;
						}
					} catch (NotEnoughSpace &e) {
						std :: cout << "got to " << j << " when proucing data for SillyReadOfC.\n";
						getRecord (data);
					}
				}
				numPages++;
				return myPage;
			}, 

			// frees the list of data objects that have been iterated
			[] (void *freeMe) -> void {
				free (freeMe);
			},

			// and this is the chunk size, or number of items to put into each tuple set
			24);
	}
};

int main () {
	
	// this is the object allocation block where all of this stuff will reside
       	makeObjectAllocatorBlock (1024 * 1024, true);

	
	// create all of the computation objects
	Handle <Computation> readA = makeObject <SillyReadOfA> ();
	Handle <Computation> readB = makeObject <SillyReadOfB> ();
	Handle <Computation> readC = makeObject <SillyReadOfC> ();
	Handle <Computation> myJoin = makeObject <SillyJoin> ();
	Handle <Computation> myWriter = makeObject <SillyWrite> ();
         
        std :: cout << "##############################" << std :: endl;

        myJoin->setInput(0, readA);
        myJoin->setInput(1, readB);
        myJoin->setInput(2, readC);
        myWriter->setInput(myJoin);
        std :: vector <Handle<Computation>> queryGraph;
        queryGraph.push_back(myWriter);
        QueryGraphAnalyzer queryAnalyzer(queryGraph);
        std :: string tcapString = queryAnalyzer.parseTCAPString();
        std :: cout << "TCAP OUTPUT:" << std :: endl;
        std :: cout << tcapString << std :: endl;

        std :: cout << "#################################" << std :: endl;
        // create all of the computation objects
        readA = makeObject <SillyReadOfA> ();
        readB = makeObject <SillyReadOfB> ();
        readC = makeObject <SillyReadOfC> ();
        Handle <Computation> myCartesianJoin = makeObject <SillyCartesianJoin> ();
        myWriter = makeObject <SillyWrite> ();
        myCartesianJoin->setInput(0, readA);
        myCartesianJoin->setInput(1, readB);
        myCartesianJoin->setInput(2, readC);
        myWriter->setInput(0, myCartesianJoin);
        std :: vector <Handle<Computation>> queryGraph1;
        queryGraph1.push_back(myWriter);
        QueryGraphAnalyzer queryAnalyzer1(queryGraph1);
        std :: string tcapString1 = queryAnalyzer1.parseTCAPString();
        std :: cout << "TCAP OUTPUT:" << std :: endl;
        std :: cout << tcapString1 << std :: endl;

        std :: cout << "#################################" << std :: endl;

        // create all of the computation objects
        readA = makeObject <SillyReadOfA> ();
        readB = makeObject <SillyReadOfB> ();
        readC = makeObject <SillyReadOfC> ();
        Handle <Computation> myTrickyJoin = makeObject <SillyTrickyJoin> ();
        myWriter = makeObject <SillyWrite> ();
        myTrickyJoin->setInput(0, readA);
        myTrickyJoin->setInput(1, readB);
        myTrickyJoin->setInput(2, readC);
        myWriter->setInput(0, myTrickyJoin);
        std :: vector <Handle<Computation>> queryGraph2;
        queryGraph2.push_back(myWriter);
        QueryGraphAnalyzer queryAnalyzer2(queryGraph2);
        std :: string tcapString2 = queryAnalyzer2.parseTCAPString();
        std :: cout << "TCAP OUTPUT:" << std :: endl;
        std :: cout << tcapString2 << std :: endl;

        std :: cout << "#################################" << std :: endl;

        // create all of the computation objects
        readA = makeObject <SillyReadOfA> ();
        readB = makeObject <SillyReadOfB> ();
        readC = makeObject <SillyReadOfC> ();
        Handle <Computation> myCoolJoin = makeObject <SillyCoolJoin> ();
        myWriter = makeObject <SillyWrite> ();
        myCoolJoin->setInput(0, readA);
        myCoolJoin->setInput(1, readB);
        myCoolJoin->setInput(2, readC);
        myWriter->setInput(0, myCoolJoin);
        std :: vector <Handle<Computation>> queryGraph3;
        queryGraph3.push_back(myWriter);
        QueryGraphAnalyzer queryAnalyzer3(queryGraph3);
        std :: string tcapString3 = queryAnalyzer3.parseTCAPString();
        std :: cout << "TCAP OUTPUT:" << std :: endl;
        std :: cout << tcapString3 << std :: endl;

        std :: cout << "#################################" << std :: endl;



}
