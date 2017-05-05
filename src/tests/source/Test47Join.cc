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

	// here is the list of computations
	Vector <Handle <Computation>> myComputations;
	
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

	
	// put them in the list of computations
	myComputations.push_back (readA);
	myComputations.push_back (readB);
	myComputations.push_back (readC);
	myComputations.push_back (myJoin);
	myComputations.push_back (myWriter);

	// now we create the TCAP string
        String myTCAPString =
	       
               "/* scan the three inputs */ \n\
	        A (a) <= SCAN ('mySet', 'myData', 'ScanSet_0') \n\
	        B (aAndC) <= SCAN ('mySet', 'myData', 'ScanSet_1') \n\
	        C (c) <= SCAN ('mySet', 'myData', 'ScanSet_2') \n\
				\n\
		/* extract and hash a from A */ \n\
		AWithAExtracted (a, aExtracted) <= APPLY (A (a), A(a), 'JoinComp_3', 'self_0') \n\
		AHashed (a, hash) <= HASHLEFT (AWithAExtracted (aExtracted), A (a), 'JoinComp_3', '==_2') \n\
				\n\
		/* extract and hash a from B */ \n\
		BWithAExtracted (aAndC, a) <= APPLY (B (aAndC), B (aAndC), 'JoinComp_3', 'attAccess_1') \n\
		BHashedOnA (aAndC, hash) <= HASHRIGHT (BWithAExtracted (a), BWithAExtracted (aAndC), 'JoinComp_3', '==_2') \n\
				\n\
		/* now, join the two of them */ \n\
		AandBJoined (a, aAndC) <= JOIN (AHashed (hash), AHashed (a), BHashedOnA (hash), BHashedOnA (aAndC), 'JoinComp_3') \n\
				\n\
		/* and extract the two atts and check for equality */ \n\
		AandBJoinedWithAExtracted (a, aAndC, aExtracted) <= APPLY (AandBJoined (a), AandBJoined (a, aAndC), 'JoinComp_3', 'self_0') \n\
		AandBJoinedWithBothExtracted (a, aAndC, aExtracted, otherA) <= APPLY (AandBJoinedWithAExtracted (aAndC), \n\
			AandBJoinedWithAExtracted (a, aAndC, aExtracted), 'JoinComp_3', 'attAccess_1') \n\
		AandBJoinedWithBool (aAndC, bool) <= APPLY (AandBJoinedWithBothExtracted (aExtracted, otherA), AandBJoinedWithBothExtracted (aAndC), \n\
			'JoinComp_3', '==_2') \n\
		AandBJoinedFiltered (aAndC) <= FILTER (AandBJoinedWithBool (bool), AandBJoinedWithBool (aAndC), 'JoinComp_3') \n\
				\n\
		/* now get ready to join the strings */ \n\
		AandBJoinedFilteredWithC (aAndC, c) <= APPLY (AandBJoinedFiltered (aAndC), AandBJoinedFiltered (aAndC), 'JoinComp_3', 'attAccess_3') \n\
		BHashedOnC (aAndC, hash) <= HASHLEFT (AandBJoinedFilteredWithC (c), AandBJoinedFilteredWithC (aAndC), 'JoinComp_3', '==_5') \n\
		CwithCExtracted (c, cExtracted) <= APPLY (C (c), C (c), 'JoinComp_3', 'self_4') \n\
		CHashedOnC (c, hash) <= HASHRIGHT (CwithCExtracted (cExtracted), CwithCExtracted (c), 'JoinComp_3', '==_5') \n\
				\n\
		/* join the two of them */ \n\
		BandCJoined (aAndC, c) <= JOIN (BHashedOnC (hash), BHashedOnC (aAndC), CHashedOnC (hash), CHashedOnC (c), 'JoinComp_3') \n\
				\n\
		/* and extract the two atts and check for equality */ \n\
		BandCJoinedWithCExtracted (aAndC, c, cFromLeft) <= APPLY (BandCJoined (aAndC), BandCJoined (aAndC, c), 'JoinComp_3', 'attAccess_3') \n\
		BandCJoinedWithBoth (aAndC, c, cFromLeft, cFromRight) <= APPLY (BandCJoinedWithCExtracted (c), BandCJoinedWithCExtracted (aAndC, c, cFromLeft), \n\
			'JoinComp_3', 'self_4') \n\
		BandCJoinedWithBool (aAndC, bool) <= APPLY (BandCJoinedWithBoth (cFromLeft, cFromRight), BandCJoinedWithBoth (aAndC), \n\
			'JoinComp_3', '==_5') \n\
		last (aAndC) <= FILTER (BandCJoinedWithBool (bool), BandCJoinedWithBool (aAndC), 'JoinComp_3') \n\
				\n\
		/* and here is the answer */ \n\
		almostFinal (result) <= APPLY (last (aAndC), last (), 'JoinComp_3', 'attAccess_7') \n\
		final (result) <= APPLY (almostFinal (result), almostFinal (), 'JoinComp_3', 'deref_8') \n\
	        nothing () <= OUTPUT (final (result), 'outSet', 'myDB', 'SetWriter_4')";
		

	// and create a query object that contains all of this stuff
	Handle <ComputePlan> myPlan = makeObject <ComputePlan> (myTCAPString, myComputations);

	// now, let's pretend that myPlan has been sent over the network, and we want to execute it... first we build 
	// a pipeline into the first join
	void *whereHashTableForBSits;
	PipelinePtr myPipeline = myPlan->buildPipeline (
		std :: string ("B"), /* this is the TupleSet the pipeline starts with */
		std :: string ("BHashedOnA"),     /* this is the TupleSet the pipeline ends with */
		std :: string ("JoinComp_3"), /* and since multiple Computation objects can consume the */
						     /* same tuple set, we apply the Computation as well */

		// this lambda supplies new temporary pages to the pipeline
		[] () -> std :: pair <void *, size_t> {
			void *myPage = malloc (64 * 1024 * 1024);
			return std :: make_pair (myPage, 64 * 1024 *1024);
		},

		// this lambda frees temporary pages that do not contain any important data
		[] (void *page) {
			free (page);
		},

		// and this lambda remembers the page that *does* contain important data...
		// in this simple aggregation, that one page will contain the hash table with
		// all of the aggregated data.
		[&] (void *page) {
			whereHashTableForBSits = page;
			std :: cout << "Remembering where hash for B is located.\n";
			std :: cout << "It is at " << (size_t) whereHashTableForBSits << ".\n";
		}	
	);
	
	// and now, simply run the pipeline and then destroy it!!!
	std :: cout << "\nRUNNING PIPELINE\n";
	myPipeline->run ();
	std :: cout << "\nDONE RUNNING PIPELINE\n";
	myPipeline = nullptr; 

	// now, let's pretend that myPlan has been sent over the network, and we want to execute it... first we build 
	// a pipeline into the first join
	void *whereHashTableForCSits;
	myPipeline = myPlan->buildPipeline (
		std :: string ("C"), /* this is the TupleSet the pipeline starts with */
		std :: string ("CHashedOnC"),     /* this is the TupleSet the pipeline ends with */
		std :: string ("JoinComp_3"), /* and since multiple Computation objects can consume the */
						     /* same tuple set, we apply the Computation as well */

		// this lambda supplies new temporary pages to the pipeline
		[] () -> std :: pair <void *, size_t> {
			void *myPage = malloc (64 * 1024 * 1024);
			return std :: make_pair (myPage, 64 * 1024 *1024);
		},

		// this lambda frees temporary pages that do not contain any important data
		[] (void *page) {
			free (page);
		},

		// and this lambda remembers the page that *does* contain important data...
		// in this simple aggregation, that one page will contain the hash table with
		// all of the aggregated data.
		[&] (void *page) {
			std :: cout << "Getting the hash table for C\n";
			whereHashTableForCSits = page;
		}	
	);
	
	// and now, simply run the pipeline and then destroy it!!!
	std :: cout << "\nRUNNING PIPELINE\n";
	myPipeline->run ();
	std :: cout << "\nDONE RUNNING PIPELINE\n";
	myPipeline = nullptr; 

	// used to store info about the joins
	std :: map <std :: string, ComputeInfoPtr> info;
	info [std :: string ("AandBJoined")] = std :: make_shared <JoinArg> (*myPlan, whereHashTableForBSits);
	info [std :: string ("BandCJoined")] = std :: make_shared <JoinArg> (*myPlan, whereHashTableForCSits);
	
	// now, let's pretend that myPlan has been sent over the network, and we want to execute it... first we build 
	// a pipeline into the first join
	myPipeline = myPlan->buildPipeline (
		std :: string ("A"), /* this is the TupleSet the pipeline starts with */
		std :: string ("final"),     /* this is the TupleSet the pipeline ends with */
		std :: string ("SetWriter_4"), /* and since multiple Computation objects can consume the */
						     /* same tuple set, we apply the Computation as well */

		// this lambda supplies new temporary pages to the pipeline
		[] () -> std :: pair <void *, size_t> {
			void *myPage = malloc (1024 * 1024);
			return std :: make_pair (myPage, 1024 *1024);
		},

		// this lambda frees temporary pages that do not contain any important data
		[] (void *page) {
			free (page);
		},

		// and this lambda remembers the page that *does* contain important data...
		// in this simple aggregation, that one page will contain the hash table with
		// all of the aggregated data.
		[] (void *page) {
			std :: cout << "\nAsked to save page at address " << (size_t) page << "!!!\n";
			Handle <Vector <Handle <String>>> myVec = ((Record <Vector <Handle <String>>> *) page)->getRootObject ();
			std :: cout << "Found that this has " << myVec->size () << " strings in it.\n";
			if (myVec->size () > 0)
				std :: cout << "First one is '" << *((*myVec)[0]) << "'\n";
			free (page);
		},

		info
	);
	
	// and now, simply run the pipeline and then destroy it!!!
	std :: cout << "\nRUNNING PIPELINE\n";
	myPipeline->run ();
	std :: cout << "\nDONE RUNNING PIPELINE\n";
	myPipeline = nullptr; 

	// and be sure to delete the contents of the ComputePlan object... this always needs to be done
	// before the object is written to disk or sent accross the network, so that we don't end up 
	// moving around a C++ smart pointer, which would be bad
	myPlan->nullifyPlanPointer ();

	free (whereHashTableForCSits);
	free (whereHashTableForBSits);

}
