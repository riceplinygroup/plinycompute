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
#include "ScanSet.h"
#include "ZA_DepartmentTotal.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"

// to run the aggregate, the system first passes each through the hash operation...
// then the system 
using namespace pdb;

class SillyWrite : public SetWriter <double> {

public:

	ENABLE_DEEP_COPY

	// eventually, this method should be moved into a class that works with the system to 
	// iterate through pages that are pulled from disk/RAM by the system... a programmer
	// should not provide this particular method
	ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &projection, ComputePlan &plan) override {
		return std :: make_shared <VectorSink <double>> (consumeMe, projection);
	}
};

class FinalQuery : public SelectionComp <double, ZA_DepartmentTotal> {

public:

	ENABLE_DEEP_COPY

	Lambda <bool> getSelection (Handle <ZA_DepartmentTotal> &checkMe) override {
		return makeLambdaFromMethod (checkMe, checkSales);
	}

	Lambda <Handle <double>> getProjection (Handle <ZA_DepartmentTotal> &checkMe) override {
		return makeLambdaFromMethod (checkMe, getTotSales);
	}
};

// this points to the location of the hash table that stores the result of the aggregation
void *whereHashTableSits;

// aggregate relies on having two methods in the output type: getKey () and getValue ()
class SillyAgg : public AggregateComp <ZA_DepartmentTotal, Employee, String, double> {

public:
	
	ENABLE_DEEP_COPY

	// the key type must have == and size_t hash () defined
	Lambda <String> getKeyProjection (Handle <Employee> &aggMe) override {
		return makeLambdaFromMember (aggMe, department);	
	}

	// the value type must have + defined
	Lambda <double> getValueProjection (Handle <Employee> &aggMe) override {
		return makeLambdaFromMethod (aggMe, getSalary);	
	}

	// eventually, this method should be moved into a class that works with the system to
	// obtain the memory necessary to create the hash table... a programmer should not
	// be asked to provide this method
	ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &projection, ComputePlan &plan) override {
		return std :: make_shared <HashSink <String, double>> (consumeMe, projection);	
	}

	// same thing... this method is here only for demonstration purposes!!
	ComputeSourcePtr getComputeSource (TupleSpec &outputSchema, ComputePlan &plan) override {
		Handle <Object> myHashTable = ((Record <Object> *) whereHashTableSits)->getRootObject ();
		return std :: make_shared <MapTupleSetIterator <String, double, ZA_DepartmentTotal>> (myHashTable, 24);
	}
};

class SillyQuery : public SelectionComp <Employee, Supervisor> {

public:

	ENABLE_DEEP_COPY

	Lambda <bool> getSelection (Handle <Supervisor> &checkMe) override {
		return makeLambdaFromMethod (checkMe, getSteve) == makeLambdaFromMember (checkMe, me);
	}

	Lambda <Handle <Employee>> getProjection (Handle <Supervisor> &checkMe) override {
		return makeLambdaFromMethod (checkMe, getMe);
	}
};

class SillyRead : public ScanSet <Supervisor> {

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
				       	Handle <Vector <Handle <Supervisor>>> supers = makeObject <Vector <Handle <Supervisor>>> ();
	
					// this will build up the department
					char first = 'A', second = 'B';
					char myString[3];
					myString[2] = 0;	
	
   				    	try {
       		    			    	for (int i = 0; true; i++) {
			
							myString[0] = first;
							myString[1] = second;
	
							// this will allow us to cycle through "AA", "AB", "AC", "BA", ...
							first++;
							if (first == 'D') {
								first = 'A';
								second++;
								if (second == 'D') 
									second = 'A';
							}
	
	       	       			         	Handle <Supervisor> super = makeObject <Supervisor> ("Steve Stevens", 20 + (i % 29), std :: string (myString), i * 34.4);
       		          			      	supers->push_back (super);
       		          			      	for (int j = 0; j < 10; j++) {
		
       		              			          	Handle <Employee> temp;
								if (i % 2 == 0)
									temp = makeObject <Employee> ("Steve Stevens", 20 + ((i + j) % 29), std :: string (myString), j * 3.54);
								else
									temp = makeObject <Employee> ("Albert Albertson", 20 + ((i + j) % 29), std :: string (myString), j * 3.54);
       		               			         	(*supers)[i]->addEmp (temp);
       		               			 	}
						}
					} catch (NotEnoughSpace &e) {}
						getRecord (supers);
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
	Handle <Computation> myScanSet = makeObject <SillyRead> ();
	Handle <Computation> myFilter = makeObject <SillyQuery> ();
	Handle <Computation> myAgg = makeObject <SillyAgg> ();
	Handle <Computation> myFinalFilter = makeObject <FinalQuery> ();
	Handle <Computation> myWrite = makeObject <SillyWrite> ();
	
	// put them in the list of computations
	myComputations.push_back (myScanSet);
	myComputations.push_back (myFilter);
	myComputations.push_back (myAgg);
	myComputations.push_back (myFinalFilter);
	myComputations.push_back (myWrite);

	// now we create the TCAP string
        String myTCAPString =
	       "inputData (in) <= SCAN ('mySet', 'myData', 'ScanSet_0') \n\
		inputWithAtt (in, att) <= APPLY (inputData (in), inputData (in), 'SelectionComp_1', 'methodCall_1') \n\
		inputWithAttAndMethod (in, att, method) <= APPLY (inputWithAtt (in), inputWithAtt (in, att), 'SelectionComp_1', 'attAccess_2') \n\
		inputWithBool (in, bool) <= APPLY (inputWithAttAndMethod (att, method), inputWithAttAndMethod (in), 'SelectionComp_1', '==_0') \n\
		filteredInput (in) <= FILTER (inputWithBool (bool), inputWithBool (in), 'SelectionComp_1') \n\
		projectedInputWithPtr (out) <= APPLY (filteredInput (in), filteredInput (), 'SelectionComp_1', 'methodCall_4') \n\
		projectedInput (out) <= APPLY (projectedInputWithPtr (out), projectedInputWithPtr (), 'SelectionComp_1', 'deref_3') \n\
		aggWithKeyWithPtr (out, key) <= APPLY (projectedInput (out), projectedInput (out), 'AggregationComp_2', 'attAccess_1') \n\
		aggWithKey (out, key) <= APPLY (aggWithKeyWithPtr (key), aggWithKeyWithPtr (out), 'AggregationComp_2', 'deref_0') \n\
		aggWithValue (key, value) <= APPLY (aggWithKey (out), aggWithKey (key), 'AggregationComp_2', 'methodCall_2') \n\
		agg (aggOut) <=	AGGREGATE (aggWithValue (key, value), 'AggregationComp_2') \n\
		checkSales (aggOut, isSales) <= APPLY (agg (aggOut), agg (aggOut), 'SelectionComp_3', 'methodCall_0') \n\
		justSales (aggOut, isSales) <= FILTER (checkSales (isSales), checkSales (aggOut), 'SelectionComp_3') \n\
		final (result) <= APPLY (justSales (aggOut), justSales (), 'SelectionComp_3', 'methodCall_1') \n\
	        nothing () <= OUTPUT (final (result), 'outSet', 'myDB', 'SetWriter_4')";

	// and create a query object that contains all of this stuff
	Handle <ComputePlan> myPlan = makeObject <ComputePlan> (myTCAPString, myComputations);

	// now, let's pretend that myPlan has been sent over the network, and we want to execute it... first we build 
	// a pipeline into the aggregation operation
	PipelinePtr myPipeline = myPlan->buildPipeline (
		std :: string ("inputData"), /* this is the TupleSet the pipeline starts with */
		std :: string ("aggWithValue"),     /* this is the TupleSet the pipeline ends with */
		std :: string ("AggregationComp_2"), /* and since multiple Computation objects can consume the */
						     /* same tuple set, we apply the Computation as well */

		// this lambda supplies new temporary pages to the pipeline
		[] () -> std :: pair <void *, size_t> {
			void *myPage = malloc (64 * 1024);
			return std :: make_pair (myPage, 64 * 1024);
		},

		// this lambda frees temporary pages that do not contain any important data
		[] (void *page) {
			free (page);
		},

		// and this lambda remembers the page that *does* contain important data...
		// in this simple aggregation, that one page will contain the hash table with
		// all of the aggregated data.
		[] (void *page) {
			whereHashTableSits = page;
		}	
	);
	
	// and now, simply run the pipeline and then destroy it!!!
	std :: cout << "\nRUNNING PIPELINE\n";
	myPipeline->run ();
	myPipeline = nullptr; 

	// after the destruction of the pointer, the current allocation block is messed up!

	// at this point, the hash table should be filled up...	so now we can build a second pipeline that covers
	// the second half of the aggregation
	myPipeline = myPlan->buildPipeline (
		std :: string ("agg"), /* this is the TupleSet the pipeline starts with */
		std :: string ("final"), /* this is the TupleSet the pipeline ends with */
		std :: string ("SetWriter_4"), /* and the Computation object that the pipeline ends with */

		// this lambda supplies new temporary pages to the pipeline
		[] () -> std :: pair <void *, size_t> {
			void *myPage = malloc (64 * 1024);
			return std :: make_pair (myPage, 64 * 1024);
		},

		// this lambda frees temporary pages that do not contain any important data
		[] (void *page) {
			free (page);
		},

		// and this lambda writes out the contents of a page that is one of the final outputs...
		// in this simple aggregation, that one page will contain the hash table with
		// all of the aggregated data.
		[] (void *page) {
			std :: cout << "\nAsked to save page at address " << (size_t) page << "!!!\n";
			std :: cout << "This should have a bunch of doubles on it... let's see.\n";
			Handle <Vector <Handle <double>>> myHashTable = ((Record <Vector <Handle <double>>> *) page)->getRootObject ();
			for (int i = 0; i < myHashTable->size (); i++) {
				std :: cout << "Got double " << *((*myHashTable)[i]) << "\n";
			}
			free (page);
		}	

	);

	// run and then kill the pipeline
	std :: cout << "\nRUNNING PIPELINE\n";
	myPipeline->run ();
	myPipeline = nullptr;

	// and be sure to delete the contents of the ComputePlan object... this always needs to be done
	// before the object is written to disk or sent accross the network, so that we don't end up 
	// moving around a C++ smart pointer, which would be bad
	myPlan->nullifyPlanPointer ();

}
