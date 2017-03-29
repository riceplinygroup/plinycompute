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
#ifndef TEST_60_H
#define TEST_60_H

//by Jia, Mar 2017
//to test TCAP string parsing

#include "TupleSetJobStage.h"
#include "SetIdentifier.h"
#include "Computation.h"
#include "Handle.h"
#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "WriteUserSet.h"
#include "SelectionComp.h"
#include "ScanUserSet.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "PDBString.h"
#include "Employee.h"

// to run the aggregate, the system first passes each through the hash operation...
// then the system 
using namespace pdb;

class MyWriteStringSet : public WriteUserSet <String> {

public:

	ENABLE_DEEP_COPY


        MyWriteStringSet () {
        }


        MyWriteStringSet (std :: string dbName, std :: string setName) {
            this->setOutput(dbName, setName);
        }


};

class MyEmployeeSelection : public SelectionComp <String, Employee> {

public:

	ENABLE_DEEP_COPY

	Lambda <bool> getSelection (Handle <Employee> &checkMe) override {
		return makeLambdaFromMethod (checkMe, isFrank);
	}

	Lambda <Handle <String>> getProjection (Handle <Employee> &checkMe) override {
		return makeLambdaFromMethod (checkMe, getName);
	}
};


class MyScanEmployeeSet : public ScanUserSet <Employee> {

public:

	ENABLE_DEEP_COPY

        MyScanEmployeeSet () {
        }

        MyScanEmployeeSet (std :: string dbName, std :: string setName) {
            this->setOutput(dbName, setName);
        }


};


int main () {
	
	// this is the object allocation block where all of this stuff will reside
       	makeObjectAllocatorBlock (1024 * 1024, true);

	// here is the list of computations
	Vector <Handle <Computation>> myComputations;
	
	// create all of the computation objects
	Handle <Computation> myScanSet = makeObject <MyScanEmployeeSet> ("chris_db", "chris_set");
	Handle <Computation> myFilter = makeObject <MyEmployeeSelection> ();
	Handle <Computation> myWrite = makeObject <MyWriteStringSet> ("chris_db", "output_set1");
	
	// put them in the list of computations
	myComputations.push_back (myScanSet);
	myComputations.push_back (myFilter);
	myComputations.push_back (myWrite);

	// now we create the TCAP string
        String myTCAPString =
               "inputData (in) <= SCAN ('chris_set', 'chris_db', 'ScanUserSet_0') \n\
                checkFrank (in, isFrank) <= APPLY (inputData (in), inputData (in), 'SelectionComp_1', 'methodCall_0') \n\
                justFrank (in, isFrank) <= FILTER (checkFrank(isFrank), checkFrank(in), 'SelectionComp_1') \n\
                projectedInputWithPtr (out) <= APPLY (justFrank (in), justFrank (), 'SelectionComp_1', 'methodCall_2') \n\
                projectedInput (out) <= APPLY (projectedInputWithPtr (out), projectedInputWithPtr (), 'SelectionComp_1', 'deref_1') \n\
                nothing() <= OUTPUT (projectedInput (out), 'output_set1', 'chris_db', 'WriteUserSet_2')";
	// and create a query object that contains all of this stuff
	Handle <ComputePlan> myPlan = makeObject <ComputePlan> (myTCAPString, myComputations);
        LogicalPlanPtr logicalPlan = myPlan->getPlan();
        AtomicComputationList computationList = logicalPlan->getComputations();
        std :: cout << "to print logical plan:" << std :: endl;        
        std :: cout << computationList << std :: endl;

        PipelinePtr myPipeline = myPlan->buildPipeline (
                std :: string ("inputData"), /* this is the TupleSet the pipeline starts with */
                std :: string ("projectedInput"), /* this is the TupleSet the pipeline ends with */
                std :: string ("WriteUserSet_2"), /* and the Computation object that the pipeline ends with */

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
                        free (page);
                }

        );






        Handle<TupleSetJobStage> jobStage = makeObject<TupleSetJobStage>(0);
        jobStage->setComputePlan(myPlan, "inputData", "final", "WriteUserSet_2");
        std :: string sourceSpecifier = "ScanUserSet_0";
        std :: cout << "to config source" << std :: endl;
        Handle<Computation> sourceComputation = myPlan->getPlan()->getNode(sourceSpecifier).getComputationHandle();
        Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(sourceComputation);
        Handle<SetIdentifier> source = makeObject<SetIdentifier>(scanner->getDatabaseName(), scanner->getSetName());
        jobStage->setSourceContext(source);
        std :: cout << "to config sink" << std :: endl;
        std :: string sinkSpecifier = "WriteUserSet_2";
        Handle<Computation> sinkComputation = myPlan->getPlan()->getNode(sinkSpecifier).getComputationHandle();
        Handle<WriteUserSet<Object>> writer = unsafeCast<WriteUserSet<Object>, Computation>(sinkComputation);
        Handle<SetIdentifier> sink = makeObject<SetIdentifier>(writer->getDatabaseName(), writer->getSetName());
        jobStage->setSinkContext(sink);
}

#endif
