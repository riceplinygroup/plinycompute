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

#ifndef NEW_EXECUTION_PIPELINE_H
#define NEW_EXECUTION_PIPELINE_H

//JiaNote: deprecated


#include "UseTemporaryAllocationBlock.h"
#include <queue>
#include "PangeaStorageServer.h"
#include "BaseQuery.h"
#include "PipelineDummyTestServer.h"
/*
namespace pdb {


// this is used to buffer unwritten pages
struct NewMemoryHolder {

	// the output vector that this guy stores
	Handle <Vector <Handle <Object>>> outputVector;

	// his memory
	void *location;
	size_t numBytes;

	// the iteration where he was last written...
	// we use this beause we canot delete 
	int iteration;

	void setIteration (int iterationIn) {
		if (outputVector != nullptr)
			getRecord (outputVector);
		iteration = iterationIn;	
	}

	NewMemoryHolder (std :: pair <void *, size_t> buildMe) {
		location = buildMe.first;
		numBytes = buildMe.second;
		makeObjectAllocatorBlock (location, numBytes, true);
		outputVector = nullptr;
	}

	Handle <Vector <Handle <Object>>> &getOutputVector (TupleSetPtr curChunk, int whichColToOutput) {
		outputVector = curChunk->getOutputVector (whichColToOutput);
		return outputVector;
	}
};

typedef std :: shared_ptr <NewMemoryHolder> NewMemoryHolderPtr;

// this is a prototype for the pipeline
class NewExecutionPipeline {

private:

	// this is a function that the pipeline calls to obtain a new page to
	// write output to.  The function returns a pair.  The first item in
	// the pair is the page, the second is the number of bytes in the page
	std :: function <std :: pair <void *, size_t> ()> getNewPage;

	// this is a function that the pipeline calls to write back a page.
	// The first arg is the page to write back (and free), and the second
	// is the size of the page
	std :: function <void (void *, size_t)> writeBackPage;

	// this is a function that the pipieline calls to free a page, without
	// writing it back (because it has no useful data)
	std :: function <void (void *, size_t)> discardPage;

	// here is our pipeline
	std :: vector <QueryExecutorPtr> pipeline; 

	// this is the map of nodes in the plan that are obtained from an Input set
	std :: map <int, Input> nodeid2Inputs;

	// map of nodes in the plan to their corresponding input node ids. the values
	// should always be smaller than the keys.
	std :: map <int, int> nodeid2Inputnodeid;

	std :: map <int, std :: vector<Output> > nodeid2Outputs;

	std :: map <std :: string, GenericLambdaObjectPtr> lambdaMap;

	PipelineDummyTestServer * server;

	// and here is all of the pages we've not yet written back
	std :: queue <NewMemoryHolderPtr> unwrittenPages;

public:

	// the first argument is a function to call that gets a new output page...
	// the second arguement is a function to call that deals with a full output page
	// the third argument is the iterator that will create TupleSets to process
	NewExecutionPipeline (std :: function <std :: pair <void *, size_t> ()> getNewPage, 
		std :: function <void (void *, size_t)> writeBackPage,
		std :: function <void (void *, size_t)> discardPage,
		PipelineDummyTestServer * serverIn) :
		getNewPage (getNewPage), writeBackPage (writeBackPage), discardPage (discardPage), server(serverIn) {}

	// adds a stage to the pipeline
	void addStage (QueryExecutorPtr addMe) {
		pipeline.push_back (addMe);
	}

	void addInputStage() {
		addStage(nullptr);
	}

	void addComputationsStage(ComputationPtr computation, TupleSpec &inputSpec);

	void build(Handle<Vector <Handle <BaseQuery>>> queriesPtr, LogicalPlan &plan);
	
	~NewExecutionPipeline ();

	// writes back any unwritten pages
	void cleanPages (int iteration);

	// runs the pipeline
	void run (int whichColToOutput);

};

}
*/
#endif
