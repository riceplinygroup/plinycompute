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

#ifndef EXECUTION_PIPELINE_H
#define EXECUTION_PIPELINE_H

#include "TupleSetIterator.h"
#include "UseTemporaryAllocationBlock.h"
#include <queue>
#include "PangeaStorageServer.h"

#include "BaseQuery.h"

namespace pdb {


// this is used to buffer unwritten pages
struct My_MemoryHolder {

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

	My_MemoryHolder (std :: pair <void *, size_t> buildMe) {
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

typedef std :: shared_ptr <My_MemoryHolder> My_MemoryHolderPtr;

// this is a prototype for the pipeline
class ExecutionPipeline {

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

	// and here is all of the pages we've not yet written back
	std :: queue <My_MemoryHolderPtr> unwrittenPages;

	pdb :: PangeaStorageServerPtr storage;

public:

	// the first argument is a function to call that gets a new output page...
	// the second arguement is a function to call that deals with a full output page
	// the third argument is the iterator that will create TupleSets to process
	ExecutionPipeline (std :: function <std :: pair <void *, size_t> ()> getNewPage, 
		std :: function <void (void *, size_t)> writeBackPage,
		std :: function <void (void *, size_t)> discardPage,
		pdb :: PangeaStorageServerPtr storageIn) :
		getNewPage (getNewPage), writeBackPage (writeBackPage), discardPage (discardPage), storage(storageIn) {}

	// adds a stage to the pipeline
	void addStage (QueryExecutorPtr addMe) {
		pipeline.push_back (addMe);
	}

	void addInputStage() {
		addStage(nullptr);
	}

	void addComputationsStage(ComputationPtr computation, TupleSpec &inputSpec) {
		std :: string name = computation->getComputationName ();
		if (name == "Apply") {
			addStage (lambdaMap [std::dynamic_pointer_cast<ApplyLambda>(computation)->getLambdaToApply()]->getExecutor (inputSpec, computation->getInput (), computation->getProjection ()));
		} else if (name == "Filter") {
			addStage (std :: make_shared <FilterQueryExecutor> (inputSpec, computation->getInput (), computation->getProjection ()));
		}
	}

	void build(shared_ptr<BaseQuery> query, LogicalPlan &plan) {
		int identifier = 0, nodeId = 0;
		query->toMap(lambdaMap, identifier);
		std :: cout << "Are " << lambdaMap.size () << " items in the map!!!.\n";
		for (auto &a : lambdaMap) {
			std :: cout << a.first << "\n";
		}

		std :: cout << "Trying to build the plan. " << std :: endl;
		std :: cout << plan << std :: endl;

		// run dfs on the plan to build the pipeline.
		std :: vector <std :: string> inputs;
		plan.getInputs().pushOutputs(inputs);
		auto allComputations = plan.getComputations();
		auto outputList = plan.getOutputs();

		for (auto a : inputs) {
			std::cout << "Input: " << a << std::endl;
			std :: vector <TupleSpec> inputTupleSets;
			
			Input &myInput = plan.getInputs ().getProducer (a);
			nodeid2Inputs[nodeId] = myInput;
			
			std :: vector <ComputationPtr> stack;
			std :: vector <int> inputNodeIds;
			addInputStage();
			auto computations = allComputations.getConsumingComputations(a);
			for (auto c : computations) {
				stack.push_back(c);
				inputNodeIds.push_back(nodeId);
				inputTupleSets.push_back(myInput.getOutput ());
				std :: cout << c->getOutputName() << std::endl;
			}
			++nodeId;

			// Add all the stages to produce outputs that are results of computations
			// based on this input
			while (!stack.empty()) {
				auto c = stack[stack.size() - 1];
				// moving on with this computation's outputs, add stage for this computation.
				addComputationsStage(c, inputTupleSets[inputTupleSets.size() - 1]);

				// Note: inputs are special nodes that are not part of the computations,
				// each computation creates a new nodeId. If a nodeId is also in the nodeId to Inputs
				// map, then that computation that creates this node needs to use the inputset specified
				// as the input
				// the first nodeid of every pipelined workflow is always the same as the stage number
				// in the pipeline vector
				nodeid2Inputnodeid[nodeId] = inputNodeIds[inputNodeIds.size() - 1];

				stack.pop_back();
				inputTupleSets.pop_back();
				inputNodeIds.pop_back();

				// push the computations that consumes the output of this computation onto the stack.
				if (outputList.isOutput(c->getOutputName())) {
					std :: cout << "This one is output." << c->getOutputName() << std :: endl;
					nodeid2Outputs[nodeId] = outputList.getConsumers(c->getOutputName());
				} else {
					computations = allComputations.getConsumingComputations(c->getOutputName());
					for (auto next : computations) {
						stack.push_back(next);
						inputNodeIds.push_back(nodeId);
						inputTupleSets.push_back(c->getOutput ());
						std :: cout << next->getOutputName() << std::endl;
					}
				}
				
				++nodeId;
			}
		}
	}
	
	~ExecutionPipeline () {

		// kill all of the pipeline stages
		while (pipeline.size ())
			pipeline.pop_back ();

		// first, reverse the queue so we go oldest to newest
		// this ensures that everything is deleted in the reverse order that it was created
		std :: vector <My_MemoryHolderPtr> reverser;
		while (unwrittenPages.size () > 0) {
			reverser.push_back (unwrittenPages.front ());
			unwrittenPages.pop ();
		}

		while (reverser.size () > 0) {
			unwrittenPages.push (reverser.back ());
			reverser.pop_back ();
		}

		// write back all of the pages
		cleanPages (999999999);
		storage->stopFlushConsumerThreads();
		if (unwrittenPages.size () != 0)
			std :: cout << "This is bad: in destructor for pipeline, still some pages with objects!!\n";
	}

	// writes back any unwritten pages
	void cleanPages (int iteration) {

		// take care of getting rid of any pages... but only get rid of those from two iterations ago...
		// pages from the last iteration may still have pointers into them 
		while (unwrittenPages.size () > 0 && iteration > unwrittenPages.front ()->iteration + 1) {
			
			// in this case, the page did not have any output data written to it... it only had
			// intermediate results, and so we will just discard it
			if (unwrittenPages.front ()->outputVector == nullptr) {
				if (getNumObjectsInAllocatorBlock (unwrittenPages.front ()->location) != 0) {

					// this is bad... there should not be any objects here because this memory
					// chunk does not store an output vector
					std :: cout << "This is bad!!  Now did I find a page with objects??\n";
				} else {
					discardPage (unwrittenPages.front ()->location, 
						unwrittenPages.front ()->numBytes);	
					unwrittenPages.pop ();
				}

			// in this case, the page DID have some data written to it
			} else {
			
				// ask for the page to be written back
				// and force the reference count for this guy to go to zero
				unwrittenPages.front ()->outputVector.emptyOutContainingBlock ();

				writeBackPage (unwrittenPages.front ()->location, 
					unwrittenPages.front ()->numBytes);	

				std :: cout << "Done killing this page.\n";

				// and get ridda him
				unwrittenPages.pop ();
			}
		}

		std :: cout << "Size was " << unwrittenPages.size () << "\n";
	}

	// runs the pipeline
	void run (int whichColToOutput) {

	
		// first, we make a really small allocation block so that we can restore the existing
		// one when we are done
		const UseTemporaryAllocationBlock tempBlock {1024};	

		// get the actual handle to write to
		Handle <Vector <Handle <Object>>> outputVector = nullptr;

		// and the current RAM we are writing to
		My_MemoryHolderPtr myRAM = std :: make_shared <My_MemoryHolder> (getNewPage ());	

		// and here is the chunk
		TupleSetPtr curChunk;

		
		for (auto it = nodeid2Inputs.begin(); it != nodeid2Inputs.end(); ++it) {
			int inputNodeId = it->first;
			auto nextIt = it;
			++nextIt;
			int nextInputNodeId = (nextIt == nodeid2Inputs.end()) ? pipeline.size() : nextIt->first;

			Input &nextInputSource = it->second;
			std :: cout << "Loading next input set: " << nextInputSource << std :: endl;

			SetPtr inputSet = storage->getSet(std :: pair <std :: string, std :: string> (nextInputSource.getDbName(), nextInputSource.getSetName()));
			storage->getCache()->pin(inputSet, MRU, Read);
        	vector<PageIteratorPtr> * pageIters = inputSet->getIterators();

        	int numIterators = pageIters->size();
		    for (int i = 0; i < numIterators; i++) {
		        PageIteratorPtr iter = pageIters->at(i);
		        while (iter->hasNext()){
		            PDBPagePtr inputPage = iter->next();
		            
		            if (inputPage != nullptr) {

		            	auto *temp = (Record <Vector <Handle <Object>>> *) inputPage->getBytes ();
						auto myGuys = temp->getRootObject ();

						TupleSetIterator dataSource (myGuys, 1784);

						int iteration = 0;
						while ((curChunk = dataSource.getNextTupleSet ()) != nullptr) {

							std :: map <int, TupleSetPtr> nodeid2TupleSetPtr;

							nodeid2TupleSetPtr[inputNodeId] = curChunk;

							// go through all of the pipeline stages
							// the first stage is always a nullptr (input stage), so skipped
							for (int stage = inputNodeId + 1; stage < nextInputNodeId; ++stage) {

								QueryExecutorPtr &q = pipeline[stage];

								try {

									curChunk = q->process (nodeid2TupleSetPtr[nodeid2Inputnodeid[stage]]);
									nodeid2TupleSetPtr[stage] = curChunk;
									
								} catch (NotEnoughSpace &n) {
									// and get a new page
									myRAM->setIteration (iteration);
									++iteration;
									std :: cout << "Pushing!!\n";
									unwrittenPages.push (myRAM);
									myRAM = std :: make_shared <My_MemoryHolder> (getNewPage ());

									// then try again
									curChunk = q->process (nodeid2TupleSetPtr[nodeid2Inputnodeid[stage]]);
									nodeid2TupleSetPtr[stage] = curChunk;
								}

								if (nodeid2Outputs.count(stage) != 0) {
									std :: cout << "Encountered sink node" << std :: endl;
									// TODO

								}
							}

							std :: cout << "Done pushing through pipeline for one chunk of input set.\n";
							
							try {

								if (outputVector == nullptr) {
									outputVector = myRAM->getOutputVector (curChunk, whichColToOutput);
								}
								curChunk->writeOutColumn (whichColToOutput, outputVector, true);

							} catch (NotEnoughSpace &n) {

								// again, we ran out of RAM here, so write back the page and then create a new output page
								myRAM->setIteration (iteration);
								std :: cout << "Pushing!!\n";
								unwrittenPages.push (myRAM);
								myRAM = std :: make_shared <My_MemoryHolder> (getNewPage ());

								// and again, try to write back the output
								outputVector = myRAM->getOutputVector (curChunk, whichColToOutput);
								curChunk->writeOutColumn (whichColToOutput, outputVector, false);
							}

							// lastly, write back all of the output pages
							iteration++;
							cleanPages (iteration);

						}

						// write the results
						std :: cout << "Cleaning!!\n";

						// have to set to nullptr to be sure the only reference is in the list of pages to be cleaned
						outputVector = nullptr;

						// set the iteration
						myRAM->setIteration (iteration);

						// and remember the page
						unwrittenPages.push (myRAM);


		            }

		            inputPage->unpin();
		        }
			}

        	inputSet->unpin();
		}

	}

};

}

#endif

