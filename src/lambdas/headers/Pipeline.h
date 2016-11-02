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

#ifndef PIPELINE_H
#define PIPELINE_H

#include "TupleSetIterator.h"
#include "UseTemporaryAllocationBlock.h"
#include <queue>

namespace pdb {

// this is used to buffer unwritten pages
struct MemoryHolder {

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

	MemoryHolder (std :: pair <void *, size_t> buildMe) {
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

typedef std :: shared_ptr <MemoryHolder> MemoryHolderPtr;

// this is a prototype for the pipeline
class Pipeline {

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

	// this is the source of data in the pipeline
	TupleSetIterator dataSource;	

	// here is our pipeline
	std :: vector <QueryExecutorPtr> pipeline; 


	// and here is all of the pages we've not yet written back
	std :: queue <MemoryHolderPtr> unwrittenPages;

public:

	// the first argument is a function to call that gets a new output page...
	// the second arguement is a function to call that deals with a full output page
	// the third argument is the iterator that will create TupleSets to process
	Pipeline (std :: function <std :: pair <void *, size_t> ()> getNewPage, 
		std :: function <void (void *, size_t)> writeBackPage,
		std :: function <void (void *, size_t)> discardPage,
		TupleSetIterator &dataSource) :
		getNewPage (getNewPage), writeBackPage (writeBackPage), discardPage (discardPage), dataSource (dataSource) {}

	// adds a stage to the pipeline
	void addStage (QueryExecutorPtr addMe) {
		pipeline.push_back (addMe);
	}
	
	~Pipeline () {

		// kill all of the pipeline stages
		while (pipeline.size ())
			pipeline.pop_back ();

		// first, reverse the queue so we go oldest to newest
		// this ensures that everything is deleted in the reverse order that it was created
		std :: vector <MemoryHolderPtr> reverser;
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
		const UseTemporaryAllocationBlock temp {1024};	

		// get the actual handle to write to
		Handle <Vector <Handle <Object>>> outputVector = nullptr;

		// and the current RAM we are writing to
		MemoryHolderPtr myRAM = std :: make_shared <MemoryHolder> (getNewPage ());	

		// and here is the chunk
		TupleSetPtr curChunk;

		// the iteration counter
		int iteration = 0;

		// while there is still data
		while ((curChunk = dataSource.getNextTupleSet ()) != nullptr) {
			
			// go through all of the pipeline stages
			for (QueryExecutorPtr &q : pipeline) {
				
				std :: cout << "Moving to next pipe stage.\n";

				try { 
					curChunk = q->process (curChunk);

				} catch (NotEnoughSpace &n) {

					// and get a new page
					myRAM->setIteration (iteration);
					std :: cout << "Pushing!!\n";
					unwrittenPages.push (myRAM);
					myRAM = std :: make_shared <MemoryHolder> (getNewPage ());

					// then try again
					curChunk = q->process (curChunk);
				}
			}

			std :: cout << "Done pushing through pipeline.\n";

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
				myRAM = std :: make_shared <MemoryHolder> (getNewPage ());

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
};

}

#endif
