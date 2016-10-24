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
#include <queue>

namespace pdb {

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

	// this is the source of data in the pipeline
	TupleSetIterator dataSource;	

	// here is our pipeline
	std :: vector <QueryExecutorPtr> pipeline; 

	// and here is all of the pages we've not yet written back
	std :: queue <std :: pair <void *, size_t>> unwrittenPages;

public:

	// the first argument is a function to call that gets a new output page...
	// the second arguement is a function to call that deals with a full output page
	// the third argument is the iterator that will create TupleSets to process
	Pipeline (std :: function <std :: pair <void *, size_t> ()> getNewPage, std :: function <void (void *, size_t)> writeBackPage,
		TupleSetIterator &dataSource) :
		getNewPage (getNewPage), writeBackPage (writeBackPage), dataSource (dataSource) {}

	// adds a stage to the pipeline
	void addStage (QueryExecutorPtr addMe) {
		pipeline.push_back (addMe);
	}
	
	~Pipeline () {

		// kill all of the pipeline stages
		std :: vector <QueryExecutorPtr> newPipeline;
		pipeline = newPipeline;

		// write back all of the pages
		writeBackPages ();

		if (unwrittenPages.size () != 0)
			std :: cout << "This is bad: in destructor for pipeline, still some pages with objects!!\n";
	}

	// writes back any unwritten pages
	void writeBackPages () {

		// write back any pages
		while (unwrittenPages.size () > 0) {
			if (getNumObjectsInAllocatorBlock (unwrittenPages.front ().first) == 0) {
				writeBackPage (unwrittenPages.front ().first, unwrittenPages.front ().second);
				unwrittenPages.pop ();
			} else {
				break;
			}
		}
	}

	// runs the pipeline
	void run (int whichColToOutput) {

		// get the page to write to
		auto outputPage = getNewPage ();
		makeObjectAllocatorBlock (outputPage.first, outputPage.second, true);

		// get the actual handle to write to
		Handle <Vector <Handle <Object>>> outputVector = nullptr;

		// and here is the chunk
		TupleSetPtr curChunk;

		// while there is still data
		while ((curChunk = dataSource.getNextTupleSet ()) != nullptr) {
			
			// go through all of the pipeline stages
			for (QueryExecutorPtr &q : pipeline) {
				
				std :: cout << "Moving to next pipe stage.\n";

				try { 
					curChunk = q->process (curChunk);
				} catch (NotEnoughSpace &n) {

					// and get a new page
					getRecord (outputVector);
					unwrittenPages.push (outputPage);
					outputPage = getNewPage ();
					makeObjectAllocatorBlock (outputPage.first, outputPage.second, true);

					// then try again
					curChunk = q->process (curChunk);
				}
			}

			std :: cout << "Done pushing through pipeline.\n";

			// write back any unwritten pages
			writeBackPages ();

			std :: cout << "Writing output.\n";

			try {

				if (outputVector == nullptr) {
					outputVector = curChunk->getOutputVector (whichColToOutput);
				}
				curChunk->writeOutColumn (whichColToOutput, outputVector, true);

			} catch (NotEnoughSpace &n) {

				// again, we ran out of RAM here, so write back the page and then create a new output page
				getRecord (outputVector);
				unwrittenPages.push (outputPage);
				Handle <Vector <Handle <Object>>> temp = ((Record <Vector <Handle <Object>>> *) outputPage.first)->getRootObject ();
				outputPage = getNewPage ();
				makeObjectAllocatorBlock (outputPage.first, outputPage.second, true);

				// and again, try to write back the output
				outputVector = curChunk->getOutputVector (whichColToOutput);
				curChunk->writeOutColumn (whichColToOutput, outputVector, false);
			}

			// lastly, write back the output page
			writeBackPages ();
		}

		// write the results
		getRecord (outputVector);
		unwrittenPages.push (outputPage);
		writeBackPages ();
	}
};

}

#endif
