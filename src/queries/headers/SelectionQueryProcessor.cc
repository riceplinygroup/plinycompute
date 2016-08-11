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

#ifndef SELECTION_QUERY_PROCESSOR_CC
#define SELECTION_QUERY_PROCESSOR_CC

#include "InterfaceFunctions.h"
#include "SelectionQueryProcessor.h"

namespace pdb {

template <class Input, class Output>
SelectionQueryProcessor <Input, Output> :: SelectionQueryProcessor (Selection <Input, Output> &forMe) {

	// get a copy of the lambdas for query processing
	selectionPred = forMe.getSelection (inputObject);
	projection = forMe.getProjection (inputObject);
	finalized = false;
}

// no need to do anything
template <class Input, class Output>
void SelectionQueryProcessor <Input, Output> :: initialize () {
	selectionFunc = selectionPred.getFunc ();
	projectionFunc = projection.getFunc ();
}

// loads up another input page to process
template <class Input, class Output>
void SelectionQueryProcessor <Input, Output> :: loadInputPage (void *pageToProcess) {
	Record <Vector <Handle <Input>>> *myRec = (Record <Vector <Handle <Input>>> *) pageToProcess;
	inputVec = myRec->getRootObject ();
	posInInput = 0;
}

// load up another output page to process
template <class Input, class Output>
void SelectionQueryProcessor <Input, Output> :: loadOutputPage (void *pageToWriteTo, size_t numBytesInPage) {
	makeObjectAllocatorBlock (pageToWriteTo, numBytesInPage, true);
	outputVec = makeObject <Vector <Handle <Output>>> (10);
}

template <class Input, class Output>
bool SelectionQueryProcessor <Input, Output> :: fillNextOutputPage () {
		
	Vector <Handle <Input>> &myInVec = *(inputVec);
	Vector <Handle <Output>> &myOutVec = *(outputVec);

	// if we are finalized, see if there are some left over records
	if (finalized && myOutVec.size () > 0) {
		getRecord (outputVec);
		return true;
	} else if (finalized) {
		return false;
	}

	// we are not finalized, so process the page
	try {
		int vecSize = myInVec.size ();
		for (; posInInput < vecSize; posInInput++) {
			inputObject = myInVec[posInInput];
			if (selectionFunc ()) {
				myOutVec.push_back (projectionFunc ());	
			}
		}	

		return false;

	} catch (NotEnoughSpace &n) {
		
		getRecord (outputVec);
		return true;
	}
}

// must be called repeately after all of the input pages have been sent in...
template <class Input, class Output>
void SelectionQueryProcessor <Input, Output> :: finalize () {
	finalized = true;
}

}

#endif
