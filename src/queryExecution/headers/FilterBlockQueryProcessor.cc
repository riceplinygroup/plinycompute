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

#ifndef FILTER_BLOCK_QUERY_PROCESSOR_CC
#define FILTER_BLOCK_QUERY_PROCESSOR_CC

#include "InterfaceFunctions.h"
#include "FilterBlockQueryProcessor.h"

namespace pdb {

template <class Output, class Input>
FilterBlockQueryProcessor <Output, Input> :: FilterBlockQueryProcessor (Selection <Output, Input> &forMe) {

	// get a copy of the lambdas for query processing
	filterPred = forMe.getSelection (inputObject);
	finalized = false;
}

template <class Output, class Input>
FilterBlockQueryProcessor <Output, Input> :: FilterBlockQueryProcessor (Lambda <bool> filterPred) {

        // get a copy of the lambdas for filter processing
        this->filterPred = filterPred;
        finalized = false;
}

// no need to do anything
template <class Output, class Input>
void FilterBlockQueryProcessor <Output, Input> :: initialize () {
	filterFunc = filterPred.getFunc ();
	finalized = false;
}

// loads up another input page to process
template <class Output, class Input>
void FilterBlockQueryProcessor <Output, Input> :: loadInputBlock (Handle<GenericBlock> inputBlock)
{
        this->inputBlock = inputBlock;
        this->batchSize = this->inputBlock->getBlock().size();
	posInInput = 0;
}

// load up another output page to process
template <class Output, class Input>
Handle<GenericBlock> FilterBlockQueryProcessor <Output, Input> :: loadOutputBlock () {

	// and here's where we write the ouput to
	this->outputBlock = makeObject <GenericBlock> ();
        return this->outputBlock;
}

template <class Output, class Input>
bool FilterBlockQueryProcessor <Output, Input> :: fillNextOutputBlock () {
		
	Vector <Handle <Input>> &myInVec = (inputBlock->getBlock());
	Vector <Handle <Input>> &myOutVec = (outputBlock->getBlock());

	// if we are finalized, see if there are some left over records
	if (finalized) {
		return false;
	}

	// we are not finalized, so process the page
	try {
		int vecSize = myInVec.size ();
		for (; posInInput < vecSize; posInInput++) {
			inputObject = myInVec[posInInput];
			if (filterFunc ()) {
				myOutVec.push_back (inputObject);
			}
		}	
		return false;

	} catch (NotEnoughSpace &n) {
                if (this->context != nullptr) {
                       PipelineContextPtr context = this->context;
		       getRecord (context->outputVec);
                       context->setOutputFull(true);
                }
		return true;
	}
}

// must be called repeately after all of the input pages have been sent in...
template <class Output, class Input>
void FilterBlockQueryProcessor <Output, Input> :: finalize () {
	finalized = true;
}

// must be called before freeing the memory in output page
template <class Output, class Input>
void FilterBlockQueryProcessor <Output, Input> :: clearOutputBlock () {
        outputBlock = nullptr;
}

// must be called before freeing the memory in input page
template <class Output, class Input>
void FilterBlockQueryProcessor <Output, Input> :: clearInputBlock () {
        inputBlock = nullptr;
}
}

#endif
