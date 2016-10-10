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
#include "SharedEmployee.h"

namespace pdb {

template <class Output, class Input>
FilterBlockQueryProcessor <Output, Input> :: ~FilterBlockQueryProcessor () {
       //std :: cout << "running FilterBlockQueryProcessor destructor" << std :: endl;
       this->inputBlock = nullptr;
       this->outputBlock = nullptr;
       this->context = nullptr;
       this->inputObject = nullptr;
}


template <class Output, class Input>
FilterBlockQueryProcessor <Output, Input> :: FilterBlockQueryProcessor (Selection <Output, Input> &forMe) {

	// get a copy of the lambdas for query processing
	filterPred = forMe.getProjectionSelection (inputObject);
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
Handle<GenericBlock> & FilterBlockQueryProcessor <Output, Input> :: loadOutputBlock () {

	// and here's where we write the ouput to
	this->outputBlock = makeObject <GenericBlock> ();
        return this->outputBlock;
}

template <class Output, class Input>
bool FilterBlockQueryProcessor <Output, Input> :: fillNextOutputBlock () {
        //std :: cout << "Filter processor is running" << std :: endl;		
	Vector <Handle <Output>> &myInVec = (inputBlock->getBlock());
	Vector <Handle <Output>> &myOutVec = (outputBlock->getBlock());

	// if we are finalized, see if there are some left over records
	if (finalized) {
		return false;
	}

	// we are not finalized, so process the page
	try {
		int vecSize = myInVec.size ();
                //std :: cout << "input object num =" << vecSize << std :: endl;
		for (; posInInput < vecSize; posInInput++) {
			inputObject = myInVec[posInInput];
                        //std :: cout << "posInInput=" << posInInput << std :: endl;
			if (filterFunc ()) {
                                //std :: cout << "to push back posInInput=" << posInInput << std :: endl;
				myOutVec.push_back (inputObject);
                                //std :: cout << "push back posInInput=" << posInInput << std :: endl;
			}
		}
                //std :: cout << "Filter processor processed an input block" << std :: endl;	
		return false;

	} catch (NotEnoughSpace &n) {
                std :: cout << "Filter processor consumed current page" << std :: endl;
                if (this->context != nullptr) {
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
        inputObject = nullptr;
}
}

#endif
