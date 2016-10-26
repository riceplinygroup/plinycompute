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

#ifndef FILTER_QUERY_PROCESSOR_H
#define FILTER_QUERY_PROCESSOR_H

#include "SimpleSingleTableQueryProcessor.h"
#include "UseTemporaryAllocationBlock.h"
#include "InterfaceFunctions.h"
#include "PDBVector.h"
#include "Selection.h"
#include "Handle.h"

namespace pdb {

template <class Output, class Input>
class FilterQueryProcessor : public SimpleSingleTableQueryProcessor {

private:

	// this is where we write the results
	UseTemporaryAllocationBlockPtr blockPtr; 

	// this is where the input objects are put
	Handle <Input> inputObject;

	// this is the list of input objects
	Handle <Vector <Handle <Input>>> inputVec;

	// this is where we are in the input
	size_t posInInput;

	// this is where the output objects are put
	Handle <Vector <Handle <Input>>> outputVec;

	// and here are the lamda objects used to proces the input vector
	SimpleLambda <bool> filterPred;

	// and here are the actual functions
	std :: function <bool ()> filterFunc;
	
	// tells whether we have been finalized
	bool finalized;

public:

	FilterQueryProcessor (Selection <Output, Input> &forMe);
        FilterQueryProcessor (SimpleLambda <bool> filterPred);
	// the standard interface functions
	void initialize () override;
        void loadInputPage (void *pageToProcess) override;
        void loadOutputPage (void *pageToWriteTo, size_t numBytesInPage) override;
        bool fillNextOutputPage () override;
        void finalize () override;
        void clearOutputPage () override;
        void clearInputPage () override;
};

}

#include "FilterQueryProcessor.cc"

#endif
