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

#ifndef PROJECTION_BLOCK_QUERY_PROCESSOR_H
#define PROJECTION_BLOCK_QUERY_PROCESSOR_H

#include "SimpleSingleTableQueryProcessor.h"
#include "UseTemporaryAllocationBlock.h"
#include "InterfaceFunctions.h"
#include "PDBVector.h"
#include "Selection.h"
#include "Handle.h"
#include "GenericBlock.h"

namespace pdb {

template <class Output, class Input> 
class ProjectionQueryProcessor : public BlockQueryProcessor {

private:

	// this is where the input objects are put
	Handle <Input> inputObject;

	// this is the list of input objects
	Handle <GenericBlock> inputBlock;

	// this is where we are in the input
	size_t posInInput;

	// this is where the output objects are put
	Handle <GenericBlock> outputBlock;

	// and here are the lamda objects used to proces the input vector
	Lambda <Handle <Output>> projection;

	// and here are the actual functions
	std :: function <Handle <Output> ()> projectionFunc;
	
	// tells whether we have been finalized
	bool finalized;

        // output batch size
        size_t batchSize;

public:

	ProjectionQueryProcessor (Selection <Output, Input> &forMe);
        ProjectionQueryProcessor (Lambda <Handle<Output>> projection);
	// the standard interface functions
	void initialize () override;
        void loadInputBlock (Handle<GenericBlock> block) override;
        Handle<GenericBlock> loadOutputPage () override;
        bool fillNextOutputBlock () override;
        void finalize () override;
        void clearOutputBlock () override;
        void clearInputBlock () override;
};

}

#include "ProjectionBlockQueryProcessor.cc"

#endif
