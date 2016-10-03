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

#ifndef BLOCK_QUERY_PROCESSOR_H
#define BLOCK_QUERY_PROCESSOR_H

#include <memory>
#include "GenericBlock.h"

namespace pdb {

class BlockQueryProcessor;
typedef std :: shared_ptr <BlockQueryProcessor> BlockQueryProcessorPtr;

// this pure virtual class is spit out by a simple query class (like the Selection class)... it is then
// used by the system to process queries
//
class BlockQueryProcessor {

public:

	// must be called before the query processor is asked to do anything
	virtual void initialize () = 0;

	// loads up another input block to read input data
	virtual void loadInputBlock (Handle<GenericBlock> block) = 0;

	// load up another output block to write output data
	virtual Handle<GenericBlock> loadOutputBlock (size_t batchSize) = 0;

	// attempts to fill the next output block with data.  Returns true if it can.  If it
	// cannot, returns false, and the next call to loadInputBlock should be made
	virtual bool fillNextOutputBlock () = 0;

	// must be called after all of the input pages have been sent in
	virtual void finalize () = 0;

        // must be called before free the data in output page
        virtual void clearOutputBlock() = 0;

        // must be called before free the data in input page
        virtual void clearInputBlock() = 0;

};

}

#endif
