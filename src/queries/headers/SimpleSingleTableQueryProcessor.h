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

#ifndef SINGLE_TABLE_QP_H
#define SINGLE_TABLE_QP_H

#include <memory>

namespace pdb {

class SimpleSingleTableQueryProcessor;
typedef std :: shared_ptr <SimpleSingleTableQueryProcessor> SimpleSingleTableQueryProcessorPtr;

// this pure virtual class is spit out by a simple query class (like the Selection class)... it is then
// used by the system to process queries
//
// The usage pattern is:
//
// 1) Create the processor from a query object
// 2) Initialize it
// 3) Load an input page
// 4) Load an output page
// 5) Call fillNextOutputPage; if it returns true, repeat from step 4
// 6) If more input pages to process, repeat from step 3
// 7) Call finalize
// 8) Load an output page
// 9) Call fillNextOutputPage; if it returns true, repeat from step 8
//
class SimpleSingleTableQueryProcessor {

public:

	// must be called before the query processor is asked to do anything
	virtual void initialize () = 0;

	// loads up another input page to process
	virtual void loadInputPage (void *pageToProcess) = 0;

	// load up another output page to process
	virtual void loadOutputPage (void *pageToWriteTo, size_t numBytesInPage) = 0;

	// attempts to fill the next output page with data.  Returns true if it can.  If it
	// cannot, returns false, and the next call to loadInputPage should be made
	virtual bool fillNextOutputPage () = 0;

	// must be called after all of the input pages have been sent in... this lets the
	// operation know that no more data will be processed, so any "left over" data
	// should be sent out
	virtual void finalize () = 0;

};

}

#endif
