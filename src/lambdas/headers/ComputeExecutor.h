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

#ifndef COMP_EXEC_H
#define COMP_EXEC_H

#include "TupleSet.h"

namespace pdb {

// a nice little typedef to shared_ptrs to CompExecutor objects
class ComputeExecutor;
typedef std :: shared_ptr <ComputeExecutor> ComputeExecutorPtr;

// this is a ComputeExecutor.  By definition, it has one method that takes an input a TupleSet, and
// then somehow transofrms it to create a new TupleSet (a TupleSet is a column-oriented list of
// tuples). 
class ComputeExecutor {

public:
        
	// precess a tuple set
	virtual TupleSetPtr process (TupleSetPtr input) = 0;

        // JiaNote: add below function for debugging
        virtual std :: string getType() {  return "UNKNOWN"; };

};

}

#endif
