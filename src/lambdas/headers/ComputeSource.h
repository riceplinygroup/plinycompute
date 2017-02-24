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

#ifndef COMPUTE_SOURCE_H
#define COMPUTE_SOURCE_H

#include "TupleSet.h"

namespace pdb {

class ComputeSource;
typedef std :: shared_ptr <ComputeSource> ComputeSourcePtr;

// this class encapsulates some source of TupleSet objects for processing...
// it might wrap up a hash table that we are iterating over, or it might wrap
// up an on-disk set of objects
class ComputeSource {

public:

	// this gets another tuple set for processing
	virtual TupleSetPtr getNextTupleSet () = 0;

	virtual ~ComputeSource () {}

};

}

#endif
