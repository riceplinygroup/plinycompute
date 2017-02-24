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

#ifndef COMPUTATION_H
#define COMPUTATION_H

#include "Object.h"
#include "Lambda.h"
#include "ComputeSource.h"
#include "ComputeSink.h"
#include <map>

namespace pdb {

class ComputePlan;

// all nodes in a user-supplied computation are descended from this
class Computation : public Object {

public:

	// this is implemented by the actual computation object... as the name implies, it is used
	// to extract the lambdas from the computation
	virtual void extractLambdas (std :: map <std :: string, GenericLambdaObjectPtr> &returnVal) {}

	// if this particular computation can be used as a compute source in a pipeline, this
	// method will return the compute source object associated with the computation...
	//
	// In the general case, this method accepts the logical plan that this guy is a part of,
	// as well as the actual TupleSpec that this guy is supposed to produce, and then returns 
	// a pointer to a ComputeSource object that can actually produce TupleSet objects corresponding
	// to that particular TupleSpec
	virtual ComputeSourcePtr getComputeSource (TupleSpec &produceMe, ComputePlan &plan) {return nullptr;}

	// likewise, if this particular computation can be used as a compute sink in a pipeline, this
	// method will return the compute sink object associated with the computation.  It requires the
	// TupleSpec that should be processed, as well as the projection of that TupleSpec that will
	// be put into the sink
	virtual ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &projection, ComputePlan &plan) {return nullptr;}

	// returns the type of this Computation
	virtual std :: string getComputationType () = 0;
};

}

#endif
