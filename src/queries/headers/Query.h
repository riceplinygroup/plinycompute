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

#ifndef QUERY_H
#define QUERY_H

#include "Handle.h"
#include "Object.h"
#include "QueryRoot.h"
#include "PDBVector.h"
#include "TypeName.h"

namespace pdb {

// this is the basic query type... all queries returning OutType derive from this class
template <typename OutType> 
class Query : public QueryRoot {

public:

	// gets the name of this output type
	std :: string getOutputType () {
		return getTypeName <OutType> ();
	}

	// gets the number of intputs to this query type
	virtual int getNumInputs () = 0;

	// gets the name of the i^th input type...
	virtual std :: string getIthInputType (int i) = 0;

	// gets a handle to the i^th input to this query, which is also a query
	Handle <Query> getIthInput (int i) {
		if (inputs != nullptr)
			return inputs[i];
		else
			return nullptr;
	}

	// sets the i^th input to be the output of a specific query... returns
	// true if this is OK, false if it is not
	bool setInput (int whichSlot, Handle <QueryRoot> toMe) {
		
		// set the array of inputs if it is a nullptr
		if (inputs == nullptr) {
			inputs = makeObject <Vector <Handle <QueryRoot>>> (getNumInputs ());
			for (int i = 0; i < getNumInputs (); i++) {
				inputs->push_back (nullptr);
			}
		}

		if (whichSlot >= getNumInputs ()) {

			// make sure the output type of the guy we are accepting meets the input type
			if (getIthInputType (whichSlot) != unsafeCast <Query <Object>> (toMe)->getOutputType ()) {
				return false;
			}

			(*inputs)[whichSlot] = toMe;
			return true;
		}

		return false;
	}

	// sets the 0^th slot
	bool setInput (Handle <Query> toMe) {
		return setInput (0, toMe);
	}

private:

	Handle <Vector <Handle <QueryRoot>>> inputs;

};

}

#endif
