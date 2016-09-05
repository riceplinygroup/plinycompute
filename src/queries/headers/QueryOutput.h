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

#ifndef QUERY_OUTPUT_H
#define QUERY_OUTPUT_H

#include "Query.h"
#include "OutputIterator.h"
#include "TypeName.h"

namespace pdb {

template <class OutType>
class QueryOutput : public Query <OutType> {

public:

	OutputIterator <OutType> begin () {
		return OutputIterator <OutType> ();
	}	

	OutputIterator <OutType> end () {
		return OutputIterator <OutType> ();
	}	

	virtual int getNumInputs () {
		return 1;
	}

	virtual std :: string getIthInputType (int i) {
		if (i != 0) {
			return "Bad index";
		}
		return getTypeName <OutType> ();
	}
};

}

#endif
