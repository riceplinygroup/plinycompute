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

#ifndef SET_SCAN_H
#define SET_SCAN_H

#include <vector>
#include <functional>
#include <iostream>
#include <memory>

#include "Query.h"

namespace pdb {

// this is the basic selection type... users derive from this class in order to write
// a selection query
template <typename Out> 
class SetScan : public Query <Out> {

public:

	// gets the number of inputs
	int getNumInputs () {return 0;}

        // gets the name of the i^th input type...
        std :: string getIthInputType (int i) {
		 return "I have no inputs!!";
	}

};

}

#endif
