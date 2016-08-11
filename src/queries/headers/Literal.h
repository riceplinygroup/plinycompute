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

#ifndef LITERAL_H
#define LITERAL_H

#include "LambdaType.h"

namespace pdb {

// this is used to store an actual, user-supplied function
template <typename Out> class Literal : public LambdaType <Out> {

	std :: vector <Handle <Object> *> params;
	std :: function <Out ()> myFunc;

public:

	std :: string toString () {
		if (params.size () == 0) {
			return "function with no params";
		} else if (params.size () == 1) {
			std :: string returnVal = "(function of param ";
			returnVal += std :: to_string ((size_t) params[0]) + ")";
			return returnVal;
		} else {
			std :: string returnVal = "(function of params ";
			returnVal += std :: to_string ((size_t) params[0]);
			for (auto i : params) {
				returnVal += ", " + std :: to_string((size_t) i);
			}
			returnVal += ")";
			return returnVal;
		}
	}

	~Literal () {}

	void addParam (Handle <Object> *addMe) {
		params.push_back (addMe);
	}

	Literal (std :: function <Out ()> funcArg) {
		myFunc = funcArg;
	}

	FuncType getType () {
		return FuncType :: UserDefined;
	}

	std :: function <Out ()> getFunc () {
		return myFunc;
	}

	std :: vector <Handle <Object> *> getParams () {
		return params;
	}
};

}

#endif
