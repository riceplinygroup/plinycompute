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

#ifndef LAMBDA_H
#define LAMBDA_H

#include <memory>
#include <vector>
#include <functional>
#include "Literal.h"
#include "Object.h"
#include "Handle.h"

namespace pdb {

template <typename Out>
class LambdaType;

// this is the lamda type... queries are written by supplying code that
// creates these objects
template <typename Out> class Lambda {

	std :: shared_ptr <LambdaType <Out>> myData;

public:

	Lambda () {}

	// returns a string giving the entire "parse tree" making up this lambda
	std :: string toString () {
		return myData->toString ();
	}

	// returns the function that actucally computes this lambda
	std :: function <Out ()> getFunc () {
		return myData->getFunc ();
	}

	// copy constructor
	Lambda (std :: shared_ptr <LambdaType <Out>> myDataIn) {
		myData = myDataIn;
	}

	// get a list of pointers to all of the input parameters that are used by this lambda
	// this is going to be useful for compilation, so that we know what parameters are
	// referenced by what lambdas
	std :: vector <Handle <Object> *> &getParams () {
		return myData->getParams ();
	}

	// make a lambda with no parameters out of a function
	Lambda (std :: function <Out ()> funcArg) {
		myData = std :: make_shared <Literal <Out>> (funcArg);
	}

	// make a lambda with the specified parameter out of a function
	template <class T1, class... T2>
    	Lambda  (Handle <T1> &firstParam, T2... rest) : Lambda (rest...) {
		Handle <Object> *temp = (Handle <Object> *) &firstParam;
		myData->addParam (temp);
	}

    bool operator==(const Lambda<Out>& rhs)
    {
        return myData == rhs.myData;
    }
};

// this helper function allows us to easily create Lambda objects
template <typename T, typename F>
auto makeLambda(T&& val, F&& func) {
    return Lambda <decltype(func())> (val, func);
}

template <typename F>
auto makeLambda(F&& func) {
    return Lambda <decltype(func())> (func);
}

}

#endif
