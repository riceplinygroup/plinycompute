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

#ifndef SELECTION_COMP
#define SELECTION_COMP

#include "Computation.h"

namespace pdb {

template <class OutputClass, class InputClass>
class SelectionComp : public Computation {

	// the computation returned by this method is called to see if a data item should be returned in the output set
	virtual Lambda <bool> getSelection (Handle <InputClass> &checkMe) = 0;

	// the computation returned by this method is called to perfom a transformation on the input item before it
	// is inserted into the output set
	virtual Lambda <Handle<OutputClass>> getProjection (Handle <InputClass> &checkMe) = 0;

	// calls getProjection and getSelection to extract the lambdas
	void extractLambdas (std :: map <std :: string, GenericLambdaObjectPtr> &returnVal) override {
		int suffix = 0;
		Handle <InputClass> checkMe = nullptr;
		Lambda <bool> selectionLambda = getSelection (checkMe);
		Lambda <Handle <OutputClass>> projectionLambda = getProjection (checkMe);
		selectionLambda.toMap (returnVal, suffix);
		projectionLambda.toMap (returnVal, suffix);
	}	

	// this is a selection computation
        std :: string getComputationType () override {
                return std :: string ("SelectionComp");
        }
};

}

#endif
