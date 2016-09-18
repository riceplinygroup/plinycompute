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

#ifndef CHRIS_SELECT_H
#define CHRIS_SELECT_H

#include "Selection.h"
#include "Employee.h"
#include "Supervisor.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "SharedEmployee.h"

using namespace pdb;

class ChrisSelection : public Selection <String, SharedEmployee> {

public:

	ENABLE_DEEP_COPY

	ChrisSelection () {}

	Lambda <bool> getSelection (Handle <SharedEmployee> &checkMe) override {
		return makeLambda (checkMe, [&] () {
			return (*(checkMe->getName ()) != "Joe Johnson48");
		});
	}

	Lambda <Handle <String>> getProjection (Handle <SharedEmployee> &checkMe) override {
		return makeLambda (checkMe, [&] {
			return checkMe->getName ();
		});
	}
};


#endif
