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

#ifndef CHECK_EMPLOYEES_H
#define CHECK_EMPLOYEES_H

#include "Selection.h"
#include "Employee.h"
#include "Supervisor.h"
#include "PDBVector.h"
#include "PDBString.h"

// PRELOAD %CheckEmployee%

namespace pdb {

// this silly little class accepts a Supervisor, and rejects that supervisor
// if he doesn't manage anyone other than "nameToExclude".  If he does manage
// someone other than "nameToExclude", then the query will return all of the
// employees for the supervisor that don't have that name 
class CheckEmployee : public Selection <Vector <Handle <Employee>>, Supervisor> {

private:

	String nameToExclude;

public:

	ENABLE_DEEP_COPY

	CheckEmployee () {}

	CheckEmployee (std :: string &nameToExcludeIn) {
		nameToExclude = nameToExcludeIn;
	}
	
	Lambda <bool> getSelection (Handle <Supervisor> &checkMe) override {
		return makeLambda (checkMe, [&] () -> bool {
			int numEmployees = checkMe->getNumEmployees ();
			for (int i = 0; i < numEmployees; i++) {
				if (*(checkMe->getEmp (i)->getName ()) != nameToExclude) {
					return true;
				}
			}
			return false;
		});
	}

	Lambda <Handle <Vector <Handle <Employee>>>> getProjection (Handle <Supervisor> &checkMe) override {
		return makeLambda (checkMe, [&] {
			Handle <Vector <Handle <Employee>>> returnVal = makeObject <Vector <Handle <Employee>>> (10);
			int numEmployees = checkMe->getNumEmployees ();
			for (int i = 0; i < numEmployees; i++) {
				if (*(checkMe->getEmp (i)->getName ()) != nameToExclude) {
					returnVal->push_back (checkMe->getEmp (i));	
				}
			}

			return returnVal;
		});
	}
};

}
#endif
