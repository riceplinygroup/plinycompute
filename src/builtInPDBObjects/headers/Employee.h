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

#ifndef EMPLOYEE_H
#define EMPLOYEE_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"

//  PRELOAD %Employee%

namespace pdb {

class Employee : public Object {
        Handle <String> name;
        int age;

public:

	ENABLE_DEEP_COPY

        ~Employee () {}
        Employee () {}

        void print () {
                std :: cout << "name is: " << *name << " age is: " << age;
        }

	Handle <String> &getName () {
		return name;
	}

        Employee (std :: string nameIn, int ageIn) {
                name = makeObject <String> (nameIn);
                age = ageIn;
        }
};

}

#endif
