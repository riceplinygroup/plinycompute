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

#ifndef SHARED_EMPLOYEE_C
#define SHARED_EMPLOYEE_C

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"
#include "GetVTable.h"

class PrintableObject : public pdb :: Object {

public:

	virtual void print () = 0;
};

class SharedEmployee : public PrintableObject {

        pdb :: Handle <pdb :: String> name;
        int age;

public:

	ENABLE_DEEP_COPY

        ~SharedEmployee () {}
        SharedEmployee () {}

        void print () override {
                std :: cout << "name is: " << *name << " age is: " << age;
        }

	pdb :: Handle <pdb :: String> &getName () {
		return name;
	}

        SharedEmployee (std :: string nameIn, int ageIn) {
                name = pdb :: makeObject <pdb :: String> (nameIn);
                age = ageIn;
        }
};

GET_V_TABLE (SharedEmployee)

#endif
