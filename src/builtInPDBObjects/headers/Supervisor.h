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

#ifndef SUPERVISOR_H
#define SUPERVISOR_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"
#include "Employee.h"

//  PRELOAD %Supervisor%

namespace pdb {

class Supervisor : public Object {

private:

        Handle <Employee> me;
        Vector <Handle <Employee>> myGuys;

public:

	ENABLE_DEEP_COPY

        ~Supervisor () {}
        Supervisor () {}

        Supervisor (std :: string name, int age) {
                me = makeObject <Employee> (name, age);
        }

        Handle <Employee> &getEmp (int who) {
                return myGuys[who];
        }

	int getNumEmployees () {
		return myGuys.size ();
	}

        void addEmp (Handle <Employee> &addMe) {
                myGuys.push_back (addMe);
        }

        void print () {
                me->print ();
                std :: cout << "\nPlus have " << myGuys.size () << " employees.\n";
		if (myGuys.size () > 0) {
			std :: cout << "\t (One is ";
			myGuys[0]->print ();
			std :: cout << ")\n";
		}
        }

};

}

#endif
