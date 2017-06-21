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

        double salary;
        String department;

	ENABLE_DEEP_COPY

        ~Employee () {}
        Employee () {}
        size_t hash () {
           return name->hash();
        }
        void print () {
                std :: cout << "name is: " << *name << " age is: " << age << " dept is: " << department;
        }

	Handle <String> &getName () {
		return name;
	}

        bool isFrank() {
                return (*name == "Frank");
        }

	int getAge() {
		return age;
	}

	double getSalary () {
		return salary;
	}

        Employee (std :: string nameIn, int ageIn, std :: string department, double salary) : salary (salary), department (department) {
                name = makeObject <String> (nameIn);
                age = ageIn;
        }

	Employee (std :: string nameIn, int ageIn) {
                name = makeObject <String> (nameIn);
                age = ageIn;
		department = "myDept";
		salary = 123.45;	
	}

	bool operator == (Employee &me) const {
		return name == me.name;
	}
};

inline std :: ostream& operator<<(std :: ostream& os, const Handle <Employee>& dt)
{
        //Handle <Employee> *foo = (Handle <Employee> *) &dt;
        //os << "Name is " << *((*foo)->getName ()) << " ";     
        os << (dt->getAge ());
        return os;
}

}

#endif
