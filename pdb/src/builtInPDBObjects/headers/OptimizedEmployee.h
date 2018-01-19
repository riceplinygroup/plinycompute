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

#ifndef OPTIMIZED_EMPLOYEE_H
#define OPTIMIZED_EMPLOYEE_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"

//  PRELOAD %OptimizedEmployee%

namespace pdb {

class OptimizedEmployee : public Object {

public:
    String name;
    int age;
    double salary;
    String department;

    ENABLE_DEEP_COPY

    ~OptimizedEmployee() {}
    OptimizedEmployee() {}
    size_t hash() {
        return name.hash();
    }
    void print() {
        std::cout << "name is: " << name << " age is: " << age;
    }

    String& getName() {
        return name;
    }

    bool isFrank() {
        return (name == "Frank");
    }

    int getAge() {
        return age;
    }

    double getSalary() {
        return salary;
    }

    OptimizedEmployee(std::string nameIn, int ageIn, std::string department, double salary)
        : name(nameIn), age(ageIn), salary(salary), department(department) {}

    OptimizedEmployee(std::string nameIn, int ageIn) : name(nameIn), age(ageIn) {
        department = "myDept";
        salary = 123.45;
    }

    bool operator==(OptimizedEmployee& me) {
        return name == me.name;
    }
};
}

#endif
