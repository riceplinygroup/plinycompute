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

#ifndef OPTIMIZED_DEPARTMENT_EMPLOYEES_H
#define OPTIMIZED_DEPARTMENT_EMPLOYEES_H

#include "Object.h"
#include "PDBString.h"
#include "OptimizedEmployee.h"
#include "PDBMap.h"
#include "PDBVector.h"

//  PRELOAD %OptimizedDepartmentEmployees%

namespace pdb {

class OptimizedDepartmentEmployees : public Object {

public:
    OptimizedDepartmentEmployees() {}

    ~OptimizedDepartmentEmployees() {}

    String departmentName;  // the name of the department
    Map<String, Vector<OptimizedEmployee>>
        employees;  // for each supervisor in this department, the list of employees
    ENABLE_DEEP_COPY


    String& getKey() {
        return departmentName;
    }

    Map<String, Vector<OptimizedEmployee>>& getValue() {
        return employees;
    }

    void print() {
        std::cout << "Department: " << departmentName << std::endl;
        auto iter = employees.begin();
        while (iter != employees.end()) {
            std::cout << "----Supervisor: " << (*iter).key << std::endl;
            std::cout << "----NumEmployees: " << (*iter).value.size() << std::endl;
            for (int i = 0; i < (*iter).value.size(); i++) {
                std::cout << i << ": ";
                (((*iter).value)[i]).print();
                std::cout << ";";
            }
            std::cout << std::endl;
            ++iter;
        }
    }
};


inline Map<String, Vector<OptimizedEmployee>>& operator+(
    Map<String, Vector<OptimizedEmployee>>& lhs, Map<String, Vector<OptimizedEmployee>>& rhs) {
    auto iter = rhs.begin();
    while (iter != rhs.end()) {
        // const String& myKey = (*iter).key;
        String myKey = (*iter).key;
        if (lhs.count(myKey) == 0) {
            try {
                lhs[myKey] = (*iter).value;
            } catch (NotEnoughSpace& n) {
                // std :: cout << "not enough space when inserting new pair" << std :: endl;
                lhs.setUnused(myKey);
                throw n;
            }
        } else {
            Vector<OptimizedEmployee>& vec = lhs[myKey];
            size_t mySize = vec.size();
            // size_t mySize = lhs[myKey].size();
            size_t otherSize = (*iter).value.size();


            // std :: cout << "mySize is " << mySize << " and otherSize is " << otherSize << std ::
            // endl;
            for (size_t i = mySize; i < mySize + otherSize; i++) {
                try {

                    // lhs[myKey].push_back((*iter).value[i-mySize]);
                    vec.push_back((*iter).value[i - mySize]);
                } catch (NotEnoughSpace& n) {

                    // std :: cout << i << ": not enough space when updating value by pushing back:
                    // " << lhs[myKey].size() << std :: endl;
                    throw n;
                }
            }
            // std :: cout << "now my size is " << (*lhs)[myKey].size() << std :: endl;
        }
        ++iter;
    }
    return lhs;
}
}

#endif
