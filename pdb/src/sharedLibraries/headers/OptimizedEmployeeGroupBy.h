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
#ifndef OPTIMIZED_EMPLOYEE_GROUPBY_H
#define OPTIMIZED_EMPLOYEE_GROUPBY_H

// by Jia, Mar 2017

#include "AggregateComp.h"
#include "OptimizedEmployee.h"
#include "OptimizedSupervisor.h"
#include "LambdaCreationFunctions.h"
#include "OptimizedDepartmentEmployees.h"

using namespace pdb;

class OptimizedEmployeeGroupBy
    : public AggregateComp<OptimizedDepartmentEmployees,
                                  OptimizedSupervisor,
                                  String,
                                  Map<String, Vector<OptimizedEmployee>>> {

public:
    ENABLE_DEEP_COPY

    OptimizedEmployeeGroupBy() {}

    // the below constructor is NOT REQUIRED
    // user can also set output later by invoking the setOutput (std :: string dbName, std :: string
    // setName)  method
    OptimizedEmployeeGroupBy(std::string dbName, std::string setName) {
        this->setOutput(dbName, setName);
    }


    // the key type must have == and size_t hash () defined
    Lambda<String> getKeyProjection(Handle<OptimizedSupervisor> aggMe) override {
        return makeLambdaFromMethod(aggMe, getDepartment);
    }

    // the value type must have + defined
    Lambda<Map<String, Vector<OptimizedEmployee>>> getValueProjection(
        Handle<OptimizedSupervisor> aggMe) override {
        return makeLambda(aggMe, [](Handle<OptimizedSupervisor>& aggMe) {

            Map<String, Vector<OptimizedEmployee>> ret;
            Vector<OptimizedEmployee>& vec = ret[aggMe->getName()];
            Vector<OptimizedEmployee>& myGuys = aggMe->myGuys;
            size_t mySize = myGuys.size();
            vec.resize(mySize);
            for (int i = 0; i < mySize; i++) {
                vec.push_back(myGuys[i]);
            }
            return ret;
        });
    }
};


#endif
