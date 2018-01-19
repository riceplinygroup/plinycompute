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
#ifndef SIMPLE_GROUPBY_H
#define SIMPLE_GROUPBY_H

// by Jia, Mar 2017

#include "ClusterAggregateComp.h"
#include "Employee.h"
#include "Supervisor.h"
#include "LambdaCreationFunctions.h"
#include "DepartmentEmployeeAges.h"

using namespace pdb;

class SimpleGroupBy : public ClusterAggregateComp<DepartmentEmployeeAges,
                                                 Supervisor,
                                                 String,
                                                 Handle<Map<String, Vector<int>>>> {

public:
    ENABLE_DEEP_COPY

    SimpleGroupBy() {}

    // the below constructor is NOT REQUIRED
    // user can also set output later by invoking the setOutput (std :: string dbName, std :: string
    // setName)  method
    SimpleGroupBy(std::string dbName, std::string setName) {
        this->setOutput(dbName, setName);
    }


    // the key type must have == and size_t hash () defined
    Lambda<String> getKeyProjection(Handle<Supervisor> aggMe) override {
        return makeLambdaFromMethod(aggMe, getDepartment);
    }

    // the value type must have + defined
    Lambda<Handle<Map<String, Vector<int>>>> getValueProjection(Handle<Supervisor> aggMe) override {
        return makeLambda(aggMe, [](Handle<Supervisor>& aggMe) {

            Handle<Map<String, Vector<int>>> ret = makeObject<Map<String, Vector<int>>>();
            String myKey = *(aggMe->getName());
            (*ret)[myKey].resize((aggMe->myGuys).size());
            for (int i = 0; i < (aggMe->myGuys).size(); i++) {
                (*ret)[myKey].push_back((aggMe->myGuys)[i]->getAge());
            }
            return ret;
        });
    }
};


#endif
