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
#ifndef COUNT_CUSTOMER_AGGERGATION_H
#define COUNT_CUSTOMER_AGGERGATION_H

#include "AggregateComp.h"
#include "LambdaCreationFunctions.h"

#include "Customer.h"
#include "SumResult.h"

using namespace pdb;

// template <class OutputClass, class InputClass, class KeyClass, class ValueClass>

class CountCustomer : public AggregateComp<SumResult, Customer, int, int> {

public:
    ENABLE_DEEP_COPY

    CountCustomer() {}

    // the below constructor is NOT REQUIRED
    // user can also set output later by invoking the setOutput (std :: string dbName, std :: string
    // setName)  method
    CountCustomer(std::string dbName, std::string setName) {
        this->setOutput(dbName, setName);
    }


    // the key type must have == and size_t hash () defined
    Lambda<int> getKeyProjection(Handle<Customer> aggMe) override {
        return makeLambda(aggMe, [](Handle<Customer>& aggMe) { return 0; });
    }

    // the value type must have + defined
    Lambda<int> getValueProjection(Handle<Customer> aggMe) override {
        return makeLambda(aggMe, [](Handle<Customer>& aggMe) { return 1; });
    }
};
#endif
