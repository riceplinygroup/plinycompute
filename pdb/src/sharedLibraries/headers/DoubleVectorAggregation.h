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
#ifndef DOUBLE_VECTOR_AGG_H
#define DOUBLE_VECTOR_AGG_H

// by Jia, May 2017

#include "AggregateComp.h"
#include "DoubleVector.h"
#include "DoubleVectorResult.h"
#include "LambdaCreationFunctions.h"


using namespace pdb;

class DoubleVectorAggregation
    : public AggregateComp<DoubleVectorResult, DoubleVector, int, DoubleVector> {

public:
    ENABLE_DEEP_COPY

    DoubleVectorAggregation() {}

    // the below constructor is NOT REQUIRED
    // user can also set output later by invoking the setOutput (std :: string dbName, std :: string
    // setName)  method
    DoubleVectorAggregation(std::string dbName, std::string setName) {
        this->setOutput(dbName, setName);
    }


    // the key type must have == and size_t hash () defined
    Lambda<int> getKeyProjection(Handle<DoubleVector> aggMe) override {
        return makeLambda(aggMe, [](Handle<DoubleVector>& aggMe) { return 0; });
    }

    // the value type must have + defined
    Lambda<DoubleVector> getValueProjection(Handle<DoubleVector> aggMe) override {
        return makeLambda(aggMe, [](Handle<DoubleVector>& aggMe) { return *aggMe; });
    }
};


#endif
