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
#ifndef K_MEANS_DATA_COUNT_AGGREGATE_H
#define K_MEANS_DATA_COUNT_AGGREGATE_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "ClusterAggregateComp.h"
#include "KMeansDoubleVector.h"
#include "limits.h"
#include "SumResult.h"

/* The aggregation that counts the total number of data points */
using namespace pdb;

class KMeansDataCountAggregate
    : public ClusterAggregateComp<SumResult, KMeansDoubleVector, int, int> {

public:
    ENABLE_DEEP_COPY

    KMeansDataCountAggregate() {}

    Lambda<int> getKeyProjection(Handle<KMeansDoubleVector> aggMe) override {
        return makeLambda(aggMe, [](Handle<KMeansDoubleVector>& aggMe) { return 0; });
    }

    Lambda<int> getValueProjection(Handle<KMeansDoubleVector> aggMe) override {
        return makeLambda(aggMe, [](Handle<KMeansDoubleVector>& aggMe) { return 1; });
    }
};


#endif
