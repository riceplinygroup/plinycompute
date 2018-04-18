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
#ifndef GMM_DATA_COUNT_AGGREGATE_H
#define GMM_DATA_COUNT_AGGREGATE_H

#include "AggregateComp.h"
#include "DoubleVector.h"
#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "SumResult.h"
#include "limits.h"

using namespace pdb;

// GmmDataCountAggregate can be used to count the size of the input dataset
class GmmDataCountAggregate
    : public AggregateComp<SumResult, DoubleVector, int, int> {

public:
  ENABLE_DEEP_COPY

  GmmDataCountAggregate() {}

  // the key type must have == and size_t hash () defined
  Lambda<int> getKeyProjection(Handle<DoubleVector> aggMe) override {
    return makeLambda(aggMe, [](Handle<DoubleVector> &aggMe) { return 0; });
  }

  // the value type must have + defined
  Lambda<int> getValueProjection(Handle<DoubleVector> aggMe) override {
    return makeLambda(aggMe, [](Handle<DoubleVector> &aggMe) { return 1; });
  }
};

#endif
