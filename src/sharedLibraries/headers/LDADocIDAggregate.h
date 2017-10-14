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
#ifndef LDA_DOC_ID_AGGREGATE_H
#define LDA_DOC_ID_AGGREGATE_H

// by Shangyu, May 2017

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "ClusterAggregateComp.h"
#include "limits.h"
#include "SumResult.h"
#include "LDADocument.h"


using namespace pdb;


class LDADocIDAggregate : public ClusterAggregateComp<SumResult, LDADocument, int, int> {

public:
    ENABLE_DEEP_COPY

    LDADocIDAggregate() {}


    // the key type must have == and size_t hash () defined
    Lambda<int> getKeyProjection(Handle<LDADocument> aggMe) override {
        return makeLambda(aggMe, [](Handle<LDADocument>& aggMe) { return (int)aggMe->getDoc(); });
    }

    // the value type must have + defined
    Lambda<int> getValueProjection(Handle<LDADocument> aggMe) override {
        return makeLambda(aggMe, [](Handle<LDADocument>& aggMe) { return 1; });
    }
};


#endif
