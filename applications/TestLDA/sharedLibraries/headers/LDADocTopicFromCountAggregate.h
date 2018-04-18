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
#ifndef LDA_DOC_TOPIC_FROM_COUNT_AGGREGATE_H
#define LDA_DOC_TOPIC_FROM_COUNT_AGGREGATE_H

// by Shangyu, May 2017

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "AggregateComp.h"
#include "IntIntVectorPair.h"
#include "LDADocWordTopicCount.h"
#include "PDBVector.h"


using namespace pdb;


class LDADocTopicFromCountAggregate
    : public AggregateComp<IntIntVectorPair, LDADocWordTopicCount, int, Vector<int>> {

private:
    int numTopic;

public:
    ENABLE_DEEP_COPY

    LDADocTopicFromCountAggregate() {}
    LDADocTopicFromCountAggregate(int fromTopic) {
        this->numTopic = fromTopic;
    }


    // the key type must have == and size_t hash () defined
    Lambda<int> getKeyProjection(Handle<LDADocWordTopicCount> aggMe) override {
        return makeLambda(aggMe,
                          [](Handle<LDADocWordTopicCount>& aggMe) { return aggMe->getDoc(); });
    }

    // the value type must have + defined
    Lambda<Vector<int>> getValueProjection(Handle<LDADocWordTopicCount> aggMe) override {
        return makeLambda(aggMe, [&](Handle<LDADocWordTopicCount>& aggMe) {
            Handle<Vector<int>> result = makeObject<Vector<int>>(this->numTopic, this->numTopic);
            result->fill(0);
            (*result)[aggMe->getTopic()] = aggMe->getCount();
            return *result;
        });
    }
};


#endif
