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
#ifndef LDA_WORD_TOPIC_AGGREGATE_H
#define LDA_WORD_TOPIC_AGGREGATE_H

//by Shangyu, May 2017

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "ClusterAggregateComp.h"
#include "IntDoubleVectorPair.h"
#include "LDATopicWordProb.h"



using namespace pdb;


class LDAWordTopicAggregate : public ClusterAggregateComp <IntDoubleVectorPair, LDATopicWordProb, int, Vector<double>> {

private:
	int numTopic;

public:

        ENABLE_DEEP_COPY

        LDAWordTopicAggregate () {}
        LDAWordTopicAggregate (int fromTopic) {
		this->numTopic = fromTopic;
	}


        // the key type must have == and size_t hash () defined
        Lambda <int> getKeyProjection (Handle <LDATopicWordProb> aggMe) override {
		return makeLambdaFromMethod(aggMe, getWord);
//                return makeLambda (aggMe, [] (Handle<LDATopicWordProb> & aggMe) {return aggMe->getDoc();});
        }

        // the value type must have + defined
        Lambda <Vector<double>> getValueProjection (Handle <LDATopicWordProb> aggMe) override {

            	return makeLambda (aggMe, [&] (Handle<LDATopicWordProb> & aggMe) { 

			Handle<Vector<double>> result = makeObject<Vector<double>>(this->numTopic, this->numTopic);
			(*result)[aggMe->getTopic()] = aggMe->getProbability();
			return *result;
		});
        }


};


#endif
