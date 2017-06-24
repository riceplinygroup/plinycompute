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
#ifndef LDA_DOC_TOPIC_AGGREGATE_H
#define LDA_DOC_TOPIC_AGGREGATE_H

//by Shangyu, May 2017

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "ClusterAggregateComp.h"
#include "IntIntVectorPair.h"
#include "LDADocWordTopicAssignment.h"
#include "PDBVector.h"


using namespace pdb;


class LDADocTopicAggregate : public ClusterAggregateComp <IntIntVectorPair, LDADocWordTopicAssignment, int, Vector<int>> {

private:
	int numTopic;

public:

        ENABLE_DEEP_COPY

        LDADocTopicAggregate () {}
        LDADocTopicAggregate (int fromTopic) {
		this->numTopic = fromTopic;
	}


        // the key type must have == and size_t hash () defined
        Lambda <int> getKeyProjection (Handle <LDADocWordTopicAssignment> aggMe) override {
                return makeLambda (aggMe, [] (Handle<LDADocWordTopicAssignment> & aggMe) {return aggMe->getDoc();});
        }

        // the value type must have + defined
        Lambda <Vector<int>> getValueProjection (Handle <LDADocWordTopicAssignment> aggMe) override {
            	return makeLambda (aggMe, [&] (Handle<LDADocWordTopicAssignment> & aggMe) { 
			Handle<Vector<int>> result = makeObject<Vector<int>>(this->numTopic, this->numTopic);
			result->fill(0);
			Vector<int>& topicAssign = aggMe->getTopicAssignment();
			for (int i = 0; i < topicAssign.size(); i+=2){
				(*result)[topicAssign[i]] += topicAssign[i+1]; 	
			}
			return *result;
		});
        }


};


#endif
