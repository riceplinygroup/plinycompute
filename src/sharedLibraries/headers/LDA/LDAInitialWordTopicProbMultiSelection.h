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

#ifndef LDA_INITIAL_WORD_TOPIC_PROB_MULTI_SELECT_H
#define LDA_INITIAL_WORD_TOPIC_PROB_MULTI_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"
#include "PDBVector.h"
#include "IntDoubleVectorPair.h"

using namespace pdb;
class LDAInitialWordTopicProbMultiSelection : public MultiSelectionComp <IntDoubleVectorPair, int> {

private:
	int numTopic;

public:

	ENABLE_DEEP_COPY

	LDAInitialWordTopicProbMultiSelection () {}
	LDAInitialWordTopicProbMultiSelection (int fromTopic) {
		this->numTopic = fromTopic;
	}

	Lambda <bool> getSelection (Handle <int> checkMe) override {
		return makeLambda (checkMe, [] (Handle<int> & checkMe) {return true;});
	}

	Lambda <Vector<Handle <IntDoubleVectorPair>>> getProjection (Handle <int> checkMe) override {
		return makeLambda (checkMe, [&] (Handle<int> & checkMe) {
			int numWord = *checkMe;
			Handle<Vector<Handle<IntDoubleVectorPair>>> result = 
				makeObject<Vector<Handle<IntDoubleVectorPair>>>(numWord, numWord);
			for (int i = 0; i < numWord; i++) {
				Handle<Vector<double>> topicProb = 
					makeObject<Vector<double>>(this->numTopic, this->numTopic);
				topicProb->fill(1.0);
				Handle<IntDoubleVectorPair> wordTopicProb= 
					makeObject<IntDoubleVectorPair>(i, topicProb);
				(*result)[i] = wordTopicProb;
			//	std::cout << "The topic probability for word " << i << " : " << std::endl;
			//	((*result)[i])->getVector()->print();

			}
			
			/*
			Handle<Vector<int>> tmpVector = makeObject <Vector<int>>(1, 1);
			tmpVector->push_back(6);
			Handle<LDADocWordTopicAssignment> tmpDWTA= makeObject <LDADocWordTopicAssignment>(1, 2, tmpVector);
			std :: cout << "The test doc: " << tmpDWTA->getDoc() << std :: endl;
			*/
			
			return *result;
		});
	}
};


#endif
