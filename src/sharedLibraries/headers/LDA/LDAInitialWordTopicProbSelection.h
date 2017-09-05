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

#ifndef LDA_INITIAL_WORD_TOPIC_PROB_SELECT_H
#define LDA_INITIAL_WORD_TOPIC_PROB_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "SelectionComp.h"
#include "PDBVector.h"
#include "IntDoubleVectorPair.h"
#include "LDA/LDATopicWordProb.h"

using namespace pdb;
class LDAInitialWordTopicProbSelection : public SelectionComp <LDATopicWordProb, int> {

private:
	unsigned numTopic;

public:

	ENABLE_DEEP_COPY

	LDAInitialWordTopicProbSelection () {}
	LDAInitialWordTopicProbSelection (unsigned numTopicIn) {
		this->numTopic = numTopicIn;
	}

	Lambda <bool> getSelection (Handle <int> checkMe) override {
		return makeLambda (checkMe, [] (Handle<int> & checkMe) {return true;});
	}

	Lambda <Handle <LDATopicWordProb>> getProjection (Handle <int> checkMe) override {


		return makeLambda (checkMe, [&] (Handle<int> & checkMe) {

			int numWord = *checkMe;

			// first, seed the RNG
			srand48 (numWord);

                        Handle<Vector <double>> wordTopicProb = makeObject<Vector <double>> (numTopic);
                        for (int j = 0; j < numTopic; j++) {
                        	wordTopicProb->push_back(drand48 ());
                        }
			Handle <LDATopicWordProb> myList = makeObject <LDATopicWordProb> (numWord, wordTopicProb);
			
			return myList;
		});
	}
};


#endif
