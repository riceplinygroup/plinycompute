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

#ifndef LDA_TOPIC_WORD_PROB_H
#define LDA_TOPIC_WORD_PROB_H

// By Shangyu

#include "Object.h"
#include "Handle.h"

//namespace pdb {

using namespace pdb;

class LDATopicWordProb  : public Object {

private:

        int topicID;
	int wordID;
	double probability;

public:

	ENABLE_DEEP_COPY

        LDATopicWordProb () {}
        LDATopicWordProb (int fromTopic, int fromWord, double fromProbability) {
		this->topicID = fromTopic;
		this->wordID = fromWord;
		this->probability = fromProbability;
	}

	int getTopic() {
		return this->topicID;
	}


	int getWord() {
		return this->wordID;
	}

	double getProbability() {
		return this->probability;
	}


        ~LDATopicWordProb () {}

};

//}

#endif
