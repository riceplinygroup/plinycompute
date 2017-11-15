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

#include "Object.h"
#include "Handle.h"
#include "NumericalVector.h"

/* This class stores a word and its topic probability */
using namespace pdb;

class LDATopicWordProb : public Object {

private:
    /* The word for which we have the topic probabilities */
    unsigned whichWord;

    /* The list of probabilities */
    NumericalVector<double> probabilities;

public:
    ENABLE_DEEP_COPY

    LDATopicWordProb() {}

    LDATopicWordProb(unsigned numTopics,
                     unsigned fromWord,
                     unsigned fromTopic,
                     double fromProbability)
        : whichWord(fromWord), probabilities(numTopics, fromTopic, fromProbability) {}

    LDATopicWordProb(unsigned whichWord, Handle<Vector<double>> probabilities)
        : whichWord(whichWord), probabilities(probabilities) {}

    unsigned& getKey() {
        return whichWord;
    }

    LDATopicWordProb& operator+(LDATopicWordProb& me) {
        probabilities += me.probabilities;
        return *this;
    }

    LDATopicWordProb& getValue() {
        return *this;
    }

    Vector<double>& getVector() {
        return probabilities.getVector();
    }

    ~LDATopicWordProb() {}
};


#endif
