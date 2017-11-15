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

#ifndef LDA_TOPIC_WORD_PROB_MULTI_SELECT_H
#define LDA_TOPIC_WORD_PROB_MULTI_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"
#include "PDBVector.h"
#include "LDATopicWordProb.h"
#include "TopicAssignment.h"
#include "LDATopicWordProb.h"
#include <gsl/gsl_rng.h>
#include <gsl/gsl_randist.h>
#include <random>
#include <gsl/gsl_vector.h>

/* This class generates the entries for the topic probability for each word */
using namespace pdb;
class LDATopicWordProbMultiSelection
    : public MultiSelectionComp<LDATopicWordProb, TopicAssignment> {

private:
    Vector<double> prior;
    Handle<Vector<char>> myMem;
    int numTopics;

public:
    ENABLE_DEEP_COPY

    LDATopicWordProbMultiSelection() {}

    LDATopicWordProbMultiSelection(Vector<double>& fromPrior, unsigned numTopics) {

        this->prior = fromPrior;
        this->numTopics = numTopics;

	/* Set up the random number generator */
        gsl_rng* src = gsl_rng_alloc(gsl_rng_mt19937);
        std::random_device rd;
        std::mt19937 gen(rd());
        gsl_rng_set(src, gen());

        int spaceNeeded = sizeof(gsl_rng) + src->type->size;
        myMem = makeObject<Vector<char>>(spaceNeeded, spaceNeeded);

        memcpy(myMem->c_ptr(), src, sizeof(gsl_rng));
        memcpy(myMem->c_ptr() + sizeof(gsl_rng), src->state, src->type->size);

        gsl_rng_free(src);
    }

    Lambda<bool> getSelection(Handle<TopicAssignment> checkMe) override {
        return makeLambda(checkMe, [](Handle<TopicAssignment>& checkMe) { return true; });
    }

    Lambda<Vector<Handle<LDATopicWordProb>>> getProjection(
        Handle<TopicAssignment> checkMe) override {
        return makeLambda(checkMe, [&](Handle<TopicAssignment>& checkMe) {

            gsl_rng* rng = getRng();
            Vector<unsigned>& wordCount = checkMe->getVector();

            unsigned wordNum = prior.size();
            double* mySamples = new double[wordNum];
            double* totalProb = new double[wordNum];
            double* myPrior = prior.c_ptr();
            for (int i = 0; i < wordNum; i++) {
                totalProb[i] = myPrior[i] + wordCount[i];
            }

	    /* Sample the word probability */
            gsl_ran_dirichlet(rng, wordNum, totalProb, mySamples);

	    /* Create the topic probability for each word */
            Vector<Handle<LDATopicWordProb>> result(wordNum);
            for (int i = 0; i < wordNum; i++) {
                Handle<LDATopicWordProb> myTWP =
                    makeObject<LDATopicWordProb>(numTopics, i, checkMe->getKey(), mySamples[i]);
                result.push_back(myTWP);
            }

            delete[] mySamples;
            delete[] totalProb;

            return result;

        });
    }

    gsl_rng* getRng() {
        gsl_rng* dst = (gsl_rng*)myMem->c_ptr();
        dst->state = (void*)(myMem->c_ptr() + sizeof(gsl_rng));
        dst->type = gsl_rng_mt19937;
        return dst;
    }
};


#endif
