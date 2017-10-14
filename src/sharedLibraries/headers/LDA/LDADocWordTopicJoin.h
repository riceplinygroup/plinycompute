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
#ifndef LDA_DOC_WORD_TOPIC_JOIN_H
#define LDA_DOC_WORD_TOPIC_JOIN_H

// by Shangyu, Mar 2017

#include "JoinComp.h"
#include "Lambda.h"
#include "LDADocWordTopicAssignment.h"
#include "LDA/LDATopicWordProb.h"
#include "LambdaCreationFunctions.h"
#include "LDADocument.h"
#include "IntDoubleVectorPair.h"
#include "PDBVector.h"
#include <gsl/gsl_rng.h>
#include <gsl/gsl_randist.h>
#include <random>
#include <gsl/gsl_vector.h>
#include <algorithm>
#include <iostream>
#include <math.h>

using namespace pdb;

class LDADocWordTopicJoin : public JoinComp<LDADocWordTopicAssignment,
                                            LDADocument,
                                            IntDoubleVectorPair,
                                            LDATopicWordProb> {

private:
    Handle<Vector<char>> myMem;
    unsigned numWords;

public:
    ENABLE_DEEP_COPY

    LDADocWordTopicJoin() {}

    LDADocWordTopicJoin(unsigned numWords) : numWords(numWords) {

        // start by setting up the gsl_rng *src...
        gsl_rng* src = gsl_rng_alloc(gsl_rng_mt19937);
        std::random_device rd;
        std::mt19937 gen(rd());
        gsl_rng_set(src, gen());

        // now allocate space needed for myRand
        int spaceNeeded = sizeof(gsl_rng) + src->type->size;
        myMem = makeObject<Vector<char>>(spaceNeeded, spaceNeeded);

        // copy src over
        memcpy(myMem->c_ptr(), src, sizeof(gsl_rng));
        memcpy(myMem->c_ptr() + sizeof(gsl_rng), src->state, src->type->size);

        // lastly, free src
        gsl_rng_free(src);
    }

    Lambda<bool> getSelection(Handle<LDADocument> doc,
                              Handle<IntDoubleVectorPair> DocTopicProb,
                              Handle<LDATopicWordProb> WordTopicProb) override {
        return (makeLambdaFromMethod(doc, getDoc) ==
                makeLambdaFromMethod(DocTopicProb, getUnsigned)) &&
            (makeLambdaFromMethod(doc, getWord) == makeLambdaFromMethod(WordTopicProb, getKey));
    }

    Lambda<Handle<LDADocWordTopicAssignment>> getProjection(
        Handle<LDADocument> doc,
        Handle<IntDoubleVectorPair> DocTopicProb,
        Handle<LDATopicWordProb> WordTopicProb) override {

        return makeLambda(
            doc,
            DocTopicProb,
            WordTopicProb,
            [&](Handle<LDADocument>& doc,
                Handle<IntDoubleVectorPair>& DocTopicProb,
                Handle<LDATopicWordProb>& WordTopicProb) {
                int size = (DocTopicProb->getVector()).size();

                // compute the posterior probailities of the word coming from each topic
                double* myProb = new double[size];
                double* topicProbs = DocTopicProb->getVector().c_ptr();
                double* wordProbs = WordTopicProb->getVector().c_ptr();
                for (int i = 0; i < size; ++i) {
                    myProb[i] = topicProbs[i] * wordProbs[i];
                }

                unsigned* topics = new unsigned[size]{0};

                // do the random assignment
                gsl_rng* rng = getRng();
                // gsl_ran_multinomial (rng, size, doc->getCount(), myProb, topics);

                // another way of sampling topics (avoid gsl)

                unsigned counts = doc->getCount();
                double* random_values = new double[counts];

                // normalize myProb and avoid overflow
                double sum = 0.0;
                for (int i = 0; i < size; ++i) {
                    sum += myProb[i];
                }

                for (int i = 0; i < counts; ++i) {
                    random_values[i] = gsl_rng_uniform(rng) * sum;
                }

                std::sort(random_values, random_values + counts);


                int j = 0;
                double accumuProb = 0.0;
                for (int i = 0; i < size; i++) {
                    accumuProb += myProb[i];
                    while ((j < counts) && (random_values[j] < accumuProb)) {
                        topics[i]++;
                        j++;
                    }
                }


                // get the container for the return value
                Handle<LDADocWordTopicAssignment> retVal = makeObject<LDADocWordTopicAssignment>();
                retVal->setup();
                LDADocWordTopicAssignment& myGuy = *retVal;

                // get the meta-data
                unsigned myDoc = doc->getDoc();
                unsigned myWord = doc->getWord();

                // extract all of the words and put the in the return value
                for (int i = 0; i < size; ++i) {
                    if (topics[i] != 0) {
                        Handle<DocAssignment> whichDoc =
                            makeObject<DocAssignment>(size, myDoc, i, topics[i]);
                        Handle<TopicAssignment> whichTopic =
                            makeObject<TopicAssignment>(numWords, i, myWord, topics[i]);
                        myGuy.push_back(whichDoc);
                        myGuy.push_back(whichTopic);
                    }
                }

                // free the memory we allocated
                delete[] myProb;
                delete[] topics;
                delete[] random_values;

                // and get outta here
                return retVal;
            });
    }

    // gets the GSL RNG from myMem

    gsl_rng* getRng() {
        gsl_rng* dst = (gsl_rng*)myMem->c_ptr();
        dst->state = (void*)(myMem->c_ptr() + sizeof(gsl_rng));
        dst->type = gsl_rng_mt19937;
        return dst;
    }
};


#endif
