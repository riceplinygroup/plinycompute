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
#include "IntIntVectorPair.h"
#include "LDATopicWordProb.h"
#include <gsl/gsl_rng.h>
#include <gsl/gsl_randist.h>
#include <random>
#include <gsl/gsl_vector.h>

using namespace pdb;
class LDATopicWordProbMultiSelection : public MultiSelectionComp <LDATopicWordProb, IntIntVectorPair> {

private:
        Vector<double> prior;
        Handle <Vector <char>> myMem;
	int wordNum;

public:

	ENABLE_DEEP_COPY

	LDATopicWordProbMultiSelection () {}

	LDATopicWordProbMultiSelection (Vector<double>& fromPrior) {

                this->prior = fromPrior;
		this->wordNum = fromPrior.size();

                // start by setting up the gsl_rng *src...
                gsl_rng *src = gsl_rng_alloc(gsl_rng_mt19937);
                std::random_device rd;
                std::mt19937 gen(rd());
                gsl_rng_set(src, gen());

                // now allocate space needed for myRand
                int spaceNeeded = sizeof (gsl_rng) + src->type->size;
                myMem = makeObject <Vector <char>> (spaceNeeded, spaceNeeded);

                // copy src over
                memcpy (myMem->c_ptr (), src, sizeof (gsl_rng));
                memcpy (myMem->c_ptr () + sizeof (gsl_rng), src->state, src->type->size);
                memcpy (myMem->c_ptr () + sizeof (gsl_rng), src->state, src->type->size);

                // lastly, free src
                gsl_rng_free (src);


        }

	Lambda <bool> getSelection (Handle <IntIntVectorPair> checkMe) override {
		return makeLambda (checkMe, [] (Handle<IntIntVectorPair> & checkMe) {return true;});
	}

	Lambda <Vector<Handle <LDATopicWordProb>>> getProjection (Handle <IntIntVectorPair> checkMe) override {
		return makeLambda (checkMe, [&] (Handle<IntIntVectorPair> & checkMe) {
	
			gsl_rng *rng = getRng();
			Vector<int>& wordCount = checkMe->getVector();

                        Handle<Vector<double>> mySamples= makeObject<Vector<double>>(wordNum, wordNum);
                        Handle<Vector<double>> totalProb= makeObject<Vector<double>>(wordNum, wordNum);
                        for (int i = 0; i < wordNum; i++) {
                                (*totalProb)[i] = (this->prior)[i] + wordCount[i];
                        }

                                std :: cout << "For topic: " << checkMe->getInt() << "\n";
                                std :: cout << "This prior: " << "\n";
                                (this->prior).print();
                                std :: cout << "Word count: " << "\n";
                                wordCount.print();

                                std :: cout << "Total Prob: " << "\n";
                                (*totalProb).print();

                        gsl_ran_dirichlet(rng, wordNum, totalProb->c_ptr(), mySamples->c_ptr());

			
			Handle<Vector<Handle<LDATopicWordProb>>> result = makeObject<Vector<Handle<LDATopicWordProb>>>();
			for (int i = 0; i < wordNum; i++) {
				Handle<LDATopicWordProb> myTWP = 
					makeObject<LDATopicWordProb>(checkMe->getInt(), i, (*mySamples)[i]);
				result->push_back(myTWP);
				
			}

                        return *result;		
				
		});
	}

	// gets the GSL RNG from myMem

        gsl_rng *getRng () {
                gsl_rng *dst = (gsl_rng *) myMem->c_ptr ();
                dst->state = (void *) (myMem->c_ptr () + sizeof (gsl_rng));
                dst->type = gsl_rng_mt19937;
                return dst;
        }
};


#endif
