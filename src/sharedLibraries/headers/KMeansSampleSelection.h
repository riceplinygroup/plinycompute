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

#ifndef K_MEANS_SAMPLE_SELECTION_H
#define K_MEANS_SAMPLE_SELECTION_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "SelectionComp.h"
#include "DoubleVector.h"
#include "PDBVector.h"
#include "PDBString.h"
//#include <gsl/gsl_rng.h>
//#include <gsl/gsl_randist.h>
//#include <gsl/gsl_vector.h>
//#include <random>
#include <ctime>
#include <cstdlib>

using namespace pdb;
class KMeansSampleSelection : public SelectionComp <DoubleVector, DoubleVector> {

private:
        double fraction;
        //JiaNote: below can't work in a cluster
	//std::uniform_real_distribution<> unif;
	//std::mt19937 gen;

        //JiaNote: so we create a pdbObject to store the random stuff
        Handle <Vector <char>> myMem;

public:

	ENABLE_DEEP_COPY

	KMeansSampleSelection () {

        }

	KMeansSampleSelection (double inputFraction) {
		this->fraction = inputFraction;

                //JiaNote: below can't work in a cluster
		//std::random_device rd;
		//std::mt19937 gen(rd());
                //srand (time(0));

                //JiaNote: below is still not good as each thread will have the same seed.
                /*
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

                // lastly, free src
                gsl_rng_free (src);
                */

	}

        //srand has already been invoked in server
	Lambda <bool> getSelection (Handle <DoubleVector> checkMe) override {
		return makeLambda (checkMe, [&] (Handle<DoubleVector> & checkMe) {
                //JiaNote: below can't work in a cluster			
		//	std::random_device rd;
		//	std::mt19937 gen(rd());
			//double myVal = this->unif(this->gen);

                //JiaNote: below is still not good as each thread will have the same seed.
                        /*
                        gsl_rng * rng = getRng();
                        //this function seems perfect for our use
                        double myVal = gsl_rng_uniform(rng);
                        */

                        double myVal = (double)rand()/(double)RAND_MAX;
			bool ifSample = (myVal <= (this->fraction));
	//		std :: cout << "The sampled value: " << myVal << std :: endl;
			if (ifSample)
				return true;
			else
				return false;
                });
	}

        //JiaNote: we need to provide different seeds for different threads, so we do not need below function that is mainly used to store one value for all threads.
        // gets the GSL RNG from myMem
        /*gsl_rng *getRng () {
                gsl_rng *dst = (gsl_rng *) myMem->c_ptr ();
                dst->state = (void *) (myMem->c_ptr () + sizeof (gsl_rng));
                dst->type = gsl_rng_mt19937;
                return dst;
        }*/




	Lambda <Handle <DoubleVector>> getProjection (Handle <DoubleVector> checkMe) override {
		return makeLambda (checkMe, [] (Handle<DoubleVector> & checkMe) {return checkMe;});
	}
};


#endif
