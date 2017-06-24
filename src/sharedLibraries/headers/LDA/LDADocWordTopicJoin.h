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

//by Shangyu, Mar 2017

#include "JoinComp.h"
#include "LDADocWordTopicAssignment.h"
#include "LambdaCreationFunctions.h"
#include "LDADocument.h"
#include "IntDoubleVectorPair.h"
#include <gsl/gsl_rng.h>
#include <gsl/gsl_randist.h>
#include <random>
#include <gsl/gsl_vector.h>

using namespace pdb;

class LDADocWordTopicJoin : public JoinComp <LDADocWordTopicAssignment, LDADocument, IntDoubleVectorPair, IntDoubleVectorPair> {

private:
	Handle <Vector <char>> myMem;

public:

        ENABLE_DEEP_COPY

        LDADocWordTopicJoin () {
		
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

        Lambda <bool> getSelection (Handle <LDADocument> doc, Handle <IntDoubleVectorPair> DocTopicProb, 
		Handle <IntDoubleVectorPair> WordTopicProb) override {
           	
		   // return (makeLambdaFromMethod (doc, getDoc) == makeLambdaFromMethod (DocTopicProb, getInt)) && 
		//		(makeLambdaFromMethod (doc, getWord) == makeLambdaFromMethod (WordTopicProb, getInt));	
		     
		    return makeLambda (doc, DocTopicProb, WordTopicProb, [] (Handle<LDADocument> & doc, 
			Handle<IntDoubleVectorPair> & DocTopicProb, Handle<IntDoubleVectorPair> & WordTopicProb) {
				return (doc->getDoc() == DocTopicProb->getInt() && doc->getWord() == WordTopicProb->getInt());
		    	});
		    

        	}

        Lambda <Handle <LDADocWordTopicAssignment>> getProjection (Handle <LDADocument> doc, 
		Handle <IntDoubleVectorPair> DocTopicProb, Handle <IntDoubleVectorPair> WordTopicProb) override {

		    return makeLambda (doc, DocTopicProb, WordTopicProb, [&] (Handle<LDADocument> & doc, 
			Handle<IntDoubleVectorPair> & DocTopicProb, Handle<IntDoubleVectorPair> & WordTopicProb) {
				//Handle<Vector<double>> topicProbForDoc = DocTopicProb->getVector();
				//Handle<Vector<double>> topicProbForWord = WordTopicProb->getVector();
				int size = (DocTopicProb->getVector()).size();

				std :: cout << "Topic size: " << size << "\n\n";	
			
				Handle<Vector<double>> myProb = makeObject<Vector<double>>(size, size);
				Handle<Vector<int>> topics = makeObject<Vector<int>>(size, size);
				Handle<Vector<int>> topicAssignment = makeObject<Vector<int>>();

				gsl_rng *rng = getRng();
                        	//rng = gsl_rng_alloc(gsl_rng_mt19937);
                        	//std::random_device rd;
                        	//std::mt19937 gen(rd());
                        	//gsl_rng_set(rng, gen());
							
				std :: cout << "For doc: " << doc->getDoc() << "\n";
				std :: cout << "DocTopicProb: " << "\n";
				(DocTopicProb->getVector()).print();
				std :: cout << "WordTopicProb: " << "\n";
				(WordTopicProb->getVector()).print();
		

				for (int i = 0; i < size; ++i) {
					(*myProb)[i] = (DocTopicProb->getVector())[i] * (WordTopicProb->getVector())[i]; 
				}

				std :: cout << "My Prob: " << "\n";
				(*myProb).print();
		


				gsl_ran_multinomial (rng, size, doc->getCount(), myProb->c_ptr(), (unsigned int*)topics->c_ptr());
	
				for (int i = 0; i < size; ++i) {
					if ((*topics)[i] != 0) {
						topicAssignment->push_back(i);	
						topicAssignment->push_back((*topics)[i]);	
						std::cout << "I get topic: " << i << ", count: " << (*topics)[i] << std::endl;
					}
				}
				
				Handle<LDADocWordTopicAssignment> result = 
					makeObject<LDADocWordTopicAssignment> (doc->getDoc(), doc->getWord(), topicAssignment);
				return result;
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
