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
#pragma once


//PRELOAD %BuiltinTopKQuery <Nothing>%

#ifndef MAX_THREADS
   #define MAX_THREADS 16
#endif

#ifndef K
   #define K 20
#endif

#define ACCURATE_TOPK


#include "BuiltinTopKInput.h"
#include "BuiltinTopKResult.h"
#include "Selection.h"
#include "PDBMap.h"
#include <sched.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>



namespace pdb {

template <typename TypeContained>
class BuiltinTopKQuery : public Selection <BuiltinTopKResult<TypeContained>, TypeContained> {

public:

       ENABLE_DEEP_COPY

       BuiltinTopKQuery () { 
         this->numK = K; 
       }

       BuiltinTopKQuery (int k) {
         this->numK = k;
      }


       ~BuiltinTopKQuery() {
           counters = nullptr;
       }


       virtual double getScore(Handle<TypeContained> object) {
           return 3.14159265359;
       };

       void initialize () {
           threadToken = 0;
           counters = makeObject<Map<pthread_t, unsigned long>>(2*MAX_THREADS);
           threads = makeObject<Vector<pthread_t>>(MAX_THREADS);
           partialResults = makeObject<Map<pthread_t, Handle<BuiltinTopKResult<TypeContained>>>>(2*MAX_THREADS);
       }

       SimpleLambda <bool> getSelection (Handle <TypeContained> &checkMe) override {
		return makeSimpleLambda (checkMe, [&] () {
                    return false; //getSelection will not be applied in pipeline, so simply return false
		});
       }


       SimpleLambda <bool> getProjectionSelection (Handle<BuiltinTopKResult<TypeContained>> &checkMe) override {
                return makeSimpleLambda (checkMe, [&] () {
                    #ifdef ACCURATE_TOPK
                       return false; //if we do accurate topK
                    #else
                        if (checkMe == nullptr) {
                            return false;
                        } else {
                            return true;
                        }
                    #endif
                });
       }



       SimpleLambda <Handle <BuiltinTopKResult<TypeContained>>> getProjection (Handle<TypeContained> &checkMe) override {
                 
		return makeSimpleLambda (checkMe, [&] {
                        pthread_t threadId = pthread_self();
                        pthread_t expected = 0;
                        if (counters->count(threadId) == 0) {
                            while(threadToken != threadId) {
                                __atomic_compare_exchange(&threadToken, &expected, &threadId,false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
                                expected = 0;
                                /*while(threadToken != 0) {
                                    sched_yield();
                                }
                                threadToken = threadId;
                                */
                            }
                            if (threadToken == threadId) {
                                UseTemporaryAllocationBlock tempBlock{12*1024*1024};
                                std::cout << "to allocate slot for thread:"<<(unsigned long)(threadId)<<std::endl;
                                if (threadToken != threadId) {
                                    //we have applied atomic_compare_exchange, this should never happen
                                    std :: cout << "WARNING: possibly a racing condition on thread token detected, threadToken="<<threadToken<<", threadId="<<threadId << std :: endl;

                                }
                                (*counters)[threadId] = 0;
                                std :: cout << "counter is set" << std :: endl;
                                Handle<BuiltinTopKResult<TypeContained>> partialResult = makeObject<BuiltinTopKResult<TypeContained>> ();
                                partialResult->initialize();
                                PDB_COUT << "to set slot in result hashmap for threadId=" <<(unsigned long) (threadId)<<std :: endl;
                                (*partialResults)[threadId] = partialResult;
                                std :: cout << "result is set" << std :: endl;
                                threads->push_back(threadId);
                                threadToken = 0;
                            }
                        }  
                        #ifndef ACCURATE_TOPK
                        if ((*counters)[threadId] == 10000) {
                            //(*partialResults)[threadId]->reset(); //to clear partial results for last emission
                            (*partialResults)[threadId]->initialize();
                            (*counters)[threadId] = 0; // to clear counter for last emission
                        }
                        #endif
                        double score = getScore(checkMe);
                        PDB_COUT << "before update topK: score=" << score << std :: endl;
                        Handle<BuiltinTopKResult<TypeContained>> partialResult = (*partialResults)[threadId];
                        partialResult->updateTopK(score, checkMe);
                        (*counters)[threadId] ++;

                        Handle<BuiltinTopKResult<TypeContained>> ret = nullptr;
                        #ifdef ACCURATE_TOPK
                        return ret;
                        #else
                        if ((*counters)[threadId] == 10000) {
                            ret = makeObject<BuiltinTopKResult<TypeContained>>();
                            (*ret) = *(*partialResults)[threadId];
                            auto elements = ret->getTopK();
                            int i;
                            for (i = 0; i < elements->size(); i ++) {
                                std::cout << "score=" << (*elements)[i]->getScore() << std::endl;
                            }
                            std :: cout << "emit partialresult with "<< ret->getTopK()->size() << " elements" << std :: endl;
                             
                        }
                        return ret;
                        #endif
		});
	}


        virtual bool isAggregation() override {   return true; }

        Handle<Vector<Handle<BuiltinTopKResult<TypeContained>>>>& getAggregatedResults () override  {
            this->aggregationResult = makeObject<Vector<Handle<BuiltinTopKResult<TypeContained>>>> (MAX_THREADS);          
            int i , j;
            for (i = 0; i < threads->size(); i ++) {
                auto result = (*partialResults)[(*threads)[i]]->getTopK();
                for (j = 0; j < result->size(); j++) {
                   std::cout <<i <<"-" << j << ":"<<(*result)[j]->getScore() << std::endl;
                }
                Handle<BuiltinTopKResult<TypeContained>> curResult= makeObject<BuiltinTopKResult<TypeContained>>();
                this->aggregationResult->push_back(curResult);
                *(*(this->aggregationResult))[i] = *((*partialResults)[(*threads)[i]]);
                auto result1 = (*(this->aggregationResult))[i]->getTopK();
                for (j = 0; j < result1->size(); j++) {
                   std::cout <<i <<"-" << j << ":"<<(*result1)[j]->getScore() << std::endl;
               }
            }
            PDB_COUT << "there are " << i << " partial results" << std :: endl;
            PDB_COUT << "aggregation result size=" << this->aggregationResult->size() << std :: endl;
            return this->aggregationResult;
        }

        Handle<Vector<Handle<BuiltinTopKResult<TypeContained>>>>& getAggregatedResultsOptimized () override  {
            this->aggregationResult = makeObject<Vector<Handle<BuiltinTopKResult<TypeContained>>>> (1);
            Handle<BuiltinTopKResult<TypeContained>> finalResult = makeObject<BuiltinTopKResult<TypeContained>>();
            finalResult->initialize();
            int i , j;
            for (i = 0; i < threads->size(); i ++) {
                auto result = (*partialResults)[(*threads)[i]]->getTopK();
                for (j = 0; j < result->size(); j++) {
                   std::cout <<i <<"-" << j << ":"<<(*result)[j]->getScore() << std::endl;
                   finalResult->updateTopK((*result)[j]->getScore(), (*result)[j]->getObject());
                }
            }
            this->aggregationResult->push_back(finalResult);
            auto result1 = (*(this->aggregationResult))[0]->getTopK();
            for (j = 0; j < result1->size(); j++) {
                std::cout << j << ":"<<(*result1)[j]->getScore() << std::endl;
            }
            PDB_COUT << "aggregation result size=" << this->aggregationResult->size() << std :: endl;
            return this->aggregationResult;
        }

        // gets the name of the i^th input type...
        virtual std :: string getIthInputType (int i) override {
                if (i == 0)
                        return getTypeName <TypeContained> ();
                else
                        return "bad index";
        }

private:

        //counters to record the number of points processed
        Handle<Map<pthread_t, unsigned long>> counters;
        Handle<Vector<pthread_t>> threads;
        //current aggregated TopK values
        //each thread has a TopKResult instance
        Handle<Map<pthread_t, Handle<BuiltinTopKResult<TypeContained>>>> partialResults;
        //number of top elements
        int numK;
        pthread_t threadToken;
};

}
