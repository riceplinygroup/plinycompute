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

#ifndef BUILTIN_TOPK_QUERY_H
#define BUILTIN_TOPK_QUERY_H

// PRELOAD %BuiltinTopKQuery%
#ifndef MAX_THREADS
   #define MAX_THREADS 8
#endif

#ifndef K
   #define K 100
#endif

#define ACCURATE_TOPK

#include "BuiltinTopKInput.h"
#include "BuiltinTopKResult.h"
#include "Selection.h"
#include "PDBMap.h"
#include <pthread.h>
#include <stdlib.h>
#include <time.h>

/*
 * This class is deprecated and not maintained.
 * The TopKQuery class is moved to the pliny project for easier scripting with Pliny classes
 * To find the new class TopKQuery from pliny project.
 */



namespace pdb {
class BuiltinTopKQuery : public Selection <BuiltinTopKResult, Object> {

public:

       ENABLE_DEEP_COPY

       BuiltinTopKQuery () { 
       }


       ~BuiltinTopKQuery() {
           counters = nullptr;
       }


       virtual double getScore(Handle<Object> object) {
           return 3.14159265359;
       };


       void initialize () {
           counters = makeObject<Map<pthread_t, unsigned long>>(MAX_THREADS);
           threads = makeObject<Vector<pthread_t>>(MAX_THREADS);
           partialResults = makeObject<Map<pthread_t, Handle<BuiltinTopKResult>>>(MAX_THREADS);
       }



       SimpleLambda <bool> getSelection (Handle <Object> &checkMe) override {
		return makeLambda (checkMe, [&] () {
                    return false; //getSelection will not be applied in pipeline, so simply return false
		});
       }


       SimpleLambda <bool> getProjectionSelection (Handle<BuiltinTopKResult> &checkMe) override {
                return makeLambda (checkMe, [&] () {
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



       SimpleLambda <Handle <BuiltinTopKResult>> getProjection (Handle<Object> &checkMe) override {
                 
		return makeLambda (checkMe, [&] {
                        pthread_t threadId = pthread_self();
                        if (counters->count(threadId) == 0) {
                            UseTemporaryAllocationBlock tempBlock{8*1024*1024};
                            std::cout << "to allocate slot for thread:"<<(unsigned long)(threadId)<<std::endl;
                            (*counters)[threadId] = 0;
                            Handle<BuiltinTopKResult> partialResult = makeObject<BuiltinTopKResult> ();
                            partialResult->initialize();
                            (*partialResults)[threadId] = partialResult;
                            threads->push_back(threadId);       
                        }  
                        #ifndef ACCURATE_TOPK
                        if ((*counters)[threadId] == 10000) {
                            //(*partialResults)[threadId]->reset(); //to clear partial results for last emission
                            (*partialResults)[threadId]->initialize();
                            (*counters)[threadId] = 0; // to clear counter for last emission
                        }
                        #endif
                        double score = getScore(checkMe);
                        (*partialResults)[threadId]->updateTopK(score, checkMe);
                        (*counters)[threadId] ++;

                        Handle<BuiltinTopKResult> ret = nullptr;
                        #ifdef ACCURATE_TOPK
                        return ret;
                        #else
                        if ((*counters)[threadId] == 10000) {
                            ret = makeObject<BuiltinTopKResult>();
                            (*ret) = *(*partialResults)[threadId];
                            Handle<Vector<Handle<BuiltinTopKInput>>> elements = ret->getTopK();
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


        Handle<Vector<Handle<BuiltinTopKResult>>>& getAggregatedResults () override  {
            aggregationResult = makeObject<Vector<Handle<BuiltinTopKResult>>> (MAX_THREADS);          
            int i , j;
            for (i = 0; i < threads->size(); i ++) {
                Handle<Vector<Handle<BuiltinTopKInput>>> result = (*partialResults)[(*threads)[i]]->getTopK();
                for (j = 0; j < result->size(); j++) {
                   PDB_COUT <<i <<"-" << j << ":"<<(*result)[j]->getScore() << std::endl;
                }
                Handle<BuiltinTopKResult> curResult= makeObject<BuiltinTopKResult>();
                aggregationResult->push_back(curResult);
                *(*aggregationResult)[i] = *((*partialResults)[(*threads)[i]]);
                Handle<Vector<Handle<BuiltinTopKInput>>> result1 = (*aggregationResult)[i]->getTopK();
                for (j = 0; j < result1->size(); j++) {
                   PDB_COUT <<i <<"-" << j << ":"<<(*result1)[j]->getScore() << std::endl;
               }
            }
            PDB_COUT << "there are " << i << " partial results" << std :: endl;
            PDB_COUT << "aggregation result size=" << aggregationResult->size() << std :: endl;
            return aggregationResult;
        }


private:

        //counters to record the number of points processed
        Handle<Map<pthread_t, unsigned long>> counters;
        Handle<Vector<pthread_t>> threads;
        //current aggregated TopK values
        //each thread has a BuiltinTopKResult instance
        Handle<Map<pthread_t, Handle<BuiltinTopKResult>>> partialResults;
};

}
#endif
