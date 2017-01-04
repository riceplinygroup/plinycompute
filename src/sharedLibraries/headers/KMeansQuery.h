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

#ifndef KMEANS_QUERY_H
#define KMEANS_QUERY_H

//by Jia, Jan 2nd, 2017

#ifndef MAX_THREADS
   #define MAX_THREADS 8
#endif

#ifndef NUM_DIMENSIONS
#define NUM_DIMENSIONS 100
#endif

#ifndef NUM_CLUSTERS
#define NUM_CLUSTERS 10
#endif

#include "Centroid.h"
#include "PartialResult.h"
#include "Selection.h"
#include "Supervisor.h"
#include <pthread.h>
#include <stdlib.h>
#include <time.h>

using namespace pdb;
class KMeansQuery : public Selection <PartialResult, double [NUM_DIMENSIONS]> {

public:

       ENABLE_DEEP_COPY

       KMeansQuery () { 
            srand((unsigned int)(time(NULL)));
            int i = 0;

            for (i = 0; i < NUM_CLUSTERS; i ++) {
                currentCentroids[i].initRandom();
            }      

            counters = makeObject<Map<pthread_t, int>>(MAX_THREADS);
            partialResults = makeObject<Map<pthread_t, Handle<PartialResult>>>(MAX_THREADS);            

       }

       KMeansQuery (Centroid * currentCentroids) {
           int i = 0;
           for ( i = 0; i < NUM_CLUSTERS; i ++) {
               this->currentCentroids[i] = currentCentroids[i];
           }
           counters = makeObject<Map<pthread_t, int>>(MAX_THREADS);
           partialResults = makeObject<Map<pthread_t, Handle<PartialResult>>>(MAX_THREADS);
       }

       int findClosestCluster (double* point) {
       
           int i;
           double minDistance = DBL_MAX;
           int bestClusterIndex = -1;
           
           double curDistance;
           for ( i = 0; i < NUM_CLUSTERS; i ++ ) {
               if ((curDistance = currentCentroids[i].computeDistance(point)) < minDistance) {
                   bestClusterIndex = i;
                   minDistance = curDistance;
               }   
           }   
           
           return bestClusterIndex;
           
       }




	SimpleLambda <bool> getSelection (Handle <double [NUM_DIMENSIONS]> &checkMe) override {
		return makeLambda (checkMe, [&] () {
                    return false; //getSelection will not be applied in pipeline, so simply return false
		});
	}


        SimpleLambda <bool> getProjectionSelection (Handle<PartialResult> &checkMe) override {
                return makeLambda (checkMe, [&] () {
                        if (checkMe == nullptr) {
                            return false;
                        } else {
                            return true;
                        }
                });
        }



	SimpleLambda <Handle <PartialResult>> getProjection (Handle <double [NUM_DIMENSIONS]> &checkMe) override {
                 
		return makeLambda (checkMe, [&] {
                        pthread_t threadId = pthread_self();
                        if (counters->count(threadId) == 0) {
                            (*counters)[threadId] = 0;
                            Handle<PartialResult> partialResult = makeObject<PartialResult> ();
                            partialResult->initialize();
                            (*partialResults)[threadId] = partialResult;
                        }

                        if ((*counters)[threadId] == 10000) {
                            (*partialResults)[threadId]->initialize(); //to clear partial results for last emission
                            (*counters)[threadId] = 0; // to clear counter for last emission
                        }
                        int clusterIndex = findClosestCluster(*checkMe);
                        (*partialResults)[threadId]->updateCentroid(clusterIndex, *checkMe);
                        (*counters)[threadId] ++;
                        Handle<PartialResult> ret;
                        if ((*counters)[threadId] == 10000) {
                            ret = makeObject<PartialResult>();
			    *ret = *((*partialResults)[threadId]);
                        } else {
                            ret = nullptr;
                        }
                        return ret;
		});
	}


private:

        //counters to record the number of points processed
        Handle<Map<pthread_t, int>> counters;

        //current centroid
        Centroid currentCentroids[NUM_CLUSTERS];

        //current aggregated centroids
        //each worker has a PartialResult instance
        Handle<Map<pthread_t, Handle<PartialResult>>> partialResults;

};


#endif
