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
#ifndef CENTROID_H
#define CENTROID_H

//by Jia, Jan 2nd, 2017

#ifndef NUM_DIMENSIONS
#define NUM_DIMENSIONS 100
#endif



#include "Object.h"
#include <stdlib.h>
#include <time.h>

class Centroid {

    public:

        Centroid() {}

        Centroid(double * point, int count) {

            this->count = count;
            
            int i;
            for ( i = 0; i < NUM_DIMENSIONS; i++ ) {
                 sum[i] = point[i];
            }

        }


        void initialize() {

            count = 0;  

            int i;
            for ( i = 0; i < NUM_DIMENSIONS; i++ ) {
                sum[i] = 0;
            }

        }

        //srand must be called before invoking following method
        void initRandom() {


            count = -1; //this means the centroid is a current value, not for aggregation

            int i;
            for ( i = 0; i < NUM_DIMENSIONS; i++ ) {
                sum[i] = rand()/double(RAND_MAX);
            }

        }

        void update( double* point ) {
            
            count ++;

            int i;
            for ( i = 0; i < NUM_DIMENSIONS; i++ ) {
                sum[i] += point[i];
            }            

        }

        double computeDistance( double* point ) {

            //we only want to compute distance for non-aggregation centroid
            if (count >= 0 ) {
                return -1;
            }

            double distance = 0;
            
            int i;
            for ( i = 0; i < NUM_DIMENSIONS; i++ ) {
                distance += (sum[i]-point[i])*(sum[i]-point[i]);
            }

            return distance;

        }

        void print() {
            int i;
            std :: cout << "count=" << count << std :: endl;
            for ( i = 0; i < NUM_DIMENSIONS; i++ ) {
                std :: cout << i << ": " << sum[i] << ";";
            }
            std :: cout << std :: endl;
        }


    private:

        double sum[NUM_DIMENSIONS];

        int count;

};


#endif
