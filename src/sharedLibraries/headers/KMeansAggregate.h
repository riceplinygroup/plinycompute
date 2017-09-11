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
#ifndef K_MEANS_AGGREGATE_H
#define K_MEANS_AGGREGATE_H

//by Shangyu, May 2017

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "ClusterAggregateComp.h"
#include "KMeansDoubleVector.h"
#include "limits.h"
#include "KMeansCentroid.h"
#include "KMeansAggregateOutputType.h"



using namespace pdb;


class KMeansAggregate : public ClusterAggregateComp <KMeansAggregateOutputType, KMeansDoubleVector, int, KMeansCentroid> {

private:

        Vector<KMeansDoubleVector> model;

public:

        ENABLE_DEEP_COPY

        KMeansAggregate () {}

        KMeansAggregate (Handle<Vector<Handle<KMeansDoubleVector>>>& inputModel) {
                if (model.size() > 0) {
                    model.clear();
                }
                for (int i = 0; i < inputModel->size(); i++) {
                     model.push_back(*(*inputModel)[i]);

                }
		
	
                /*std :: cout << "The model I get is: " << std :: endl;
                for(int i = 0; i < (this->model).size(); i ++) {
                        (this->model)[i].print();
                }*/
	

        }


        // the key type must have == and size_t hash () defined
        Lambda <int> getKeyProjection (Handle <KMeansDoubleVector> aggMe) override {
                return makeLambda (aggMe, [&] (Handle<KMeansDoubleVector> & aggMe) {return this->computeClusterMemberOptimized(*aggMe);});
        }

        // the value type must have + defined
        Lambda <KMeansCentroid> getValueProjection (Handle <KMeansDoubleVector> aggMe) override {
            	/*return makeLambda (aggMe, [] (Handle<DoubleVector> & aggMe) {
			Handle<KMeansCentroid> result = makeObject<KMeansCentroid>(1, *aggMe);
			return *result;
		}); */

                return makeLambda (aggMe, [] (Handle<KMeansDoubleVector> & aggMe) {
                        return KMeansCentroid(1, *aggMe);
                }); 
        }

        int computeClusterMember(Handle<KMeansDoubleVector> data) {
        	int closestDistance = INT_MAX;
        	int cluster = 0;
		/*
	        std :: cout << "my data is: " << std :: endl;
		data->print();
	        std :: cout << "my model is: " << std :: endl;
		for(int j = 0; j < (this->model).size(); j ++) {
			(this->model)[j]->print();
		}
		*/
                Vector<KMeansDoubleVector> & myModel = this->model;
                KMeansDoubleVector & myData = *data;	
                size_t modelSize = myModel.size();
        	for(int j = 0; j < modelSize; j ++) {
        		KMeansDoubleVector & mean = myModel[j];
			double distance = myData.getSquaredDistance(mean);

			if (distance < closestDistance) {
				closestDistance = distance;
				cluster = j;
			}
			
        	}
//		std :: cout << "my cluster is: " << cluster << std :: endl;
        	return cluster;
        }

        //JiaNote: add this to be consistent with Spark MLLib
        int computeClusterMemberOptimized(KMeansDoubleVector & data) {
                int closestDistance = INT_MAX;
                int cluster = 0;
                Vector<KMeansDoubleVector> & myModel = model;
                size_t modelSize = myModel.size();
                for(int i = 0; i < modelSize; i ++) {
                        KMeansDoubleVector & mean = myModel[i];
                        double lowerBoundOfSqDist = mean.norm - data.norm;
                        lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist;
                        if (lowerBoundOfSqDist < closestDistance) {
                            double distance = data.getFastSquaredDistance(mean);
                            if (distance < closestDistance) {
                                closestDistance = distance;
                                cluster = i;
                            }
                        }
                }
                return cluster;
        }


};


#endif
