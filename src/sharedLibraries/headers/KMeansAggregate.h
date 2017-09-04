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
#include "DoubleVector.h"
#include "limits.h"
#include "KMeansCentroid.h"
#include "KMeansAggregateOutputType.h"



using namespace pdb;


class KMeansAggregate : public ClusterAggregateComp <KMeansAggregateOutputType, DoubleVector, int, KMeansCentroid> {

private:
		Vector<Handle<DoubleVector>> model;

public:

        ENABLE_DEEP_COPY

        KMeansAggregate () {}

/*        KMeansAggregate (Handle<Vector<DoubleVector>> inputModel) {
        	this->model = *inputModel;
	        std :: cout << "The model I get is: " << std :: endl;
		for(int j = 0; j < (this->model).size(); j ++) {
			(this->model)[j].print();
		}	

        }*/

        KMeansAggregate (Handle<Vector<Handle<DoubleVector>>>& inputModel) {
                if (model.size() > 0) {
                    model.clear();
                }
                for (int i = 0; i < inputModel->size(); i++) {
                     model.push_back((*inputModel)[i]);

                }
		
		/*
                std :: cout << "The model I get is: " << std :: endl;
                for(int j = 0; j < (this->model).size(); j ++) {
                        (this->model)[j]->print();
                }
		*/

        }


        // the key type must have == and size_t hash () defined
        Lambda <int> getKeyProjection (Handle <DoubleVector> aggMe) override {
                return makeLambda (aggMe, [&] (Handle<DoubleVector> & aggMe) {return this->computeClusterMemberOptimized(aggMe);});
        }

        // the value type must have + defined
        Lambda <KMeansCentroid> getValueProjection (Handle <DoubleVector> aggMe) override {
            	return makeLambda (aggMe, [] (Handle<DoubleVector> & aggMe) {
			Handle<KMeansCentroid> result = makeObject<KMeansCentroid>(1, *aggMe);
			return *result;
		});
        }

        int computeClusterMember(Handle<DoubleVector> data) {
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
                size_t modelSize = (this->model).size();
        	for(int j = 0; j < modelSize; j ++) {
        		Handle<DoubleVector> mean = (this->model)[j];
			double distance = data->getSquaredDistance(*mean);

			if (distance < closestDistance) {
				closestDistance = distance;
				cluster = j;
			}
			
        	}
//		std :: cout << "my cluster is: " << cluster << std :: endl;
        	return cluster;
        }


        int computeClusterMemberOptimized(Handle<DoubleVector> data) {
                int closestDistance = INT_MAX;
                int cluster = 0;
                 size_t modelSize = (this->model).size();
                for(int j = 0; j < modelSize; j ++) {
                        Handle<DoubleVector> mean = (this->model)[j];
                        double lowerBoundOfSqDist = mean->norm - data->norm;
                        lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist;
                        if (lowerBoundOfSqDist < closestDistance) {
                            double distance = data->getFastSquaredDistance(*mean);
                            if (distance < closestDistance) {
                                closestDistance = distance;
                                cluster = j;
                            }
                        }
                }
                return cluster;
        }


};


#endif
