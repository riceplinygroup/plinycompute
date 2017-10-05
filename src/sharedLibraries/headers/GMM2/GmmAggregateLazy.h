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
#ifndef GMM_AGGREGATE_H
#define GMM_AGGREGATE_H

//by Tania, August 2017

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "ClusterAggregateComp.h"
#include "DoubleVector.h"

#include "GmmAggregateOutputLazy.h"

#include <gsl/gsl_vector.h>
#include <gsl/gsl_matrix.h>
#include <gsl/gsl_blas.h>
#include <gsl/gsl_linalg.h>

#include "GmmModel.h"



using namespace pdb;


class GmmAggregateLazy : public ClusterAggregateComp <GmmAggregateOutputLazy,
						DoubleVector, int, GmmAggregateOutputLazy> {


private:
		Handle<GmmModel> model;

public:

        ENABLE_DEEP_COPY

        GmmAggregateLazy () {}

        GmmAggregateLazy (Handle<GmmModel> inputModel) {
        	std::cout << "Entering GmmAggregate constructor" << std::endl;

			this->model = inputModel;
			std::cout << "UPDATED MODEL" << std::endl;
			std::cout << "WITH K=" << this->model->getNumK() <<
					"AND NDIM="<< this->model->getNDim() << std::endl;

			this->model->calcInvCovars();

        	std::cout << "Exiting GmmAggregate constructor" << std::endl;



        }


        // the key type must have == and size_t hash () defined
        Lambda <int> getKeyProjection (Handle <DoubleVector> aggMe) override {
			// Same key for all intermediate objects. The output is a single
			// GmmNewComp object with the info related to all components

			return makeLambda (aggMe, [] (Handle<DoubleVector> & aggMe) {
				return (1);
			});
        }

        // the value type must have + defined
        Lambda <GmmAggregateOutputLazy> getValueProjection (Handle <DoubleVector> aggMe) override {

        	return makeLambda (aggMe, [&] (Handle<DoubleVector> & aggMe) {
        		Handle<GmmAggregateOutputLazy> result = makeObject<GmmAggregateOutputLazy>(model,aggMe);
        		return *(result);
            });
        }
};


#endif
