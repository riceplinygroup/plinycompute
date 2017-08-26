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
#include "GmmModel.h"
#include "GmmNewComp.h"
#include "GmmAggregateOutputType.h"

#include <gsl/gsl_vector.h>
#include <gsl/gsl_matrix.h>
#include <gsl/gsl_blas.h>
#include <gsl/gsl_linalg.h>


using namespace pdb;


class GmmAggregate : public ClusterAggregateComp <GmmAggregateOutputType,
						DoubleVector, int, GmmNewComp> {

private:
		Handle<GmmModel> model;

public:

        ENABLE_DEEP_COPY

        GmmAggregate () {}

        GmmAggregate (Handle<GmmModel> inputModel) {
        	std::cout << "Entering GmmAggregate constructor" << std::endl;

			this->model = inputModel;
			std::cout << "UPDATED MODEL" << std::endl;
			std::cout << "WITH K=" << this->model->getNumK() <<
					"AND NDIM="<< this->model->getNDim() << std::endl;

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
        Lambda <GmmNewComp> getValueProjection (Handle <DoubleVector> aggMe) override {

        	return makeLambda (aggMe, [&] (Handle<DoubleVector> & aggMe) {
        		//int k = (this->model).getNDim();
            	return *(this->createNewComp(aggMe));
            });
        }

        Handle<GmmNewComp> createNewComp(Handle<DoubleVector> & data){
        	/* For each component in the model, calculate:
			 * - responsabilities: r
			 * - r * x
			 * - r * x * x
			 */

        	const double EPSILON = 2.22044604925e-16;


			int k = model->getNumK();
			int ndim = model->getNDim();

			//First - Calculate responsabilities per component and normalize
			//dividing by the total sum totalR
			Handle<DoubleVector> r_values = makeObject<DoubleVector>(k); //Sum of r
			double totalR = 0.0;
			double r;

			for (int i = 0; i < k; i++) {
				r = model->getWeight(i);
				r *= model->log10_normpdf(i, data, false);
				r += EPSILON; //See Spark implementation

				totalR += r;
				r_values->setDouble(i,r); //Update responsability
			}


			//Now normalize r,
			gsl_vector_view gnewSumR = gsl_vector_view_array(r_values->data->c_ptr(), k);
			gsl_vector_scale(&gnewSumR.vector, 1/totalR);

			//And calculate r * x and r * x * x^T
			Handle<Vector<size_t>> newcount = makeObject<Vector<size_t>>(); //num data points
			Handle<Vector<DoubleVector>> newweightedX = makeObject<Vector<DoubleVector>>(); //Sum of r*x
			Handle<Vector<DoubleVector>> newweightedX2  = makeObject<Vector<DoubleVector>>(); //Sum of r*(x**2)

			gsl_vector_view gdata = gsl_vector_view_array(data->data->c_ptr(), ndim);

			for (int i = 0; i < k; i++) {

				//Mean = r * x
				Handle<DoubleVector> weightedX = makeObject<DoubleVector>(ndim);
				gsl_vector_view gweightedX = gsl_vector_view_array(weightedX->data->c_ptr(), ndim);
				gsl_vector_memcpy (&gweightedX.vector, &gdata.vector);
				gsl_vector_scale(&gweightedX.vector,r_values->getDouble(i));

				//Covar = r * x * x^T
				Handle<DoubleVector> weightedX2 = makeObject<DoubleVector>(ndim*ndim);
				gsl_matrix_view gweightedX2 = gsl_matrix_view_array(weightedX2->data->c_ptr(), ndim, ndim);

				//BLAS.syr(p(i), Vectors.fromBreeze(x), Matrices.fromBreeze(sums.sigmas(i)).asInstanceOf[DenseMatrix])
				//gsl_blas_dsyr (CBLAS_UPLO_t Uplo, double alpha, const gsl_vector * x, gsl_matrix * A) - //A = \alpha x x^T + A
				gsl_blas_dsyr (CblasUpper, r_values->getDouble(i), &gdata.vector, &gweightedX2.matrix);

				//Copy lower triangular
				for (int row=0; row<ndim; row++){
					for (int col=row+1; col<ndim; col++){
						//matrix[j][i] = matrix[i][j]
						double d = gsl_matrix_get(&gweightedX2.matrix,row,col);
						gsl_matrix_set(&gweightedX2.matrix,col,row, d);
					}
				}

				newweightedX->push_back(*weightedX); //r * x
				newweightedX2->push_back(*weightedX2); //r * x * x^T
			}


			Handle<GmmNewComp> result = makeObject<GmmNewComp>
					(*r_values, *newweightedX, *newweightedX2);

			return result;

        }
};


#endif
