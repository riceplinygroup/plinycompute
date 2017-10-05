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
#ifndef GMM_AGGREGATE_OUTPUT_H
#define GMM_AGGREGATE_OUTPUT_H

#include "Object.h"
#include "Handle.h"
#include "GMM/GmmNewComp.h"
#include "GmmModel.h"
#include "DoubleVector.h"


// By Tania, August 2017

using namespace pdb;

class GmmAggregateOutputLazy : public Object {

private:
    int key = 1;

    Handle<GmmModel> model;				//GMM model with means, covars and weights
    Handle<DoubleVector> datapoint;		//Datapoint to be processed
    Handle<GmmNewComp> newComp;			//newComp contains the partial results of
    									//processing datapoint for current model
public:

    ENABLE_DEEP_COPY

    GmmAggregateOutputLazy () {
    }

    GmmAggregateOutputLazy (Handle<GmmModel> model, Handle<DoubleVector> datapoint) {
    	this->model = model;
    	this->datapoint = datapoint;
    	this->newComp = nullptr;
    }

    GmmAggregateOutputLazy(const GmmAggregateOutputLazy & other) {
    	this->model = other.model;
    	this->datapoint = other.datapoint;
    	this->newComp = other.newComp;
    }

	int &getKey(){
		return key;
	}

	GmmAggregateOutputLazy &getValue(){
		return (*this);
	}

	//Lazy evaluation -> if newComp == null, the data point has not been processed yet
	//In that case, we process newComp and later perform the overload +

	GmmAggregateOutputLazy& operator + (GmmAggregateOutputLazy &other) {

		//std::cout << "Entering GmmAggregateOutputType operator+" << std::endl;

		if (this->newComp == nullptr) {
			//std::cout << "		Creating left GmmNewComp (NULL)" << std::endl;
			this->newComp = model->createNewComp(this->datapoint);
		}
		/*else {
			std::cout << "		*************************** left Already created!!" << std::endl;
		}*/

		if (other.newComp == nullptr) {
			addNewCompToThis(other.datapoint);
		}
		else {
			addNewCompToThis(other.newComp);
		}

		//Return left object (this) with the resulting sum

		return (*this);
	}

	void addNewCompToThis(Handle<GmmNewComp> other) {
		//std::cout << "	Entering addNewCompToThis" << std::endl;

		newComp->setLogLikelihood(newComp->getLogLikelihood() + other->getLogLikelihood());

		newComp->setSumR(newComp->getSumR() + other->getSumR());

		for (int i = 0; i < model->getNumK(); i++) {
			newComp->setWeightedX(i, newComp->getWeightedX(i) + other->getWeightedX(i));
			newComp->setWeightedX2(i, newComp->getWeightedX2(i) + other->getWeightedX2(i));
		}
		//std::cout << "	Exiting addNewCompToThis" << std::endl;

	}



	void addNewCompToThis(Handle<DoubleVector> data){

		int k = model->getNumK();
		int ndim = model->getNDim();

		//Process other datapoint
		DoubleVector r_values = DoubleVector(k); //Sum of r
		double* r_valuesptr = r_values.getRawData();

		double r;

		for (int i = 0; i < k; i++) {
			//in log space
			r = model->log_normpdf(i, data, true);
			r += log(model->getWeight(i));
			r_valuesptr[i] = r; //Update responsability
		}


		//Now normalize r
		double logLikelihood = model->logSumExp(r_values);

		this->newComp->setLogLikelihood(this->newComp->getLogLikelihood() + logLikelihood);

		double* this_rvaluesptr = this->newComp->getSumR().getRawData();

		for (int i = 0; i < k; i++) {
			r_valuesptr[i] = exp(r_valuesptr[i] - logLikelihood);
			this_rvaluesptr[i] += r_valuesptr[i];
		}

		gsl_vector_view gdata = gsl_vector_view_array(data->data->c_ptr(), ndim);
		double* dataptr = data->getRawData();

		for (int i = 0; i < k; i++) {

			//Mean = r * x
			double* weightedX = this->newComp->getWeightedX(i).getRawData();
			for (int j=0; j<ndim; j++) {
				weightedX[j] += dataptr[j] * r_valuesptr[i];
			}

			//Covar = r * x * x^T
			gsl_matrix_view gweightedX2 = gsl_matrix_view_array(this->newComp->getWeightedX2(i).getRawData(), ndim, ndim);
			gsl_blas_dsyr (CblasUpper, r_valuesptr[i], &gdata.vector, &gweightedX2.matrix);

		}

	}

	GmmNewComp getNewComp(){
		return *(this->newComp);
	}


    ~GmmAggregateOutputLazy () {
    }

};

#endif
