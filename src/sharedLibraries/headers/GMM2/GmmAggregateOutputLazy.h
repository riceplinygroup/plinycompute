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
#include "PDBVector.h"
#include "GmmAggregateDatapoint.h"
#include "GmmAggregateNewComp.h"



// By Tania, October 2017

using namespace pdb;

class GmmAggregateOutputLazy : public Object {

private:
    int key = 1;

    Handle<GmmAggregateDatapoint> aggDatapoint;		//Datapoint to be processed and responsabilities
    Handle<GmmAggregateNewComp> newComp;			//newComp contains the partial results of
    									//processing datapoint for current model
public:

    ENABLE_DEEP_COPY

    GmmAggregateOutputLazy () {
    }

    GmmAggregateOutputLazy (Handle<GmmAggregateDatapoint>& aggDatapoint) {
    	this->aggDatapoint = aggDatapoint;
    	this->newComp = nullptr;
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

		int dim = this->aggDatapoint->getDatapoint().size;
		int k = this->aggDatapoint->getRvalues().size();


		//if LHS == NULL, process LHS -> Calculate sumMean and sumCovar
		if (this->newComp == nullptr) {

			this->newComp = makeObject<GmmAggregateNewComp>(k,dim);

			gsl_vector_view gdata = gsl_vector_view_array(this->aggDatapoint->getDatapoint().getRawData(), dim);
			double * r_valuesptr = this->aggDatapoint->getRvalues().c_ptr();

			for (int i=0; i<k; i++) {

				//Covar = r * x * x^T
				gsl_matrix_view gsumCovar = gsl_matrix_view_array(this->newComp->getSumCovar(i).c_ptr(), dim, dim);
				gsl_blas_dsyr (CblasUpper, r_valuesptr[i], &gdata.vector, &gsumCovar.matrix);

				//Mean = r * x
				gsl_vector_view gsumMean = gsl_vector_view_array(this->newComp->getSumMean(i).c_ptr(), dim);
				gsl_blas_daxpy (r_valuesptr[i], &gdata.vector, &gsumMean.vector);

				//std::cout << "rvalues " << i << " " << r_valuesptr[i] << std::endl;
				//std::cout << "data " << i << " "; this->aggDatapoint->getDatapoint().print();
				//std::cout << "mean " << i << " "; this->newComp->getSumMean(i).print();

				this->newComp->setSumWeights(i, r_valuesptr[i]);
			}
			this->newComp->setLogLikelihood(this->aggDatapoint->getLogLikelihood());
		}


		//if RHS == NULL, process RHS and add it to LHS
		if (other.newComp == nullptr) {
			gsl_vector_view gotherdata = gsl_vector_view_array(other.aggDatapoint->getDatapoint().getRawData(), dim);

			for (int i=0; i<k; i++) {

				//Covar = r * x * x^T
				gsl_matrix_view gsumCovar = gsl_matrix_view_array(this->newComp->getSumCovar(i).c_ptr(), dim, dim);
				gsl_blas_dsyr (CblasUpper, other.aggDatapoint->getRvalues()[i], &gotherdata.vector, &gsumCovar.matrix);


				//Mean = r * x
				gsl_vector_view gsumMean = gsl_vector_view_array(this->newComp->getSumMean(i).c_ptr(), dim);
				gsl_blas_daxpy (other.aggDatapoint->getRvalues()[i], &gotherdata.vector, &gsumMean.vector);

				//Sum r
				this->newComp->setSumWeights(i, this->newComp->getSumWeights(i) + other.aggDatapoint->getRvalues()[i]);
			}
			//Set loglikelihood
			this->newComp->setLogLikelihood(this->newComp->getLogLikelihood() + other.aggDatapoint->getLogLikelihood());
		}

		//Return LHS (this) with the resulting sum

		return (*this);
	}



	GmmAggregateNewComp getNewComp(){
		return *(this->newComp);
	}


    ~GmmAggregateOutputLazy () {
    }

};

#endif
