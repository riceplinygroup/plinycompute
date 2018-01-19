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

#include "GmmAggregateDatapoint.h"
#include "GmmAggregateNewComp.h"
#include "Handle.h"
#include "Object.h"
#include "PDBVector.h"

// By Tania, October 2017

using namespace pdb;

// GmmAggregateOutputLazy is the output class from the GmmAggregate
// It contains two objects, the datapoint that is processed in the first step
// and the newComp that contains the partial results calculated during
// the operator+

// When performing GmmAggregate, we could do directly generate an output object
// with the all the partial vectors (sumWeights, sumMeans, sumCovars). However,
// this would require allocating too much memory space for vectors (intermediate
// data).

// Instead, we let the GmmAggregate to only calculate a small vector of
// responsabilities
// for each datapoint. This is stored in aggDatapoint.
// Then, in a second step, during the operator+, we reuse existing vectors (from
// LHS) for the
// partial calculations (sumWeights, sumMeans, sumCovars). This is stored in
// newComp

class GmmAggregateOutputLazy : public Object {

private:
  int key = 1;

  Handle<GmmAggregateDatapoint>
      aggDatapoint; // Datapoint to be processed and responsabilities
  Handle<GmmAggregateNewComp>
      newComp; // newComp contains the partial results of
               // processing datapoint for current model
public:
  ENABLE_DEEP_COPY

  GmmAggregateOutputLazy() {}

  GmmAggregateOutputLazy(Handle<GmmAggregateDatapoint> &aggDatapoint) {
    this->aggDatapoint = aggDatapoint;
    this->newComp = nullptr;
  }

  int &getKey() { return key; }

  GmmAggregateOutputLazy &getValue() { return (*this); }

  // Lazy evaluation -> if newComp == null, the data point has not been
  // processed yet
  // In that case, we process newComp and later perform the overload +

  GmmAggregateOutputLazy &operator+(GmmAggregateOutputLazy &other) {

    // if LHS == NULL, process LHS -> Calculate sumMean and sumCovar
    // This means that the datapoint has not been processed yet,
    // so we do it
    if (this->aggDatapoint != nullptr && this->newComp == nullptr) {

      int dim = this->aggDatapoint->getDatapoint().size;
      int k = this->aggDatapoint->getRvalues().size();

      this->newComp = makeObject<GmmAggregateNewComp>(k, dim);

      gsl_vector_view gdata = gsl_vector_view_array(
          this->aggDatapoint->getDatapoint().getRawData(), dim);
      double *r_valuesptr = this->aggDatapoint->getRvalues().c_ptr();

      for (int i = 0; i < k; i++) {

        // Covar = r * x * x^T
        gsl_matrix_view gsumCovar = gsl_matrix_view_array(
            this->newComp->getSumCovar(i).c_ptr(), dim, dim);
        gsl_blas_dsyr(CblasUpper, r_valuesptr[i], &gdata.vector,
                      &gsumCovar.matrix);

        // Mean = r * x
        gsl_vector_view gsumMean =
            gsl_vector_view_array(this->newComp->getSumMean(i).c_ptr(), dim);
        gsl_blas_daxpy(r_valuesptr[i], &gdata.vector, &gsumMean.vector);

        this->newComp->setSumWeights(i, r_valuesptr[i]);
      }
      this->newComp->setLogLikelihood(this->aggDatapoint->getLogLikelihood());

      // Now mark as processed
      this->aggDatapoint = nullptr;
    }

    // In case that LHS has been processed, but RHS has not been processed yet,
    // we calculate the partial results for RHS and sum them to LHS. In this
    // way,
    // we are avoiding allocating vectors for RHS

    // if RHS == NULL, process RHS and add it to LHS
    if (other.aggDatapoint != nullptr && other.newComp == nullptr) {

      int dim = other.aggDatapoint->getDatapoint().size;
      int k = other.aggDatapoint->getRvalues().size();

      gsl_vector_view gotherdata = gsl_vector_view_array(
          other.aggDatapoint->getDatapoint().getRawData(), dim);

      for (int i = 0; i < k; i++) {

        // Covar = r * x * x^T
        gsl_matrix_view gsumCovar = gsl_matrix_view_array(
            this->newComp->getSumCovar(i).c_ptr(), dim, dim);
        gsl_blas_dsyr(CblasUpper, other.aggDatapoint->getRvalues()[i],
                      &gotherdata.vector, &gsumCovar.matrix);

        // Mean = r * x
        gsl_vector_view gsumMean =
            gsl_vector_view_array(this->newComp->getSumMean(i).c_ptr(), dim);
        gsl_blas_daxpy(other.aggDatapoint->getRvalues()[i], &gotherdata.vector,
                       &gsumMean.vector);

        // Sum r
        this->newComp->setSumWeights(i,
                                     this->newComp->getSumWeights(i) +
                                         other.aggDatapoint->getRvalues()[i]);
      }
      // Set loglikelihood
      this->newComp->setLogLikelihood(this->newComp->getLogLikelihood() +
                                      other.aggDatapoint->getLogLikelihood());
    }

    // Just sum LHS and RHS
    // Just sum both
    else { // this->aggDatapoint == nullptr and/or other->aggDatapoint ==
           // nullptr

      int dim = this->newComp->getSumMean(0).size();
      int k = this->newComp->getSumWeights().size();

      for (int i = 0; i < k; i++) {
        // Covar = r * x * x^T
        gsl_vector_view gsumCovar = gsl_vector_view_array(
            this->newComp->getSumCovar(i).c_ptr(), dim * dim);
        gsl_vector_view gOtherSumCovar = gsl_vector_view_array(
            other.newComp->getSumCovar(i).c_ptr(), dim * dim);
        gsl_blas_daxpy(1.0, &gOtherSumCovar.vector, &gsumCovar.vector);

        // Mean = r * x
        gsl_vector_view gsumMean =
            gsl_vector_view_array(this->newComp->getSumMean(i).c_ptr(), dim);
        gsl_vector_view gOtherSumMean =
            gsl_vector_view_array(other.newComp->getSumMean(i).c_ptr(), dim);

        gsl_blas_daxpy(1.0, &gOtherSumMean.vector, &gsumMean.vector);

        // Sum r
        this->newComp->setSumWeights(i, this->newComp->getSumWeights(i) +
                                            other.newComp->getSumWeights(i));
      }
      // Set loglikelihood
      this->newComp->setLogLikelihood(this->newComp->getLogLikelihood() +
                                      other.newComp->getLogLikelihood());
    }

    // Return LHS (this) with the resulting sum
    return (*this);
  }

  GmmAggregateNewComp getNewComp() { return *(this->newComp); }

  ~GmmAggregateOutputLazy() {}
};

#endif
