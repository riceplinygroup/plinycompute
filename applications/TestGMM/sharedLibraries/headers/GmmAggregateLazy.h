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
#ifndef GMM_AGGREGATE_LAZY_H
#define GMM_AGGREGATE_LAZY_H

// by Tania, October 2017

#include "AggregateComp.h"
#include "DoubleVector.h"
#include "Lambda.h"
#include "LambdaCreationFunctions.h"

#include "GmmAggregateOutputLazy.h"

#include "GmmModel.h"

using namespace pdb;

// This class contains the MAIN logic for GMM:
// It performs the Expectation-Maximization Steps
// The output is later used to update the weights, means and covars of the new
// model
class GmmAggregateLazy
    : public AggregateComp<GmmAggregateOutputLazy, DoubleVector, int, GmmAggregateOutputLazy> {

private:
  Handle<GmmModel> model;

public:
  ENABLE_DEEP_COPY

  GmmAggregateLazy() {}

  GmmAggregateLazy(Handle<GmmModel> inputModel) {
    std::cout << "Entering GmmAggregate constructor" << std::endl;

    this->model = inputModel;
    std::cout << "UPDATED MODEL" << std::endl;
    std::cout << "WITH K=" << this->model->getNumK()
              << "AND NDIM=" << this->model->getNDim() << std::endl;

    // this->model->calcInvCovars();
    std::cout << "Exiting GmmAggregate constructor" << std::endl;
  }

  Lambda<int> getKeyProjection(Handle<DoubleVector> aggMe) override {
    // Same key for all intermediate objects. The output is a single
    // GmmAggregateOutputLazy object with the info related to all components

    return makeLambda(aggMe, [](Handle<DoubleVector> &aggMe) { return (1); });
  }

  // We calculate the responsability (or posterior probability) of each
  // datapoint
  // for each component, and calculate the aggregated sums that will later be
  // used
  // to update the weights, means and covars
  Lambda<GmmAggregateOutputLazy>
  getValueProjection(Handle<DoubleVector> aggMe) override {

    return makeLambda(aggMe, [&](Handle<DoubleVector> &aggMe) {

      int k = model->getNumK();

      // Calculate responsabilities per component and normalize
      // dividing by the total sum totalR
      Vector<double> r_values(k, k);
      double *r_valuesptr = r_values.c_ptr();

      for (int i = 0; i < k; i++) {
        r_valuesptr[i] =
            model->log_normpdf(i, aggMe) + log(model->getWeight(i));
      }

      // Now normalize r (in log space)
      double logLikelihood = model->logSumExp(r_values);

      for (int i = 0; i < k; i++) {
        r_valuesptr[i] = exp(r_valuesptr[i] - logLikelihood);
      }

      // GmmAggregateDatapoint contains a pointer to the datapoint
      // and the responsabilities
      // The aggregate (sum) of those values is done lazily in the operator+
      Handle<GmmAggregateDatapoint> aggDatapoint =
          makeObject<GmmAggregateDatapoint>(*aggMe, r_values, logLikelihood);
      Handle<GmmAggregateOutputLazy> result =
          makeObject<GmmAggregateOutputLazy>(aggDatapoint);
      return *(result);
    });
  }
};

#endif
