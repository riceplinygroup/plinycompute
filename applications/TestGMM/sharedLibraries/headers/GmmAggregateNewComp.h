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
#ifndef GMM_AGGREGATE_NEWCOMP_H
#define GMM_AGGREGATE_NEWCOMP_H

#include "Object.h"
#include "PDBVector.h"

#include <gsl/gsl_blas.h>
#include <gsl/gsl_linalg.h>
#include <gsl/gsl_matrix.h>
#include <gsl/gsl_vector.h>

// By Tania, October 2017

using namespace pdb;

// This class contains the partial results for the GMM new model, that is,
// sumWeights, sumMeans and sumCovars that will be later used to update the new
// model.
class GmmAggregateNewComp : public Object {

private:
  double logLikelihood;
  Vector<double> sumWeights;        //[k]
  Vector<Vector<double>> sumMeans;  // r*datapoint [dim]
  Vector<Vector<double>> sumCovars; // r*datapoint*datapoint^T [dim*dim]

public:
  ENABLE_DEEP_COPY

  GmmAggregateNewComp() {}

  GmmAggregateNewComp(int k, int dim) {
    logLikelihood = 0;
    sumWeights = Vector<double>(k, k);
    for (int i = 0; i < k; i++) {
      sumMeans.push_back(Vector<double>(dim, dim));
      sumCovars.push_back(Vector<double>(dim * dim, dim * dim));
    }
  }

  double getLogLikelihood() { return this->logLikelihood; }

  void setLogLikelihood(double logLikelihood) {
    this->logLikelihood = logLikelihood;
  }

  Vector<double> &getSumWeights() { return sumWeights; }

  double getSumWeights(int index) { return sumWeights[index]; }

  void setSumWeights(int index, double val) { sumWeights[index] = val; }

  Vector<double> &getSumMean(int index) { return sumMeans[index]; }

  void setSumMean(int index, Vector<double> &sumMean) {
    this->sumMeans[index] = sumMean;
  }

  Vector<double> &getSumCovar(int index) { return sumCovars[index]; }

  void setSumCovar(int index, Vector<double> &sumCovar) {
    this->sumCovars[index] = sumCovar;
  }

  ~GmmAggregateNewComp() {}
};

#endif
