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
#ifndef GMM_AGGREGATE_DATAPOINT_H
#define GMM_AGGREGATE_DATAPOINT_H

#include "DoubleVector.h"
#include "Object.h"
#include "PDBVector.h"

// By Tania, October 2017

using namespace pdb;

// GmmAggregateDatapoint contains a processed datapoint and the responsabilities
// or posterior probabilities (rvalues) of this datapoint for each model
// component
class GmmAggregateDatapoint : public Object {

private:
  DoubleVector datapoint; // Datapoint to be processed
  Vector<double> rvalues; // Responsabilities
  double logLikelihood;

public:
  ENABLE_DEEP_COPY

  GmmAggregateDatapoint() {}

  GmmAggregateDatapoint(DoubleVector datapoint, Vector<double> rvalues,
                        double logLikelihood) {
    this->datapoint = datapoint;
    this->rvalues = rvalues;
    this->logLikelihood = logLikelihood;
  }

  DoubleVector &getDatapoint() { return this->datapoint; }

  Vector<double> &getRvalues() { return this->rvalues; }

  double getLogLikelihood() { return this->logLikelihood; }

  void setLogLikelihood(double logLikelihood) {
    this->logLikelihood = logLikelihood;
  }

  ~GmmAggregateDatapoint() {}
};

#endif
