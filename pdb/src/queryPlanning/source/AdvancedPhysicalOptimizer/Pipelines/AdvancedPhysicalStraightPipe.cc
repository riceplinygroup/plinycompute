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

#include <AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalPipelineAlgorithm.h>
#include "AdvancedPhysicalOptimizer/Pipes/AdvancedPhysicalStraightPipe.h"

namespace pdb {

AdvancedPhysicalStraightPipe::AdvancedPhysicalStraightPipe(string &jobId,
                                                                   Handle<ComputePlan> &computePlan,
                                                                   LogicalPlanPtr &logicalPlan,
                                                                   ConfigurationPtr &conf,
                                                                   vector<AtomicComputationPtr> &pipeComputations,
                                                                   size_t id) :
                                                                   AdvancedPhysicalAbstractPipe(jobId,
                                                                                                    computePlan,
                                                                                                    logicalPlan,
                                                                                                    conf,
                                                                                                    pipeComputations,
                                                                                                    id) {}

AdvancedPhysicalAbstractAlgorithmPtr AdvancedPhysicalStraightPipe::selectOutputAlgorithm() {

  selectedAlgorithm = std::make_shared<AdvancedPhysicalPipelineAlgorithm>(getAdvancedPhysicalNodeHandle(),
                                                                           jobId,
                                                                           isJoining(),
                                                                           consumers.empty(),
                                                                           sourceSetIdentifier,
                                                                           pipeComputations,
                                                                           computePlan,
                                                                           logicalPlan,
                                                                           conf);

  return selectedAlgorithm;
}

std::vector<AdvancedPhysicalAbstractAlgorithmPtr> AdvancedPhysicalStraightPipe::getPossibleAlgorithms(const StatisticsPtr &stats) {

  // currently every straight pipeline is realized as a selection
  std::vector<AdvancedPhysicalAbstractAlgorithmPtr> ret = { std::make_shared<AdvancedPhysicalPipelineAlgorithm>(getAdvancedPhysicalNodeHandle(),
                                                                                                                 jobId,
                                                                                                                 isJoining(),
                                                                                                                 consumers.empty(),
                                                                                                                 sourceSetIdentifier,
                                                                                                                 pipeComputations,
                                                                                                                 computePlan,
                                                                                                                 logicalPlan,
                                                                                                                 conf) };

  return ret;
}

AdvancedPhysicalAbstractAlgorithmPtr AdvancedPhysicalStraightPipe::propose(std::vector<AdvancedPhysicalAbstractAlgorithmPtr> algorithms) {

  //TODO this is just some placeholder logic to select the broadcast join if we can
  AdvancedPhysicalAbstractAlgorithmPtr best = nullptr;

  // go through each algorithm
  for (const auto &algorithm : algorithms) {

    // we prefer the broadcast algorithm, but if we have none we are fine
    if (algorithm->getType() == JOIN_BROADCASTED_HASHSET_ALGORITHM || best == nullptr) {

      // select the best algorithm
      best = algorithm;
    }
  }

  // if this is false there is something seriously wrong with our system
  assert(best != nullptr);

  // return the chosen algorithm
  return best;
}

AdvancedPhysicalPipelineTypeID AdvancedPhysicalStraightPipe::getType() {
  return STRAIGHT;
}

}