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

#include <AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractAlgorithm.h>
#include <AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalJoinBroadcastedHashsetAlgorithm.h>
#include <AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalShuffledHashsetPipelineAlgorithm.h>
#include <AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalShuffleSetAlgorithm.h>
#include <AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalPipelineAlgorithm.h>
#include "AdvancedPhysicalOptimizer/Pipes/AdvancedPhysicalJoinSidePipe.h"

namespace pdb {

AdvancedPhysicalJoinSidePipe::AdvancedPhysicalJoinSidePipe(string &jobId,
                                                           Handle<ComputePlan> &computePlan,
                                                           LogicalPlanPtr &logicalPlan,
                                                           ConfigurationPtr &conf,
                                                           vector<AtomicComputationPtr> &pipeComputations,
                                                           size_t id) : AdvancedPhysicalAbstractPipe(jobId,
                                                                                                     computePlan,
                                                                                                     logicalPlan,
                                                                                                     conf,
                                                                                                     pipeComputations,
                                                                                                     id) {}

AdvancedPhysicalAbstractAlgorithmPtr AdvancedPhysicalJoinSidePipe::selectOutputAlgorithm() {

  // a join side is never an output
  static_assert(true, "this is not supposed to happen");

  // return a dummy
  return pdb::AdvancedPhysicalAbstractAlgorithmPtr();
}

vector<AdvancedPhysicalAbstractAlgorithmPtr> AdvancedPhysicalJoinSidePipe::getPossibleAlgorithms(const StatisticsPtr &stats) {

  // all the algorithms that we can use
  vector<AdvancedPhysicalAbstractAlgorithmPtr> algorithms;

  // check if we can use a broadcast algorithm
  if (getCost(stats) < BROADCAST_JOIN_COST_THRESHOLD) {
    algorithms.push_back(std::make_shared<AdvancedPhysicalJoinBroadcastedHashsetAlgorithm>(getAdvancedPhysicalNodeHandle(),
                                                                                           jobId,
                                                                                           isJoining(),
                                                                                           consumers.empty(),
                                                                                           sourceSetIdentifier,
                                                                                           pipeComputations,
                                                                                           computePlan,
                                                                                           logicalPlan,
                                                                                           conf));
  }

  // we can always do a shuffle algorithm and then build a hash set
  algorithms.push_back(std::make_shared<AdvancedPhysicalShuffledHashsetPipelineAlgorithm>(getAdvancedPhysicalNodeHandle(),
                                                                                          jobId,
                                                                                          isJoining(),
                                                                                          consumers.empty(),
                                                                                          sourceSetIdentifier,
                                                                                          pipeComputations,
                                                                                          computePlan,
                                                                                          logicalPlan,
                                                                                          conf));

  // we can always do a hash shuffle partitioning
  algorithms.push_back(std::make_shared<AdvancedPhysicalShuffleSetAlgorithm>(getAdvancedPhysicalNodeHandle(),
                                                                             jobId,
                                                                             isJoining(),
                                                                             consumers.empty(),
                                                                             sourceSetIdentifier,
                                                                             pipeComputations,
                                                                             computePlan,
                                                                             logicalPlan,
                                                                             conf));

  // we can always do a pipeline algorithm on this single pipe
  algorithms.push_back(std::make_shared<AdvancedPhysicalPipelineAlgorithm>(getAdvancedPhysicalNodeHandle(),
                                                                           jobId,
                                                                           isJoining(),
                                                                           consumers.empty(),
                                                                           sourceSetIdentifier,
                                                                           pipeComputations,
                                                                           computePlan,
                                                                           logicalPlan,
                                                                           conf));

  return algorithms;
}

AdvancedPhysicalPipelineTypeID AdvancedPhysicalJoinSidePipe::getType() {
  return JOIN_SIDE;
}

std::string AdvancedPhysicalJoinSidePipe::getGeneratedHashSet() {
  return hashSet;
}

void AdvancedPhysicalJoinSidePipe::setHashSet(const string &hashSet) {
  this->hashSet = hashSet;
}

}