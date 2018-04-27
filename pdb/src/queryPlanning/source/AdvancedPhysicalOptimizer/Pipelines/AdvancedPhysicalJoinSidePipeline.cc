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
#include "AdvancedPhysicalOptimizer/Pipelines/AdvancedPhysicalJoinSidePipeline.h"

namespace pdb {

AdvancedPhysicalJoinSidePipeline::AdvancedPhysicalJoinSidePipeline(string &jobId,
                                                                   Handle<ComputePlan> &computePlan,
                                                                   LogicalPlanPtr &logicalPlan,
                                                                   ConfigurationPtr &conf,
                                                                   vector<AtomicComputationPtr> &pipeComputations,
                                                                   size_t id) :
                                                                   AdvancedPhysicalAbstractPipeline(jobId,
                                                                                                    computePlan,
                                                                                                    logicalPlan,
                                                                                                    conf,
                                                                                                    pipeComputations,
                                                                                                    id) {}

bool AdvancedPhysicalJoinSidePipeline::isPipelinable(AdvancedPhysicalPipelineNodePtr node) {

  // if by any case the child of a join is not a join side pipeline assert
  assert(node->getType() == JOIN_SIDE);

  // for each side of the join that is not the one we are coming from we check if it is processed
  for(auto &producer : producers) {

    // if on of the producers is not a join side fail
    assert(std::dynamic_pointer_cast<AdvancedPhysicalAbstractPipeline>(producer)->getType() == JOIN_SIDE);

    // cast the consumer to the join
    auto casted_producer = std::dynamic_pointer_cast<AdvancedPhysicalAbstractPipeline>(producer);

    // if the consumer is not processed this operator is not pipelinable if
    // A) this operator is not executed or B) We shuffled the other side so now we have to shuffle this side
    if(node != producer && (!casted_producer->isExecuted() || casted_producer->getSelectedAlgorithm()->getType() == JOIN_HASH_ALGORITHM)) {

      // ok we can not pipeline this
      return false;
    }
  }

  // we can not pipeline this the other side is not processed
  return true;
}

AdvancedPhysicalAbstractAlgorithmPtr AdvancedPhysicalJoinSidePipeline::selectOutputAlgorithm() {
  return pdb::AdvancedPhysicalAbstractAlgorithmPtr();
}

bool AdvancedPhysicalJoinSidePipeline::isExecuted() {
  return false;
}

AdvancedPhysicalPipelineTypeID AdvancedPhysicalJoinSidePipeline::getType() {
  return JOIN_SIDE;
}

}