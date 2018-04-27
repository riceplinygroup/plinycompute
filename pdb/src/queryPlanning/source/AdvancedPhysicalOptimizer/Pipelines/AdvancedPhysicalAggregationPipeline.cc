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


#include <AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalAggregationAlgorithm.h>
#include "SimplePhysicalOptimizer/SimplePhysicalPartitionNode.h"
#include "AdvancedPhysicalOptimizer/Pipelines/AdvancedPhysicalAggregationPipeline.h"

namespace pdb {

AdvancedPhysicalAggregationPipeline::AdvancedPhysicalAggregationPipeline(string &jobId,
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

bool AdvancedPhysicalAggregationPipeline::isPipelinable(AdvancedPhysicalPipelineNodePtr node) {
  return false;
}

AdvancedPhysicalAbstractAlgorithmPtr AdvancedPhysicalAggregationPipeline::selectOutputAlgorithm() {
  return std::make_shared<AdvancedPhysicalAggregationAlgorithm>(getAdvancedPhysicalNodeHandle(),
                                                                jobId,
                                                                sourceSetIdentifier,
                                                                pipeComputations,
                                                                computePlan,
                                                                logicalPlan,
                                                                conf);
}

bool AdvancedPhysicalAggregationPipeline::isExecuted() {
  return false;
}

AdvancedPhysicalPipelineTypeID AdvancedPhysicalAggregationPipeline::getType() {
  return AGGREGATION;
}

}

