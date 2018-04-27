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

#ifndef PDB_AGGREGATIONPIPENODE_H
#define PDB_AGGREGATIONPIPENODE_H

#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractPipeline.h"

namespace pdb {

class AdvancedPhysicalAggregationPipeline : public AdvancedPhysicalAbstractPipeline {
public:

  AdvancedPhysicalAggregationPipeline(string &jobId,
                                      Handle<ComputePlan> &computePlan,
                                      LogicalPlanPtr &logicalPlan,
                                      ConfigurationPtr &conf,
                                      vector<AtomicComputationPtr> &pipeComputations,
                                      size_t id);

  /**
   * Pipelines the provided pipeline to this pipeline
   * @param pipeline - the pipeline we are providing
   * @return the result of the pipelining
   */
  PhysicalOptimizerResultPtr pipelineMe(int nextStageID, std::vector<AdvancedPhysicalPipelineNodePtr> pipeline) override;

  /**
   * Returns true if this is pipelinable
   * @param node
   * @return true if node can be pipelined to this pipeline
   */
  bool isPipelinable(AdvancedPhysicalPipelineNodePtr node) override;

  /**
   * If this operator is executed returns true false otherwise.
   * @return  true if it is false otherwise
   */
  bool isExecuted() override;

  /**
   * Returns the type of this pipeline
   * @return the type
   */
  AdvancedPhysicalPipelineTypeID getType() override;

  /**
   * Selects the output algorithm for this pipeline
   * @return selects the output algorithm for this pipeline
   */
  AdvancedPhysicalAbstractAlgorithmPtr selectOutputAlgorithm() override;

  /**
   * Returns all the possible algorithms that can be used to execute the pipeline
   * @return a vector of possible algorithms
   */
  vector<AdvancedPhysicalAbstractAlgorithmPtr> getPossibleAlgorithms(const StatisticsPtr &stats) override;


  AdvancedPhysicalAbstractAlgorithmPtr propose(std::vector<AdvancedPhysicalAbstractAlgorithmPtr> algorithms) override;
};

}



#endif //PDB_AGGREGATIONPIPENODE_H
