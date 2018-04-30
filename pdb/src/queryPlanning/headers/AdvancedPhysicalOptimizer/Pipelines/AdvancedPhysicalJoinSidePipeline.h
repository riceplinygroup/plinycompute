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

#ifndef PDB_ADVANCEDPHYSICALJOINPIPELINE_H
#define PDB_ADVANCEDPHYSICALJOINPIPELINE_H

#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractPipeline.h"

namespace pdb {

class AdvancedPhysicalJoinSidePipeline : public AdvancedPhysicalAbstractPipeline {
public:

  AdvancedPhysicalJoinSidePipeline(string &jobId,
                               Handle<ComputePlan> &computePlan,
                               LogicalPlanPtr &logicalPlan,
                               ConfigurationPtr &conf,
                               vector<AtomicComputationPtr> &pipeComputations,
                               size_t id);

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

  /**
   * Picks one of the algorithms provided as the proposed (the best algorithm)
   * @param algorithms - the lise of algorithms
   * @return the algorithm we like
   */
  AdvancedPhysicalAbstractAlgorithmPtr propose(std::vector<AdvancedPhysicalAbstractAlgorithmPtr> algorithms) override;

 protected:

  /**
   * This is the largest cost that can we can broadcast
   */
  const double BROADCAST_JOIN_COST_THRESHOLD = 15000;

};

}

#endif //PDB_ADVANCEDPHYSICALJOINPIPELINE_H
