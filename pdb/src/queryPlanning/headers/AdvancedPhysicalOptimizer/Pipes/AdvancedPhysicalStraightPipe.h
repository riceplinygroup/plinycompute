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

#ifndef PDB_ADVANCEDPHYSICALSTRAIGHTPIPELINE_H
#define PDB_ADVANCEDPHYSICALSTRAIGHTPIPELINE_H

#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractPipe.h"

namespace pdb {

class AdvancedPhysicalStraightPipe : public AdvancedPhysicalAbstractPipe {
public:

  AdvancedPhysicalStraightPipe(string &jobId,
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
  std::vector<AdvancedPhysicalAbstractAlgorithmPtr> getPossibleAlgorithms(const StatisticsPtr &stats) override;

};

}

#endif //PDB_ADVANCEDPHYSICALSTRAIGHTPIPELINE_H
