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

#ifndef PDB_ADVANCEDPHYSICALALGORITHM_H
#define PDB_ADVANCEDPHYSICALALGORITHM_H

#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractPipeline.h"

namespace pdb {


enum AdvancedPhysicalAbstractAlgorithmTypeID {
  SELECTION_ALGORITHM,
  AGGREGATION_ALGORITHM,
  JOIN_ALGORITHM,
  JOIN_BROADCAST_ALGORITHM,
  JOIN_HASH_ALGORITHM
};

class AdvancedPhysicalAbstractAlgorithm;
typedef std::shared_ptr<AdvancedPhysicalAbstractAlgorithm> AdvancedPhysicalAbstractAlgorithmPtr;

class AdvancedPhysicalAbstractAlgorithm {

public:

  AdvancedPhysicalAbstractAlgorithm(const AdvancedPhysicalPipelineNodePtr &handle,
                                    const std::string &jobID,
                                    bool isProbing,
                                    Handle<SetIdentifier> source,
                                    const vector<AtomicComputationPtr> &pipeComputations,
                                    Handle<ComputePlan> computePlan,
                                    const LogicalPlanPtr &logicalPlan,
                                    const ConfigurationPtr &conf);

  /**
   * Generates the stages for this algorithm
   * @return
   */
  virtual PhysicalOptimizerResultPtr generate(int nextStageID) = 0;

  /**
   * Generates the stages for pipelined oprators
   * @param nextStageID
   * @param pipeline
   * @return
   */
  virtual PhysicalOptimizerResultPtr generatePipelined(int nextStageID, std::vector<AdvancedPhysicalPipelineNodePtr> &pipeline) = 0;

  /**
   * Returns the type of the algorithm
   * @return the type id
   */
  virtual AdvancedPhysicalAbstractAlgorithmTypeID getType() = 0;

protected:

  /**
   * The handle to the node this algorithm is associated with
   */
  AdvancedPhysicalPipelineNodePtr handle;

  /**
   * The id of the next stage
   */
  std::string jobID;

  /**
   * Contains all the atomic computations that make-up this pipe
   */
  vector<AtomicComputationPtr> pipeComputations;

  /**
   * Logical plan generated from the compute plan
   */
  LogicalPlanPtr logicalPlan;

  /**
   * The ComputePlan generated from input computations and the input TCAP string
   */
  Handle<ComputePlan> computePlan;

  /**
   * A configuration object for this server node
   */
  ConfigurationPtr conf;

  /**
   * The sink of this algorithm
   */
  Handle<SetIdentifier> sink;

  /**
   * Source set associated with this node.
   */
  Handle<SetIdentifier> source;

};

}


#endif //PDB_ADVANCEDPHYSICALALGORITHM_H
