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

#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractPipe.h"

namespace pdb {


enum AdvancedPhysicalAbstractAlgorithmTypeID {
  SELECTION_ALGORITHM,
  AGGREGATION_ALGORITHM,
  JOIN_ALGORITHM,
  JOIN_BROADCASTED_HASHSET_ALGORITHM,
  JOIN_SHUFFLED_HASHSET_ALGORITHM,
  JOIN_SUFFLE_SET_ALGORITHM
};

class AdvancedPhysicalAbstractAlgorithm;
typedef std::shared_ptr<AdvancedPhysicalAbstractAlgorithm> AdvancedPhysicalAbstractAlgorithmPtr;

class AdvancedPhysicalAbstractAlgorithm {

public:

  AdvancedPhysicalAbstractAlgorithm(const AdvancedPhysicalPipelineNodePtr &handle,
                                      const std::string &jobID,
                                      bool isProbing,
                                      bool isOutput,
                                      Handle<SetIdentifier> source,
                                      const vector<AtomicComputationPtr> &pipeComputations,
                                      Handle<ComputePlan> computePlan,
                                      const LogicalPlanPtr &logicalPlan,
                                      const ConfigurationPtr &conf);

  /**
   * Generates the stages for this algorithm
   * @return
   */
  virtual PhysicalOptimizerResultPtr generate(int nextStageID, const StatisticsPtr &stats) = 0;

  /**
   * Generates the stages for pipelined operators
   * @param nextStageID
   * @param pipeline
   * @return
   */
  virtual PhysicalOptimizerResultPtr generatePipelined(int nextStageID,
                                                       const StatisticsPtr &stats,
                                                       std::vector<AdvancedPhysicalPipelineNodePtr> &pipeline);

  /**
   * Returns the type of the algorithm
   * @return the type id
   */
  virtual AdvancedPhysicalAbstractAlgorithmTypeID getType() = 0;

protected:

  /**
   * Approximates the size of the result of this algorithm. The default implementation simply returns the same size
   * as the source set
   * @return the size of the sets
   */
  virtual DataStatistics approximateResultSize(const StatisticsPtr &stats);

  /**
   * Goes through every consumer and sets the source set of this consumer to be the sink of this algorithm
   * @param sourceSetIdentifier - the set identifier of the new source
   * @param approxSize - the approximate size of the new source
   */
  void updateConsumers(const Handle<SetIdentifier> &sink,
                       DataStatistics approxSize,
                       const StatisticsPtr &stats);

  /**
   * A join has two sides in case that we generated a pipeline breaker for the probe set of the join (the one we don't
   * use to build the hash map) we have to include it's hash atomic computation into the pipeComputations so we can
   * do that join. (The reason why we have to do this is because TCAP is structured that way)
   */
  void includeHashComputation();

  /**
   * The handle to the node this algorithm is associated with
   */
  std::list<AdvancedPhysicalPipelineNodePtr> pipeline;

  /**
   * The id of the next stage
   */
  std::string jobID;

  /**
   * Contains all the atomic computations that make-up this pipe
   */
  list<AtomicComputationPtr> pipeComputations;

  /**
   * All the hash sets we are probing in this algorithm
   */
  unordered_map<std::string, std::string> probingHashSets;

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

  /**
   * If pipeline is probing a hash set this is true
   */
  bool isProbing;

  /**
   * Is this an output pipeline
   */
  bool isOutput;
};

}


#endif //PDB_ADVANCEDPHYSICALALGORITHM_H
