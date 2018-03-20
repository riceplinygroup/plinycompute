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
#ifndef PDB_PHYSICAL_OPTIMIZER_H
#define PDB_PHYSICAL_OPTIMIZER_H

#include <set>
#include "AggregationJobStage.h"
#include "AtomicComputationList.h"
#include "BroadcastJoinBuildHTJobStage.h"
#include "Computation.h"
#include "ComputePlan.h"
#include "Configuration.h"
#include "HashPartitionedJoinBuildHTJobStage.h"
#include "LogicalPlan.h"
#include "PDBLogger.h"
#include "Statistics.h"
#include "TupleSetJobStage.h"
#include "AbstractPhysicalNodeFactory.h"

namespace pdb {

/**
 * This class takes in as input a graph made out of @see AbstractPhysicalNode objects and preforms PhysicalOptimization
 * on them.
 *
 * This is accomplished by iteratively calling the method getNextStagesOptimized to generate a sequence of JobStages.
 * As an input to the getNextStagesOptimized we have to provide the storage statistics, so
 * it can determine the best starting source.
 *
 * The statistics should be iteratively updated after the execution of each sequence of JobStages, to reflect the
 * current state.
 *
 * There are four types of JobStages that can be generated
 * -- TupleSetJobStage: a pipeline
 * -- AggregationJobStage: This stage performs the aggregation on shuffled data
 * -- BroadcastJoinBuildHTJobStage: Builds a hash table for the broadcast join
 * -- HashPartitionedJoinBuildHTJobStage: Builds the hash table for the partitioned join
 *
 */
class PhysicalOptimizer {
 public:

  /**
   * The constructor for the PhysicalOptimizer from a TCAP string and a list of computations associated with it
   * @param sources the source nodes of the graph to analyze
   * @param logger an instance of the PDBLogger
   */
  PhysicalOptimizer(std::vector<AbstractPhysicalNodePtr> &sources, PDBLoggerPtr &logger);

  /**
     * Returns a sequence of job stages that, make up a partial physical plan. After the execution we gather the
     * statistics about the newly created sets and use them to generate the next partial plan.
     * @param physicalPlanToOutput a list where we want to put the sequence of job stages
     * @param interGlobalSets a list of intermediates sets that need to be created
     * @param stats the statistics about
     * @param jobStageId the id of the current job stage
     * @return true if we succeeded in creating the partial physical plan.
     */
  bool getNextStagesOptimized(std::vector<pdb::Handle<AbstractJobStage>> &physicalPlanToOutput,
                              std::vector<pdb::Handle<SetIdentifier>> &interGlobalSets,
                              StatisticsPtr &stats,
                              int &jobStageId);

  /**
   * Check if we still have some sources to process
   * @return true if we do, false otherwise
   */
  bool hasSources();

  /**
   * Returns true if the the provided source still has any consumers that we need to process
   * @param name source set name in the form of "databaseName:setName"
   * @return true if the the provided source has any consumers, false otherwise
   */
  bool hasConsumers(std::string &name);


  /**
   * Returns the best source node based on heuristics
   * @return the node
   */
  AbstractPhysicalNodePtr getBestNode(StatisticsPtr &ptr);

 private:

  /**
   * This is the factor applied to the cost of the source if penalized
   */
  static constexpr double SOURCE_PENALIZE_FACTOR = 1000.00;

  /**
    * Hash map where the key is the name of the source set in the form of "databaseName:setName"
    * and the AbstractTCAPAnalyzerNodePtr associated with it.
    */
  std::map<std::string, AbstractPhysicalNodePtr> sourceNodes;

  /**
   * Penalized source sets in the form databaseName:setName
   */
  std::set<std::string> penalizedSets;

  /**
   * An instance of the PDBLogger
   */
  PDBLoggerPtr logger;

};

}


#endif //PDB_PHYSICAL_OPTIMIZER_H
