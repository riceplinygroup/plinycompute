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
#ifndef PDB_ABSTRACTPHYSICALNODE_H
#define PDB_ABSTRACTPHYSICALNODE_H

#include <memory>
#include <list>
#include "TupleSetJobStage.h"
#include "BroadcastJoinBuildHTJobStage.h"
#include "HashPartitionedJoinBuildHTJobStage.h"
#include "AggregationJobStage.h"
#include "AtomicComputation.h"
#include "AbstractJobStage.h"
#include "SetIdentifier.h"
#include "Handle.h"
#include "Statistics.h"

namespace pdb {

class TCAPAnalyzerResult;
class AbstractPhysicalNode;

typedef std::shared_ptr<TCAPAnalyzerResult> TCAPAnalyzerResultPtr;
typedef std::shared_ptr<AbstractPhysicalNode> AbstractTCAPAnalyzerNodePtr;

/**
 * This structure is used to give back the result of a TCAPAnalysis.
 * It is always returned by the analyze method of the AbstractTCAPAnalyzerNode
 */
struct TCAPAnalyzerResult {

  /**
   * A sequence of AbstractJobStages that need to be executed
   */
  std::list<Handle<AbstractJobStage>> physicalPlanToOutput;

  /**
   * The intermediate sets we need to create
   */
  std::list<Handle<SetIdentifier>> interGlobalSets;


  /**
   * The computation associated with the new set
   */
  AbstractTCAPAnalyzerNodePtr newSourceComputation;

  /**
   * Is the result we got a good one to execute?
   */
  bool success;
};


/**
 * All TCAPAnalyzerNodes inherit from this class. Instances of this abstract class are used to analyze the TCAP graph
 * and generate a physical plan out of it.
 */
class AbstractPhysicalNode {
public:

  AbstractPhysicalNode(string &jobId,
                           AtomicComputationPtr &node,
                           const Handle<ComputePlan> &computePlan,
                           LogicalPlanPtr &logicalPlan,
                           ConfigurationPtr &conf);

  /**
   * Performs the actual analysis of the TCAP and returns a partial physical plan in the case it succeeds
   * @return the TCAPAnalyzerResult is the result of the analysis
   */
  virtual TCAPAnalyzerResultPtr analyze(const StatisticsPtr &stats, int nextStageID) = 0;

  /**
   * Given the source set for this node and the statistics this method returns a cost based on a heuristic
   * @param source - the set identifier of the source
   * @param stats - the statistics about the sets
   * @return the cost value
   */
  virtual double getCost(Handle<SetIdentifier> source, const StatisticsPtr &stats) = 0;

  /**
   * Return the cost by calling the @see getCost method with the @see sourceSetIdentifier as a parameter.
   * @param stats - the statistics about the sets
   * @return the cost value
   */
  virtual double getCost(const StatisticsPtr &stats);

  /**
   * Returns true if this node still has consumers
   * @return true if it has, false otherwise
   */
  virtual bool hasConsumers() = 0;

  /**
   * Returns the AtomicComputation associated with this AbstractTCAPAnalyzerNode
   * @return the node
   */
  const AtomicComputationPtr &getNode() const;

  /**
   * This method returns the set identifier of the source if this node is a source, returns null otherwise
   * @return the set identifier
   */
  const Handle<SetIdentifier> &getSourceSetIdentifier() const;

  /**
   * Removes a consumer of this node
   * @param consumer the consumer we want to remove
   */
  void removeConsumer(const AbstractTCAPAnalyzerNodePtr &consumer) {
    consumers.remove(consumer);
  }

  /**
  * Adds a consumer to the node
  * @param consumer the consumer
  */
  virtual void addConsumer(const AbstractTCAPAnalyzerNodePtr &consumer) {
    consumers.push_back(consumer);
  }

protected:

  /**
   * The jobId for this query (can be any string that is can be a database name)
   */
  std::string jobId;

  /**
   * The AtomicComputation associated with this node
   */
  AtomicComputationPtr node;

  /**
   * The ComputePlan generated from input computations and the input TCAP string
   */
  Handle<ComputePlan> computePlan;

  /**
   * Logical plan generated from the compute plan
   */
  LogicalPlanPtr logicalPlan;

  /**
   * A configuration object for this server node
   */
  ConfigurationPtr conf;

  /**
   * A list of consumers of this node
   */
  std::list<AbstractTCAPAnalyzerNodePtr> consumers;

  /**
   * A list of producers of this node TODO implement this
   */
  std::list<AbstractTCAPAnalyzerNodePtr> producers;

  /**
   * Source set associated with this node.
   */
  Handle<SetIdentifier> sourceSetIdentifier;

  /**
   * Extracts a set identifier from a computation
   * @param computation the computation
   * @return the set we extracted
   */
  Handle<SetIdentifier> getSetIdentifierFromComputation(Handle<Computation> computation);
};

}



#endif //PDB_ABSTRACTPHYSICALNODE_H
