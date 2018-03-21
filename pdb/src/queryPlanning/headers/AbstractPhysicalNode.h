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

class PhysicalOptimizerResult;
class AbstractPhysicalNode;

typedef std::shared_ptr<PhysicalOptimizerResult> PhysicalOptimizerResultPtr;
typedef std::shared_ptr<AbstractPhysicalNode> AbstractPhysicalNodePtr;
typedef std::shared_ptr<AbstractPhysicalNode> AbstractPhysicalNodeWeakPtr;

/**
 * This structure is used to give back the result of a TCAPAnalysis.
 * It is always returned by the analyze method of the AbstractPhysicalNode
 */
struct PhysicalOptimizerResult {

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
  AbstractPhysicalNodePtr newSourceComputation;

  /**
   * Is the result we got a good one to execute?
   */
  bool success;
};


/**
 * All PhysicalNodes inherit from this class. Instances of this abstract class are used to analyze the TCAP graph
 * and generate a physical plan out of it.
 */
class AbstractPhysicalNode {
public:

  AbstractPhysicalNode(string &jobId,
                       const Handle<ComputePlan> &computePlan,
                       LogicalPlanPtr &logicalPlan,
                       ConfigurationPtr &conf);

  /**
   * Performs the actual analysis of the TCAP and returns a partial physical plan in the case it succeeds
   * @return the PhysicalOptimizerResult is the result of the analysis
   */
  virtual PhysicalOptimizerResultPtr analyze(const StatisticsPtr &stats, int nextStageID) = 0;

  /**
   * Given the source set for this node and the statistics this method returns a cost based on a heuristic
   * @param source - the set identifier of the source
   * @param stats - the statistics about the sets
   * @return the cost value
   */
  virtual double getCost(const StatisticsPtr &stats) = 0;

  /**
   * Returns true if this node still has consumers
   * @return true if it has, false otherwise
   */
  virtual bool hasConsumers() = 0;

  /**
   * Returns a string that uniquely identifies this node
   * @return the string
   */
  virtual std::string getNodeIdentifier() = 0;

  /**
   * Returns a shared pointer handle to this node
   * @return the shared pointer handle
   */
  AbstractPhysicalNodePtr getHandle();

  /**
   * Removes a consumer of this node
   * @param consumer the consumer we want to remove
   */
  virtual void removeConsumer(const AbstractPhysicalNodePtr &consumer) {
    consumers.remove(consumer);
    consumer->producers.remove(getHandle());
  }

  /**
  * Adds a consumer to the node
  * @param consumer the consumer
  */
  virtual void addConsumer(const AbstractPhysicalNodePtr &consumer) {
    consumers.push_back(consumer);
    consumer->producers.push_back(getHandle());
  }

protected:

  /**
   * The jobId for this query (can be any string that is can be a database name)
   */
  std::string jobId;

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
  std::list<AbstractPhysicalNodePtr> consumers;

  /**
   * A list of producers of this node
   */
  std::list<AbstractPhysicalNodeWeakPtr> producers;

  /**
   * A shared pointer to an instance of this node
   */
  AbstractPhysicalNodeWeakPtr handle;

  /**
   * Extracts a set identifier from a computation
   * @param computation the computation
   * @return the set we extracted
   */
  Handle<SetIdentifier> getSetIdentifierFromComputation(Handle<Computation> computation);
};

}



#endif //PDB_ABSTRACTPHYSICALNODE_H
