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
#ifndef PDB_SIMPLEPHYSICALNODE_H
#define PDB_SIMPLEPHYSICALNODE_H

#include "BroadcastJoinBuildHTJobStage.h"
#include "HashPartitionedJoinBuildHTJobStage.h"
#include "JobStageBuilders/TupleSetJobStageBuilder.h"
#include "AggregationJobStage.h"
#include "TupleSetJobStage.h"
#include "AbstractPhysicalNode.h"

namespace pdb {

class SimplePhysicalNode;
typedef std::shared_ptr<SimplePhysicalNode> SimplePhysicalNodePtr;

class SimplePhysicalNode : public AbstractPhysicalNode {

public:

  SimplePhysicalNode(string jobId,
                     AtomicComputationPtr node,
                     const Handle<ComputePlan> &computePlan,
                     LogicalPlanPtr logicalPlan,
                     ConfigurationPtr conf);

  /**
   * Use the default destructor
   */
  ~SimplePhysicalNode() override = default;

  /**
   * This method starts the analysis for the simple physical optimizer.
   * What it does is it sets up a TupleStageBuilder and then calls recursively the other analyze method
   * @return the resulting partial plan if succeeded
   */
  PhysicalOptimizerResultPtr analyze(const StatisticsPtr &stats, int nextStageID) override;

  /**
   * Depending on the number of active consumers this node has it either :
   *
   * analyzeOutput - in the case this node does not have any consumers
   * analyzeSingleConsumer - in the case there is just one consumer
   * analyzeMultipleConsumers - in the case that this node has multiple consumers
   *
   * @param shared_ptr
   * @return the resulting partial plan if succeeded
   */
  PhysicalOptimizerResultPtr analyze(TupleSetJobStageBuilderPtr &shared_ptr,
                                     SimplePhysicalNodePtr &prevNode,
                                     const StatisticsPtr &stats,
                                     int nextStageID);

  /**
   * Returns the AtomicComputation associated with this AbstractPhysicalNode
   * @return the node
   */
  const AtomicComputationPtr &getNode() const;

  /**
   * Adds a consumer to this node
   * This method calls the base method but also adds the consumer to the list of @see activeConsumers.
   * @param consumer
   */
  void addConsumer(const AbstractPhysicalNodePtr &consumer) override;

  /**
   * Returns true if this node has any unprocessed consumers, false otherwise
   * @return the value
   */
  bool hasConsumers() override;

  bool isConsuming(Handle<SetIdentifier> &set) override;

  /**
   * Return the cost by calling the @see getCost method with the @see sourceSetIdentifier as a parameter.
   * @param stats - the statistics about the sets
   * @return the cost value
   */
  double getCost(const StatisticsPtr &stats) override;

  /**
   * Returns the shared_pointer to this node
   * @return the handle
   */
  SimplePhysicalNodePtr getSimpleNodeHandle();

protected:

  /**
   * This method is called in the case that we have just one consumer of this node
   * What it does is, it just adds AtomicComputation associated with this node to the pipline we are building
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   *
   * @return the result will contain a partial physical plan
   */
  virtual PhysicalOptimizerResultPtr analyzeSingleConsumer(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                           SimplePhysicalNodePtr &prevNode,
                                                           const StatisticsPtr &stats,
                                                           int nextStageID);

  /**
   * This method is called if we do not have any consumers thus this is an output node
   * What it does is, it just creates a job stage and returns a result with success
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   *
   * @return the result will contain a partial physical plan
   */
  virtual PhysicalOptimizerResultPtr analyzeOutput(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                   SimplePhysicalNodePtr &prevNode,
                                                   const StatisticsPtr &stats,
                                                   int nextStageID);

  /**
   * This method is called if we have multiple consumers thus this node needs to materialized
   * What it does it check whether we already need to materialize this node if not, it makes
   * it materializable the it just creates a job stage
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   *
   * @return the result will contain a partial physical plan
   */
  virtual PhysicalOptimizerResultPtr analyzeMultipleConsumers(TupleSetJobStageBuilderPtr &ptr,
                                                              SimplePhysicalNodePtr &prevNode,
                                                              const StatisticsPtr &stats,
                                                              int nextStageID);

  /**
   * Returns the identifier of this node in this case in the form of <databaseName>:<setName>
   * @return the identifier
   */
  std::string getNodeIdentifier() override;

 protected:

  /**
   * This method calculates the cost of the provided source. The cost is calculated by the formula :
   * cost = number_of_bytes / 1000000
   * @param source
   * @param stats
   * @return the const
   */
  double getCost(Handle<SetIdentifier> source, const StatisticsPtr &stats);

  /**
   * This method returns the set identifier of the source if this node is a source, returns null otherwise
   * @return the set identifier
   */
  const Handle<SetIdentifier> &getSourceSetIdentifier() const;

  /**
   * A list of consumers of this node
   */
  std::list<SimplePhysicalNodePtr> activeConsumers;

  /**
   * Source set associated with this node.
   */
  Handle<SetIdentifier> sourceSetIdentifier;

  /**
   * The AtomicComputation associated with this node
   */
  AtomicComputationPtr node;

};

}

#endif //PDB_SIMPLEPHYSICALNODE_H
