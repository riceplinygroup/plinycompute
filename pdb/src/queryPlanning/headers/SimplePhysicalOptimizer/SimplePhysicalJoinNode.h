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
#ifndef PDB_SIMPLEPHYSICALJOINNODE_H
#define PDB_SIMPLEPHYSICALJOINNODE_H

#include "JobStageBuilders/TupleSetJobStageBuilder.h"
#include "SimplePhysicalNode.h"

namespace pdb {

class SimplePhysicalJoinNode : public SimplePhysicalNode {
public:

  SimplePhysicalJoinNode(string jobId,
                             AtomicComputationPtr node,
                             const Handle<ComputePlan> &computePlan,
                             LogicalPlanPtr logicalPlan,
                             ConfigurationPtr conf);

  /**
   * The join can not be an output node therefore this will cause an error
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   * @return the result will contain a partial physical plan
   *
   * @return - non existent
   */
  PhysicalOptimizerResultPtr analyzeOutput(TupleSetJobStageBuilderPtr &ptr,
                                      SimplePhysicalNodePtr &prevNode,
                                      const StatisticsPtr &stats,
                                      int nextStageID) override;

  /**
   * This method is called in the case that we have just one consumer of this join node
   * We have two main cases to handle when dealing with a join
   * 1. This is the first time we are processing this join therefore no side of the join has been hashed
   * and then broadcasted or partitioned, therefore we can not probe it
   * 2. This is the second time we are processing this join therefore the one side of the join is hashed and then
   * broadcasted or partitioned, we can therefore probe it!
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   * @return the result will contain a partial physical plan
   *
   * @return the result of the analysis
   */
  PhysicalOptimizerResultPtr analyzeSingleConsumer(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                              SimplePhysicalNodePtr &prevNode,
                                              const StatisticsPtr &stats,
                                              int nextStageID) override;

private:

  /**
   * This is the value where
   */
  const double BROADCAST_JOIN_COST_THRESHOLD = 15000;

  /**
   * Has one side of this join already been hashed?
   */
  bool transversed;

  /**
   * Name of the hash this join has produced
   */
  string hashSetName;

  /**
   * Did we rollback the planning for this join or not...
   */
  bool rollbacked;

};

}


#endif //PDB_SIMPLEPHYSICALJOINNODE_H
