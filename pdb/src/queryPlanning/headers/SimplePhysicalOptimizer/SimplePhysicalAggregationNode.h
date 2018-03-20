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
#ifndef PDB_SIMPLEPHYSICALAGGREGATIONNODE_H
#define PDB_SIMPLEPHYSICALAGGREGATIONNODE_H

#include <JobStageBuilders/TupleSetJobStageBuilder.h>
#include "SimplePhysicalNode.h"

namespace pdb {

class SimplePhysicalAggregationNode : public SimplePhysicalNode {

public:

  SimplePhysicalAggregationNode(string jobId,
                                    AtomicComputationPtr node,
                                    const Handle<ComputePlan> &computePlan,
                                    LogicalPlanPtr logicalPlan,
                                    ConfigurationPtr conf);


protected:

  /**
   * In the case that this aggregation has only one consumer this method is called.
   * It essentially builds the @see pdb::TupleSetJobStage and @see pdb::AggregationJobStage
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   *
   * @return the result will contain a partial physical plan
   */
  PhysicalOptimizerResultPtr analyzeSingleConsumer(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                              SimplePhysicalNodePtr &prevNode,
                                              const StatisticsPtr &stats,
                                              int nextStageID) override;

  /**
   * In the case that this aggregation is the output this method is called.
   * It essentially builds the @see pdb::TupleSetJobStage and @see pdb::AggregationJobStage with the right parameters.
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   *
   * @return the result will contain a partial physical plan
   */
  PhysicalOptimizerResultPtr analyzeOutput(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                      SimplePhysicalNodePtr &prevNode,
                                      const StatisticsPtr &stats,
                                      int nextStageID) override;

  /**
   * In the case that this aggregation has only one consumer this method is called.
   * It essentially builds the @see pdb::TupleSetJobStage and @see pdb::AggregationJobStage with the right parameters.
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   *
   * @return the result will contain a partial physical plan
   */
  PhysicalOptimizerResultPtr analyzeMultipleConsumers(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                 SimplePhysicalNodePtr &prevNode,
                                                 const StatisticsPtr &stats,
                                                 int nextStageID) override;

};
}



#endif //PDB_SIMPLEPHYSICALAGGREGATIONNODE_H
