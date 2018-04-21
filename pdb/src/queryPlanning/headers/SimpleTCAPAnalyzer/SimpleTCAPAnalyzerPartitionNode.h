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
#ifndef SIMPLE_TCAP_ANALYZER_PARTITION_NODE_H
#define SIMPLE_TCAP_ANALYZER_PARTITION_NODE_H

#include <JobStageBuilders/TupleSetJobStageBuilder.h>
#include "SimpleTCAPAnalyzerNode.h"

namespace pdb {

class SimpleTCAPAnalyzerPartitionNode : public SimpleTCAPAnalyzerNode {

public:

  SimpleTCAPAnalyzerPartitionNode(string jobId,
                                    AtomicComputationPtr node,
                                    const Handle<ComputePlan> &computePlan,
                                    LogicalPlanPtr logicalPlan,
                                    ConfigurationPtr conf);


protected:
  /**
   * In the case that this partition has only one consumer.
   * It essentially builds the pdb::TupleSetJobStage.
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   *
   * @return the result will contain a partial physical plan
   */
  TCAPAnalyzerResultPtr analyzeSingleConsumer(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                              SimpleTCAPAnalyzerNodePtr &prevNode,
                                              const StatisticsPtr &stats,
                                              int nextStageID) override;

  /**
   * In the case that this partition is the output.
   * It essentially builds the pdb::TupleSetJobStage.
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   *
   * @return the result will contain a partial physical plan
   */
  TCAPAnalyzerResultPtr analyzeOutput(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                      SimpleTCAPAnalyzerNodePtr &prevNode,
                                      const StatisticsPtr &stats,
                                      int nextStageID) override;

  /**
   * In the case that this partition has only one consumer.
   * It essentially builds the @see pdb::TupleSetJobStage.
   *
   * @param tupleStageBuilder - the builder for the tuple set job stage, that contains all the computations in our
   * pipeline so far.
   * @param prevNode - the previous node we are coming from to analyze this node
   * @param stats - the statistics about the sets that are in the catalog
   * @param nextStageID - the id of the next stage for this job
   *
   * @return the result will contain a partial physical plan
   */
  TCAPAnalyzerResultPtr analyzeMultipleConsumers(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                 SimpleTCAPAnalyzerNodePtr &prevNode,
                                                 const StatisticsPtr &stats,
                                                 int nextStageID) override;

};
}



#endif //SIMPLE_TCAP_ANALYZER_PARTITION_NODE_H
