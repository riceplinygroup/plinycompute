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
#include "SimpleTCAPAnalyzer/SimpleTCAPAnalyzerNodeFactory.h"
#include "SimpleTCAPAnalyzer/SimpleTCAPAnalyzerAggregationNode.h"
#include "SimpleTCAPAnalyzer/SimpleTCAPAnalyzerPartitionNode.h"
#include "SimpleTCAPAnalyzer/SimpleTCAPAnalyzerJoinNode.h"
#include "SimpleTCAPAnalyzer/SimpleTCAPAnalyzerNode.h"

namespace pdb {

SimpleTCAPAnalyzerNodeFactory::SimpleTCAPAnalyzerNodeFactory(const string &jobId,
                                                             const Handle<ComputePlan> &computePlan,
                                                             const ConfigurationPtr &conf) : AbstractTCAPAnalyzerNodeFactory(computePlan),
                                                                                             jobId(jobId),
                                                                                             conf(conf) {}

AbstractTCAPAnalyzerNodePtr SimpleTCAPAnalyzerNodeFactory::createAnalyzerNode(AtomicComputationPtr tcapNode) {

  // check the type of the atomic computation
  switch (tcapNode->getAtomicComputationTypeID()){

    // we are dealing with an aggregate
    case ApplyAggTypeID: {
      return (new SimpleTCAPAnalyzerAggregationNode(jobId, tcapNode, computePlan, logicalPlan, conf))->getHandle();
    }

    // we are dealing with partitioning
    case ApplyPartitionTypeID: {
      return (new SimpleTCAPAnalyzerPartitionNode(jobId, tcapNode, computePlan, logicalPlan, conf))->getHandle();
    }

    // we are dealing with a join
    case ApplyJoinTypeID: {
      return (new SimpleTCAPAnalyzerJoinNode(jobId, tcapNode, computePlan, logicalPlan, conf))->getHandle();
    }

    // we are dealing with node that is not an aggregate or a join (no special treatment needed)
    default: {
      return (new SimpleTCAPAnalyzerNode(jobId, tcapNode, computePlan, logicalPlan, conf))->getHandle();
    }
  }
}

}
