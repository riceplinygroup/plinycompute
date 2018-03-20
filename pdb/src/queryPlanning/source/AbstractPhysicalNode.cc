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
#include "AbstractPhysicalNode.h"

#include <utility>
#include "SelectionComp.h"
#include "MultiSelectionComp.h"

namespace pdb {

AbstractPhysicalNode::AbstractPhysicalNode(string &jobId,
                                                   AtomicComputationPtr &node,
                                                   const Handle<ComputePlan> &computePlan,
                                                   LogicalPlanPtr &logicalPlan,
                                                   ConfigurationPtr &conf) : jobId(jobId),
                                                                             node(node),
                                                                             computePlan(computePlan),
                                                                             logicalPlan(logicalPlan),
                                                                             conf(conf) {

  // if this node is a scan set we want to create a set identifier for it
  if(node->getAtomicComputationTypeID() == ScanSetAtomicTypeID) {

    // grab the computation
    Handle<Computation> comp = logicalPlan->getNode(node->getComputationName()).getComputationHandle();

    // create a set identifier from it
    sourceSetIdentifier = getSetIdentifierFromComputation(comp);
  }
}

double AbstractPhysicalNode::getCost(const StatisticsPtr &stats) {
  // return the cost of the source set identifier
  return getCost(sourceSetIdentifier, stats);
}

const Handle<SetIdentifier> &AbstractPhysicalNode::getSourceSetIdentifier() const {
  return sourceSetIdentifier;
}

const AtomicComputationPtr &AbstractPhysicalNode::getNode() const {
  return node;
}

Handle<SetIdentifier> AbstractPhysicalNode::getSetIdentifierFromComputation(Handle<Computation> computation) {

  switch (computation->getComputationTypeID()) {
    case ScanUserSetTypeID :
    case ScanSetTypeID : {

      // this is a ScanUserSet cast it
      Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(computation);

      // create a set identifier from it
      return makeObject<SetIdentifier>(scanner->getDatabaseName(), scanner->getSetName());
    }
    case ClusterAggregationCompTypeID : {

      // this is an AbstractAggregateComp cast it
      Handle<AbstractAggregateComp> aggregator = unsafeCast<AbstractAggregateComp, Computation>(computation);

      // create the set identifier from it
      return makeObject<SetIdentifier>(aggregator->getDatabaseName(), aggregator->getSetName());
    }
    case SelectionCompTypeID : {

      // this is a SelectionComp cast it
      Handle<SelectionComp<Object, Object>>
      selector = unsafeCast<SelectionComp<Object, Object>, Computation>(computation);

      // create the set identifier from it
      return makeObject<SetIdentifier>(selector->getDatabaseName(), selector->getSetName());
    }
    case MultiSelectionCompTypeID : {

      // this is a MultiSelectionComp cast it
      Handle<MultiSelectionComp<Object, Object>>
      selector = unsafeCast<MultiSelectionComp<Object, Object>, Computation>(computation);

      // create the set identifier from it
      return makeObject<SetIdentifier>(selector->getDatabaseName(), selector->getSetName());
    }
    default: {
      // this is bad, we can not cast this thing...
      PDB_COUT << "Source Computation Type: " << computation->getComputationType()
               << " are not supported as source node right now" << std::endl;
      PDB_COUT << "Master exit...Please restart cluster\n";
      exit(1); // TODO we are killing the server for a bad query might not be smart!
    }
  }
}

}