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
#include "SelectionComp.h"
#include "PartitionComp.h"
#include "MultiSelectionComp.h"

namespace pdb {

AbstractPhysicalNode::AbstractPhysicalNode(string &jobId,
                                           const Handle<ComputePlan> &computePlan,
                                           LogicalPlanPtr &logicalPlan,
                                           ConfigurationPtr &conf) : jobId(jobId),
                                                                     computePlan(computePlan),
                                                                     logicalPlan(logicalPlan),
                                                                     conf(conf),
                                                                     handle(nullptr) {}

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
    case PartitionCompTypeID : {

      // this is a PartitionComp cast it
      Handle<PartitionComp<Object, Object>> partitioner = unsafeCast<PartitionComp<Object, Object>, Computation>(computation);

      // create the set identifier from it
      return makeObject<SetIdentifier>(partitioner->getDatabaseName(), partitioner->getSetName());

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
      PDB_COUT << "Manager exit...Please restart cluster\n";
      exit(1); // TODO we are killing the server for a bad query might not be smart!
    }
  }
}


AbstractPhysicalNodePtr AbstractPhysicalNode::getHandle() {

  // if we do not have a handle to this node already
  if(handle == nullptr) {
    handle = std::shared_ptr<AbstractPhysicalNode> (this);
  }

  return handle;
}

pdb::AbstractPhysicalNodePtr AbstractPhysicalNode::getConsumer(int idx) {

  // set the iterator to the idx-th element
  auto it = consumers.begin();
  std::advance(it, idx);

  // return the consumer
  return *it;
}

}
