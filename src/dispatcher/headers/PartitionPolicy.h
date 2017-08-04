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
#ifndef OBJECTQUERYMODEL_PARTITIONPOLICY_H
#define OBJECTQUERYMODEL_PARTITIONPOLICY_H

#include "DataTypes.h"
#include "Handle.h"
#include "Object.h"
#include "Record.h"
#include "PDBVector.h"

#include "NodeDispatcherData.h"
#include "NodePartitionData.h"

#include <unordered_map>

namespace pdb {

class PartitionPolicy;
typedef std::shared_ptr<PartitionPolicy> PartitionPolicyPtr;

// TODO: The list of nodes should contain which databases/sets are located on each of them

/**
 * An interface used by DispatcherServer to properly map PDB::Objects to
 * the Storage Node which they should be stored. Calls to partition may be non-deterministic (see RandomPolicy).
 */
class PartitionPolicy {
public:

    enum Policy {RANDOM, ROUNDROBIN, FAIR, DEFAULT};

    std::vector<NodePartitionDataPtr> createNodePartitionData(Handle<Vector<Handle<NodeDispatcherData>>> storageNodes);

    /**
     * Partitions a Vector of PDB data into a number of smaller Vectors all mapped to a respective Storage Node
     *
     * @param toPartition a vector of PDB::Objects to be stored
     * @return a mapping from node ids to the PDB::Objects that should be stored there
     */
    virtual std::shared_ptr<std::unordered_map<NodeID, Handle<Vector<Handle<Object>>>>>
        partition(Handle<Vector<Handle<Object>>> toPartition) = 0;

    /**
     * Updates PartitionPolicy with a collection of all the available storage nodes in the cluster
     *
     * @param storageNodes a vector of the live storage nodes
     */
    virtual void updateStorageNodes(Handle<Vector<Handle<NodeDispatcherData>>> storageNodes) = 0;

    virtual NodePartitionDataPtr updateExistingNode(NodePartitionDataPtr newNodeData,
                                            NodePartitionDataPtr oldNodeData) = 0;

    virtual NodePartitionDataPtr updateNewNode(NodePartitionDataPtr newNode) = 0;

    virtual NodePartitionDataPtr handleDeadNode(NodePartitionDataPtr deadNode) = 0;

    std::vector<NodePartitionDataPtr> storageNodes;

};

}

#endif //OBJECTQUERYMODEL_PARTITIONPOLICY_H
