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
#ifndef OBJECTQUERYMODEL_ROUNDROBINPOLICY_CC
#define OBJECTQUERYMODEL_ROUNDROBINPOLICY_CC

#include "PDBDebug.h"
#include "RoundRobinPolicy.h"

namespace pdb {

RoundRobinPolicy::RoundRobinPolicy() {
    this->storageNodes = std::vector<NodePartitionDataPtr>();
    pthread_mutex_init(&idMutex, nullptr);
}

RoundRobinPolicy::~RoundRobinPolicy() {}

unsigned int RoundRobinPolicy::curNodeId = 0;

void RoundRobinPolicy::updateStorageNodes(
    Handle<Vector<Handle<NodeDispatcherData>>> activeStorageNodesRaw) {

    auto oldNodes = storageNodes;
    auto activeStorageNodes = createNodePartitionData(activeStorageNodesRaw);
    storageNodes = std::vector<NodePartitionDataPtr>();

    for (int i = 0; i < activeStorageNodes.size(); i++) {
        bool alreadyContains = false;
        for (int j = 0; j < oldNodes.size(); j++) {
            if ((*activeStorageNodes[i]) == (*oldNodes[j])) {
                // Update the pre-existing node with the new information
                auto updatedNode = updateExistingNode(activeStorageNodes[i], oldNodes[j]);
                storageNodes.push_back(updatedNode);
                oldNodes.erase(oldNodes.begin() + j);
                alreadyContains = true;
                break;
            }
        }
        if (!alreadyContains) {
            storageNodes.push_back(updateNewNode(activeStorageNodes[i]));
        }
    }
    for (auto oldNode : oldNodes) {
        handleDeadNode(oldNode);
    }
    this->numNodes = storageNodes.size();
}

std::vector<NodePartitionDataPtr> RoundRobinPolicy::createNodePartitionData(
    Handle<Vector<Handle<NodeDispatcherData>>> storageNodes) {
    std::vector<NodePartitionDataPtr> newData = std::vector<NodePartitionDataPtr>();
    for (int i = 0; i < storageNodes->size(); i++) {
        auto nodeData = (*storageNodes)[i];
        auto newNode =
            std::make_shared<NodePartitionData>(nodeData->getNodeId(),
                                                nodeData->getPort(),
                                                nodeData->getAddress(),
                                                std::pair<std::string, std::string>("", ""));
        PDB_COUT << newNode->toString() << std::endl;
        newData.push_back(newNode);
    }
    return newData;
}

NodePartitionDataPtr RoundRobinPolicy::updateExistingNode(NodePartitionDataPtr newNode,
                                                          NodePartitionDataPtr oldNode) {
    PDB_COUT << "Updating existing node " << newNode->toString() << std::endl;
    return oldNode;
}

NodePartitionDataPtr RoundRobinPolicy::updateNewNode(NodePartitionDataPtr newNode) {
    PDB_COUT << "Updating new node " << newNode->toString() << std::endl;
    return newNode;
}

NodePartitionDataPtr RoundRobinPolicy::handleDeadNode(NodePartitionDataPtr deadNode) {
    PDB_COUT << "Deleting node " << deadNode->toString() << std::endl;
    return deadNode;
}

std::shared_ptr<std::unordered_map<NodeID, Handle<Vector<Handle<Object>>>>>
RoundRobinPolicy::partition(Handle<Vector<Handle<Object>>> toPartition) {

    auto partitionedData =
        std::make_shared<std::unordered_map<NodeID, Handle<Vector<Handle<Object>>>>>();
    if (storageNodes.size() == 0) {
        std::cout
            << "FATAL ERROR: there is no storage node in the cluster, please check conf/serverlist"
            << std::endl;
        exit(-1);
    }
    pthread_mutex_lock(&idMutex);
    curNodeId = (curNodeId + 1) % numNodes;
    auto nodeToUse = storageNodes[curNodeId];
    pthread_mutex_unlock(&idMutex);
    partitionedData->insert(
        std::pair<NodeID, Handle<Vector<Handle<Object>>>>(nodeToUse->getNodeId(), toPartition));
    return partitionedData;
}
}

#endif
