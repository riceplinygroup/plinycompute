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
#ifndef OBJECTQUERYMODEL_ROUNDROBINPOLICY_H
#define OBJECTQUERYMODEL_ROUNDROBINPOLICY_H

//created by Jia, Aug 2017


#include "PartitionPolicy.h"
#include <pthread.h>

namespace pdb {

class RoundRobinPolicy;
typedef std::shared_ptr<RoundRobinPolicy> RoundRobinPolicyPtr;

/**
 * RoundRobinPolicy simply selects the next node from its Storage Nodes List to send the entire Vector of data to. We send
 * the entire Vector to a single node instead of partitioning it on an Object granularity to save time.
 */
class RoundRobinPolicy : public PartitionPolicy {
public:

    RoundRobinPolicy();
    ~RoundRobinPolicy();
    static unsigned int curNodeId;
    int numNodes = 0;
    pthread_mutex_t idMutex;
    void updateStorageNodes(Handle<Vector<Handle<NodeDispatcherData>>> storageNodes);

    std::shared_ptr<std::unordered_map<NodeID, Handle<Vector<Handle<Object>>>>>
        partition(Handle<Vector <Handle <Object>>> toPartition);

    

private:

    std::vector<NodePartitionDataPtr> createNodePartitionData(Handle<Vector<Handle<NodeDispatcherData>>> storageNodes);
    NodePartitionDataPtr updateExistingNode(NodePartitionDataPtr newNodeData,
                                                    NodePartitionDataPtr oldNodeData);
    NodePartitionDataPtr updateNewNode(NodePartitionDataPtr newNode);
    NodePartitionDataPtr handleDeadNode(NodePartitionDataPtr deadNode);
};

}


#endif //OBJECTQUERYMODEL_ROUNDROBINPOLICY_H
