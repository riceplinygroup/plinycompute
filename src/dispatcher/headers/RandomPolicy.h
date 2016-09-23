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
//
// Created by Joseph Hwang on 9/12/16.
//

#ifndef OBJECTQUERYMODEL_RANDOMPOLICY_H
#define OBJECTQUERYMODEL_RANDOMPOLICY_H

#include "PartitionPolicy.h"

#include <random>

namespace pdb {

class RandomPolicy;
typedef std::shared_ptr<RandomPolicy> RandomPolicyPtr;

/**
 * RandomPolicy simply selects a random node from its Storage Nodes List to send the entire Vector of data to. We send
 * the entire Vector to a single node instead of partitioning it on an Object granularity to save time.
 */
class RandomPolicy : public PartitionPolicy {
public:

    RandomPolicy();
    ~RandomPolicy();

    void updateStorageNodes(Handle<Vector<Handle<NodeDispatcherData>>> storageNodes);

    std::shared_ptr<std::unordered_map<NodeID, Handle<Vector<Handle<Object>>>>>
        partition(Handle<Vector <Handle <Object>>> toPartition);

private:

    // Seed used for the PRNG. It is configurable so that we can deterministically test RandomPolicy's behavior.
    const int SEED = 0;

    std::vector<Handle<NodeDispatcherData>> storageNodes;

    Handle<NodeDispatcherData> updateExistingNode(Handle<NodeDispatcherData> newNodeData,
                                                    Handle<NodeDispatcherData> oldNodeData);
    Handle<NodeDispatcherData> updateNewNode(Handle<NodeDispatcherData> newNode);
    Handle<NodeDispatcherData> handleDeadNode(Handle<NodeDispatcherData> deadNode);
};

}


#endif //OBJECTQUERYMODEL_RandomPOLICY_H
