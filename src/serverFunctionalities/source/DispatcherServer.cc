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

#ifndef DISPATCHER_SERVER_CC
#define DISPATCHER_SERVER_CC

#include "DispatcherServer.h"
#include "SimpleRequestHandler.h"
#include "SimpleRequestResult.h"
#include "DispatcherAddData.h"
#include "BuiltInObjectTypeIDs.h"

#include "RandomPolicy.h"

namespace pdb {

DispatcherServer :: DispatcherServer (PDBLoggerPtr logger) {
    this->logger = logger;
    this->storageNodes = pdb::makeObject<Vector<Handle<NodeDispatcherData>>>();
    this->partitionPolicies = std::map<std::pair<std::string, std::string>, PartitionPolicyPtr>();

    // TODO: Assume we have node dispatcher, and then Resource Manager

}

void DispatcherServer :: initialize() {
    // TODO JIA: registerStorageNodes
}

DispatcherServer :: ~DispatcherServer () {

}

void DispatcherServer :: registerHandlers (PDBServer &forMe) {
    forMe.registerHandler(DispatcherAddData_TYPEID, make_shared<SimpleRequestHandler<DispatcherAddData>> (
            [&] (Handle <DispatcherAddData> request, PDBCommunicatorPtr sendUsingMe) {

                std :: string errMsg;
                bool res = true;

                size_t numBytes = sendUsingMe->getSizeOfNextObject();
                const UseTemporaryAllocationBlock tempBlock{numBytes + 1024};

                Handle<Vector<Handle<Object>>> dataToSend = sendUsingMe->getNextObject<Vector <Handle <Object>>> (res, errMsg);
                dispatchData(std::pair<std::string, std::string>(request->getSetName(), request->getDatabaseName()),
                             request->getTypeName(), dataToSend);

                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                res = sendUsingMe->sendObject (response, errMsg);

                return make_pair(res, errMsg);
    }));
}

void DispatcherServer :: registerStorageNodes(Handle<Vector<Handle<NodeDispatcherData>>> storageNodes) {
    this->storageNodes = storageNodes;
    for (int i = 0; i < storageNodes->size(); i++) {
        auto node = (*storageNodes)[i];
        std::cout << "Dispatcher register node: " << node->getAddress() << " : " << node->getPort()  << std::endl;
    }

    for (auto const partitionPolicy : partitionPolicies) {
        partitionPolicy.second->updateStorageNodes(storageNodes);
    }
}

void DispatcherServer :: registerSet(std::pair<std::string, std::string> setAndDatabase, PartitionPolicyPtr partitionPolicy) {
    if (partitionPolicies.find(setAndDatabase) != partitionPolicies.end()) {
        std::cout << "Updating old set" << setAndDatabase.first << ":" << setAndDatabase.second << std::endl;
    } else {
        std::cout << "Found new set: " << setAndDatabase.first << ":" << setAndDatabase.second << std::endl;
    }
    partitionPolicies.insert(std::pair<std::pair<std::string, std::string>, PartitionPolicyPtr>(setAndDatabase, partitionPolicy));
    partitionPolicies[setAndDatabase]->updateStorageNodes(storageNodes);
}



bool DispatcherServer :: dispatchData (std::pair<std::string, std::string> setAndDatabase, std::string type, Handle<Vector<Handle<Object>>> toDispatch) {
    // TODO: Implement this

    if (partitionPolicies.find(setAndDatabase) == partitionPolicies.end()) {
        std::cout << "No partition policy was found for set: " << setAndDatabase.first << ":" << setAndDatabase.second << std::endl;
        std::cout << "Defaulting to random policy" << std::endl;
        registerSet(setAndDatabase, std::make_shared<RandomPolicy>());
        return dispatchData(setAndDatabase, type, toDispatch);
    } else {

        auto mappedPartitions = partitionPolicies[setAndDatabase]->partition(toDispatch);

        for (auto const &pair : (* mappedPartitions)) {

            if (!sendData(setAndDatabase, type, findNode(pair.first), pair.second)) {
                return false;
            }
        }
        return true;
    }
}

bool DispatcherServer :: sendData (std::pair<std::string, std::string> setAndDatabase, std::string type,
                                   Handle<NodeDispatcherData> destination, Handle<Vector<Handle<Object>>> toSend) {

    std::cout << "Sending data to " << destination->getPort() << " : " << destination->getAddress() << std::endl;

    std::string err;
    StorageClient storageClient = StorageClient(destination->getPort(), destination->getAddress(), logger);
    if (!storageClient.storeData (toSend, setAndDatabase.second, setAndDatabase.first, type, err)) {
        cout << "Not able to store data: " << err << std::endl;
        return 0;
    }
    return 1;
}

Handle<NodeDispatcherData> DispatcherServer :: findNode(NodeID nodeId) {
    for (int i = 0; i < storageNodes->size(); i++) {
        auto storageNode = (* storageNodes)[i];
        if (storageNode->getNodeId() == nodeId) {
            return storageNode;
        }
    }
    return nullptr;
}

}

#endif
