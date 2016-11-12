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
#include "CatalogServer.h"

#include "SimpleRequestHandler.h"
#include "SimpleRequestResult.h"
#include "DispatcherAddData.h"
#include "BuiltInObjectTypeIDs.h"

#include "PartitionPolicyFactory.h"
#include "DispatcherRegisterPartitionPolicy.h"

namespace pdb {

DispatcherServer :: DispatcherServer (PDBLoggerPtr logger) {
    this->logger = logger;
    this->storageNodes = pdb::makeObject<Vector<Handle<NodeDispatcherData>>>();
    this->partitionPolicies = std::map<std::pair<std::string, std::string>, PartitionPolicyPtr>();
}

void DispatcherServer :: initialize() {

}

DispatcherServer :: ~DispatcherServer () {

}

void DispatcherServer :: registerHandlers (PDBServer &forMe) {
    forMe.registerHandler(DispatcherAddData_TYPEID, make_shared<SimpleRequestHandler<DispatcherAddData>> (
            [&] (Handle <DispatcherAddData> request, PDBCommunicatorPtr sendUsingMe) {

                std :: string errMsg;
                bool res = true;
                std :: cout << "DispatcherAddData handler running" << std :: endl;
                // Receive the data to send
                size_t numBytes = sendUsingMe->getSizeOfNextObject();
                std :: cout << "NumBytes = " << numBytes << std :: endl;
                const UseTemporaryAllocationBlock tempBlock{numBytes + 1024};
                Handle<Vector<Handle<Object>>> dataToSend = sendUsingMe->getNextObject<Vector <Handle <Object>>> (res, errMsg);

                // Check that the type of the data being stored matches what is known to the catalog
                if (!validateTypes( request->getDatabaseName(), request->getSetName(), request->getTypeName(), errMsg)) {
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    res = sendUsingMe->sendObject (response, errMsg);
                    std :: cout << errMsg << std :: endl;
                    return make_pair(false, errMsg);
                }

                dispatchData(std::pair<std::string, std::string>(request->getSetName(), request->getDatabaseName()),
                             request->getTypeName(), dataToSend);

                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                res = sendUsingMe->sendObject (response, errMsg);

                return make_pair(res, errMsg);
    }));
    forMe.registerHandler(DispatcherRegisterPartitionPolicy_TYPEID, make_shared<SimpleRequestHandler<DispatcherRegisterPartitionPolicy>> (
            [&] (Handle <DispatcherRegisterPartitionPolicy> request, PDBCommunicatorPtr sendUsingMe) {

                std::cout << "Registering partition policy for set " << request->getSetName() << ":" << request->getDatabaseName() << std::endl;

                std :: string errMsg;
                bool res = true;

                registerSet(std::pair<std::string, std::string>(request->getSetName(), request->getDatabaseName()),
                            PartitionPolicyFactory::buildPartitionPolicy(request->getPolicy()));

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
        registerSet(setAndDatabase, PartitionPolicyFactory::buildDefaultPartitionPolicy());
        return dispatchData(setAndDatabase, type, toDispatch);
    } else {
        auto mappedPartitions = partitionPolicies[setAndDatabase]->partition(toDispatch);
        std :: cout << "mappedPartitions size = " << mappedPartitions->size() << std :: endl;
        for (auto const &pair : (* mappedPartitions)) {
            if (!sendData(setAndDatabase, type, findNode(pair.first), pair.second)) {
                return false;
            }
        }
        return true;
    }
}

bool DispatcherServer :: validateTypes (const std::string& databaseName, const std::string& setName,
        const std::string& typeName, std::string& errMsg) {
    std :: cout << "running validateTypes with typeName" << typeName << std :: endl;
    std::string fullSetName = databaseName + "." + setName;
    int catalogType = PDBCatalogMsgType::CatalogPDBSet;
    Handle<pdb::Vector<CatalogSetMetadata>> returnValues = makeObject<pdb::Vector<CatalogSetMetadata>>();

    if (getFunctionality<CatalogServer>().getCatalog()->getMetadataFromCatalog<CatalogSetMetadata>(false,
            fullSetName, returnValues, errMsg, catalogType)) {
        if (returnValues->size() == 0) {
            errMsg = "Set " + fullSetName + " cannot be found in the catalog";
            std :: cout << errMsg << std :: endl;
            return false;
        } else {
            if ((* returnValues)[0].getObjectTypeName() == typeName) {
                std :: cout << "validateTypes succeed" << std :: endl;
                return true;
            } else {
                errMsg = "Dispatched type " + typeName + " does not match stored type " +
                        (* returnValues)[0].getObjectTypeName().c_str();
                std :: cout << errMsg << std :: endl;
                return false;
            }
        }
    }
    std :: cout << fullSetName << std :: endl;
    std :: cout << errMsg << std :: endl;
    return false;
}

bool DispatcherServer :: sendData (std::pair<std::string, std::string> setAndDatabase, std::string type,
                                   Handle<NodeDispatcherData> destination, Handle<Vector<Handle<Object>>> toSend) {

    std::cout << "Sending data to " << destination->getPort() << " : " << destination->getAddress() << std::endl;
    std::string err;
    StorageClient storageClient = StorageClient(destination->getPort(), destination->getAddress(), logger);
    if (!storageClient.storeData(toSend, setAndDatabase.second, setAndDatabase.first, type, err)) {
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
