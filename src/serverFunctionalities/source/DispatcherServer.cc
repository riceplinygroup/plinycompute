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
#include "PDBDebug.h"
#include "SimpleRequestHandler.h"
#include "SimpleRequestResult.h"
#include "DispatcherAddData.h"
#include "BuiltInObjectTypeIDs.h"
#include "QuerySchedulerServer.h"
#include "Statistics.h"
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
                PDB_COUT << "DispatcherAddData handler running" << std :: endl;
                // Receive the data to send
                size_t numBytes = sendUsingMe->getSizeOfNextObject();
                PDB_COUT << "NumBytes = " << numBytes << std :: endl;
                const UseTemporaryAllocationBlock tempBlock{numBytes + 2048};
                Handle<Vector<Handle<Object>>> dataToSend = sendUsingMe->getNextObject<Vector <Handle <Object>>> (res, errMsg);
                if (dataToSend->size() == 0) {
                    errMsg = "Warning: client attemps to store zero object vector";
                    Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(false, errMsg);
                    res = sendUsingMe->sendObject(response, errMsg);
                    std::cout << errMsg << std::endl;
                    return make_pair(false, errMsg);

                }
                // Check that the type of the data being stored matches what is known to the catalog
                if (!validateTypes( request->getDatabaseName(), request->getSetName(), request->getTypeName(), errMsg)) {
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    res = sendUsingMe->sendObject (response, errMsg);
                    std :: cout << errMsg << std :: endl;
                    return make_pair(false, errMsg);
                }
                //JiaNote: to accelerate data dispatching
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                res = sendUsingMe->sendObject (response, errMsg);

               
                dispatchData(std::pair<std::string, std::string>(request->getSetName(), request->getDatabaseName()),
                             request->getTypeName(), dataToSend);

                //update stats
                StatisticsPtr stats = getFunctionality<QuerySchedulerServer>().getStats();
                if (stats == nullptr) {
                    getFunctionality<QuerySchedulerServer>().collectStats();
                    stats = getFunctionality<QuerySchedulerServer>().getStats();
                }
                size_t oldNumBytes = stats->getNumBytes(request->getDatabaseName(), request->getSetName());
                size_t newNumBytes = oldNumBytes + numBytes;
                stats->setNumBytes (request->getDatabaseName(), request->getSetName(), newNumBytes);

                //Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                //res = sendUsingMe->sendObject (response, errMsg);

                return make_pair(res, errMsg);
    }));

    forMe.registerHandler(DispatcherRegisterPartitionPolicy_TYPEID, make_shared<SimpleRequestHandler<DispatcherRegisterPartitionPolicy>> (
            [&] (Handle <DispatcherRegisterPartitionPolicy> request, PDBCommunicatorPtr sendUsingMe) {

                PDB_COUT << "Registering partition policy for set " << request->getSetName() << ":" << request->getDatabaseName() << std::endl;

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
        PDB_COUT << "Dispatcher register node: " << node->getAddress() << " : " << node->getPort()  << std::endl;
    }

    for (auto const partitionPolicy : partitionPolicies) {
        partitionPolicy.second->updateStorageNodes(storageNodes);
    }
}

void DispatcherServer :: registerSet(std::pair<std::string, std::string> setAndDatabase, PartitionPolicyPtr partitionPolicy) {
    if (partitionPolicies.find(setAndDatabase) != partitionPolicies.end()) {
        PDB_COUT << "Updating old set" << setAndDatabase.first << ":" << setAndDatabase.second << std::endl;
    } else {
        PDB_COUT << "Found new set: " << setAndDatabase.first << ":" << setAndDatabase.second << std::endl;
    }
    partitionPolicies.insert(std::pair<std::pair<std::string, std::string>, PartitionPolicyPtr>(setAndDatabase, partitionPolicy));
    partitionPolicies[setAndDatabase]->updateStorageNodes(storageNodes);
}



bool DispatcherServer :: dispatchData (std::pair<std::string, std::string> setAndDatabase, std::string type, Handle<Vector<Handle<Object>>> toDispatch) {
    // TODO: Implement this

    if (partitionPolicies.find(setAndDatabase) == partitionPolicies.end()) {
        PDB_COUT << "No partition policy was found for set: " << setAndDatabase.first << ":" << setAndDatabase.second << std::endl;
        PDB_COUT << "Defaulting to random policy" << std::endl;
        registerSet(setAndDatabase, PartitionPolicyFactory::buildDefaultPartitionPolicy());
        return dispatchData(setAndDatabase, type, toDispatch);
    } else {
        auto mappedPartitions = partitionPolicies[setAndDatabase]->partition(toDispatch);
        PDB_COUT << "mappedPartitions size = " << mappedPartitions->size() << std :: endl;
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
    PDB_COUT << "running validateTypes with typeName" << typeName << std :: endl;
/*    std::string fullSetName = databaseName + "." + setName;
    Handle<pdb::Vector<CatalogSetMetadata>> returnValues = makeObject<pdb::Vector<CatalogSetMetadata>>();

    getFunctionality<CatalogServer>().getCatalog()->getListOfSets(returnValues, fullSetName) ;

    if (returnValues->size() == 0) {
        errMsg = "Set " + fullSetName + " cannot be found in the catalog";
        std :: cout << errMsg << std :: endl;
        return false;
    } else {
        if ((* returnValues)[0].getObjectTypeName() == typeName) {
            PDB_COUT << "validateTypes succeed" << std :: endl;
            return true;
        } else {
            errMsg = "Dispatched type " + typeName + " does not match stored type " +
                    (* returnValues)[0].getObjectTypeName().c_str();
            std :: cout << errMsg << std :: endl;
            return false;
        }
    }
    PDB_COUT << fullSetName << std :: endl;
    std :: cout << errMsg << std :: endl;
    return false;
*/

    return true;
}

bool DispatcherServer :: sendData (std::pair<std::string, std::string> setAndDatabase, std::string type,
                                   Handle<NodeDispatcherData> destination, Handle<Vector<Handle<Object>>> toSend) {

    PDB_COUT << "Sending data to " << destination->getPort() << " : " << destination->getAddress() << std::endl;
    std::string err;
    StorageClient storageClient = StorageClient(destination->getPort(), destination->getAddress(), logger);
    if (!storageClient.storeData(toSend, setAndDatabase.second, setAndDatabase.first, type, err)) {
        PDB_COUT << "Not able to store data: " << err << std::endl;
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
