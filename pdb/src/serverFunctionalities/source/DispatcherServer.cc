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
#include "SimpleSendBytesRequest.h"
#include "DispatcherAddData.h"
#include "BuiltInObjectTypeIDs.h"
#include "QuerySchedulerServer.h"
#include "Statistics.h"
#include "PartitionPolicyFactory.h"
#include "DispatcherRegisterPartitionPolicy.h"
#include <snappy.h>
#define MAX_CONCURRENT_REQUESTS 10

namespace pdb {

DispatcherServer::DispatcherServer(PDBLoggerPtr logger) {
    this->logger = logger;
    this->storageNodes = pdb::makeObject<Vector<Handle<NodeDispatcherData>>>();
    this->partitionPolicies = std::map<std::pair<std::string, std::string>, PartitionPolicyPtr>();
    pthread_mutex_init(&mutex, nullptr);
    numRequestsInProcessing = 0;
}

void DispatcherServer::initialize() {}

DispatcherServer::~DispatcherServer() {
    pthread_mutex_destroy(&mutex);
}

void DispatcherServer::registerHandlers(PDBServer& forMe) {
    forMe.registerHandler(
        DispatcherAddData_TYPEID,
        make_shared<SimpleRequestHandler<DispatcherAddData>>([&](Handle<DispatcherAddData> request,
                                                                 PDBCommunicatorPtr sendUsingMe) {
            pthread_mutex_lock(&mutex);
            while (numRequestsInProcessing > MAX_CONCURRENT_REQUESTS) {
                pthread_mutex_unlock(&mutex);
                sleep(1);
                pthread_mutex_lock(&mutex);
            }
            numRequestsInProcessing += 1;
            pthread_mutex_unlock(&mutex);
            std::string errMsg;
            bool res = true;
            PDB_COUT << "DispatcherAddData handler running" << std::endl;
            // Receive the data to send
            size_t numBytes = sendUsingMe->getSizeOfNextObject();
            std::cout << "Dispacher received numBytes = " << numBytes << std::endl;
            Handle<Vector<Handle<Object>>> dataToSend;
            char* tempPage = nullptr;
            char* readToHere = nullptr;
            if (request->isShallowCopy() == false) {
                const UseTemporaryAllocationBlock tempBlock{numBytes + 65535};
                dataToSend = sendUsingMe->getNextObject<Vector<Handle<Object>>>(res, errMsg);
            } else {
#ifdef ENABLE_COMPRESSION
                tempPage = new char[numBytes];
                sendUsingMe->receiveBytes(tempPage, errMsg);
#else
                readToHere = malloc(numBytes);
                sendUsingMe->receiveBytes(readToHere, errMsg);
#endif

#ifdef ENABLE_COMPRESSION
                size_t uncompressedSize = 0;
                snappy::GetUncompressedLength(tempPage, numBytes, &uncompressedSize);
                readToHere = (char*)malloc(uncompressedSize);
                snappy::RawUncompress(tempPage, numBytes, (char*)(readToHere));
                Record<Vector<Handle<Object>>>* myRecord =
                    (Record<Vector<Handle<Object>>>*)readToHere;
                dataToSend = myRecord->getRootObject();
#endif
            }
            if (dataToSend->size() == 0) {
                errMsg = "Warning: client attemps to store zero object vector";
                Handle<SimpleRequestResult> response =
                    makeObject<SimpleRequestResult>(false, errMsg);
                res = sendUsingMe->sendObject(response, errMsg);
                std::cout << errMsg << std::endl;
                return make_pair(false, errMsg);

            } else {
                std::cout << "Dispatch to send vector size = " << dataToSend->size() << std::endl;
            }
            // Check that the type of the data being stored matches what is known to the catalog
            if (!validateTypes(request->getDatabaseName(),
                               request->getSetName(),
                               request->getTypeName(),
                               errMsg)) {
                Handle<SimpleRequestResult> response =
                    makeObject<SimpleRequestResult>(false, errMsg);
                res = sendUsingMe->sendObject(response, errMsg);
                std::cout << errMsg << std::endl;
                return make_pair(false, errMsg);
            }
            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);
            res = sendUsingMe->sendObject(response, errMsg);

            if (request->isShallowCopy() == false) {
                dispatchData(std::pair<std::string, std::string>(request->getSetName(),
                                                                 request->getDatabaseName()),
                             request->getTypeName(),
                             dataToSend);
            } else {

#ifdef ENABLE_COMPRESSION
                dispatchBytes(std::pair<std::string, std::string>(request->getSetName(),
                                                                  request->getDatabaseName()),
                              request->getTypeName(),
                              tempPage,
                              numBytes);
                free(tempPage);
#else
                dispatchBytes(std::pair<std::string, std::string>(request->getSetName(),
                                                                  request->getDatabaseName()),
                              request->getTypeName(),
                              readToHere,
                              numBytes);
#endif
                free(readToHere);
            }

            // update stats
            pthread_mutex_lock(&mutex);
            StatisticsPtr stats = getFunctionality<QuerySchedulerServer>().getStats();
            if (stats == nullptr) {
                getFunctionality<QuerySchedulerServer>().collectStats();
                stats = getFunctionality<QuerySchedulerServer>().getStats();
            }
            size_t oldNumBytes =
                stats->getNumBytes(request->getDatabaseName(), request->getSetName());
            size_t newNumBytes = oldNumBytes + numBytes;
            stats->setNumBytes(request->getDatabaseName(), request->getSetName(), newNumBytes);
            numRequestsInProcessing = numRequestsInProcessing - 1;
            pthread_mutex_unlock(&mutex);
            return make_pair(res, errMsg);
        }));

    forMe.registerHandler(
        DispatcherRegisterPartitionPolicy_TYPEID,
        make_shared<SimpleRequestHandler<DispatcherRegisterPartitionPolicy>>(
            [&](Handle<DispatcherRegisterPartitionPolicy> request, PDBCommunicatorPtr sendUsingMe) {

                PDB_COUT << "Registering partition policy for set " << request->getSetName() << ":"
                         << request->getDatabaseName() << std::endl;

                std::string errMsg;
                bool res = true;

                registerSet(std::pair<std::string, std::string>(request->getSetName(),
                                                                request->getDatabaseName()),
                            PartitionPolicyFactory::buildPartitionPolicy(request->getPolicy()));

                Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);
                res = sendUsingMe->sendObject(response, errMsg);

                return make_pair(res, errMsg);
            }));
}

void DispatcherServer::registerStorageNodes(
    Handle<Vector<Handle<NodeDispatcherData>>> storageNodes) {
    this->storageNodes = storageNodes;
    for (int i = 0; i < storageNodes->size(); i++) {
        auto node = (*storageNodes)[i];
        PDB_COUT << "Dispatcher register node: " << node->getAddress() << " : " << node->getPort()
                 << std::endl;
    }

    for (auto const partitionPolicy : partitionPolicies) {
        partitionPolicy.second->updateStorageNodes(storageNodes);
    }
}

void DispatcherServer::registerSet(std::pair<std::string, std::string> setAndDatabase,
                                   PartitionPolicyPtr partitionPolicy) {
    if (partitionPolicies.find(setAndDatabase) != partitionPolicies.end()) {
        PDB_COUT << "Updating old set" << setAndDatabase.first << ":" << setAndDatabase.second
                 << std::endl;
    } else {
        PDB_COUT << "Found new set: " << setAndDatabase.first << ":" << setAndDatabase.second
                 << std::endl;
    }
    partitionPolicies.insert(std::pair<std::pair<std::string, std::string>, PartitionPolicyPtr>(
        setAndDatabase, partitionPolicy));
    partitionPolicies[setAndDatabase]->updateStorageNodes(storageNodes);
}


bool DispatcherServer::dispatchData(std::pair<std::string, std::string> setAndDatabase,
                                    std::string type,
                                    Handle<Vector<Handle<Object>>> toDispatch) {
    // TODO: Implement this

    if (partitionPolicies.find(setAndDatabase) == partitionPolicies.end()) {
        PDB_COUT << "No partition policy was found for set: " << setAndDatabase.first << ":"
                 << setAndDatabase.second << std::endl;
        PDB_COUT << "Defaulting to random policy" << std::endl;
        registerSet(setAndDatabase, PartitionPolicyFactory::buildDefaultPartitionPolicy());
        return dispatchData(setAndDatabase, type, toDispatch);
    } else {
        auto mappedPartitions = partitionPolicies[setAndDatabase]->partition(toDispatch);
        PDB_COUT << "mappedPartitions size = " << mappedPartitions->size() << std::endl;
        for (auto const& pair : (*mappedPartitions)) {
            if (!sendData(setAndDatabase, type, findNode(pair.first), pair.second)) {
                return false;
            }
        }
        return true;
    }
}


bool DispatcherServer::dispatchBytes(std::pair<std::string, std::string> setAndDatabase,
                                     std::string type,
                                     char* bytes,
                                     size_t numBytes) {
    // TODO: Implement this

    if (partitionPolicies.find(setAndDatabase) == partitionPolicies.end()) {
        PDB_COUT << "No partition policy was found for set: " << setAndDatabase.first << ":"
                 << setAndDatabase.second << std::endl;
        PDB_COUT << "Defaulting to random policy" << std::endl;
        registerSet(setAndDatabase, PartitionPolicyFactory::buildDefaultPartitionPolicy());
        return dispatchBytes(setAndDatabase, type, bytes, numBytes);
    } else {
        auto mappedPartitions = partitionPolicies[setAndDatabase]->partition(nullptr);
        PDB_COUT << "mappedPartitions size = " << mappedPartitions->size() << std::endl;
        for (auto const& pair : (*mappedPartitions)) {
            if (!sendBytes(setAndDatabase, type, findNode(pair.first), bytes, numBytes)) {
                return false;
            }
        }
        return true;
    }
}


bool DispatcherServer::validateTypes(const std::string& databaseName,
                                     const std::string& setName,
                                     const std::string& typeName,
                                     std::string& errMsg) {
    PDB_COUT << "running validateTypes with typeName" << typeName << std::endl;
    /*    std::string fullSetName = databaseName + "." + setName;
        Handle<pdb::Vector<CatalogSetMetadata>> returnValues =
       makeObject<pdb::Vector<CatalogSetMetadata>>();

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

bool DispatcherServer::sendData(std::pair<std::string, std::string> setAndDatabase,
                                std::string type,
                                Handle<NodeDispatcherData> destination,
                                Handle<Vector<Handle<Object>>> toSend) {

    PDB_COUT << "Sending data to " << destination->getPort() << " : " << destination->getAddress()
             << std::endl;
    std::string err;
    StorageClient storageClient =
        StorageClient(destination->getPort(), destination->getAddress(), logger);
    if (!storageClient.storeData(toSend, setAndDatabase.second, setAndDatabase.first, type, err)) {
        PDB_COUT << "Not able to store data: " << err << std::endl;
        return 0;
    }
    return 1;
}

bool DispatcherServer::sendBytes(std::pair<std::string, std::string> setAndDatabase,
                                 std::string type,
                                 Handle<NodeDispatcherData> destination,
                                 char* bytes,
                                 size_t numBytes) {
#ifndef ENABLE_COMPRESSION
    std::cout << "Now only objects or compressed bytes can be dispatched!!" << std::endl;
#endif
    int port = destination->getPort();
    std::string address = destination->getAddress();
    std::string databaseName = setAndDatabase.second;
    std::string setName = setAndDatabase.first;
    std::string errMsg;
    std::cout << "store compressed bytes to address=" << address << " and port=" << port
              << ", with compressed byte size = " << numBytes << " to database=" << databaseName
              << " and set=" << setName << " and type = IntermediateData" << std::endl;
    return simpleSendBytesRequest<StorageAddData, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr)
                if (!result->getRes().first) {
                    logger->error("Error sending data: " + result->getRes().second);
                    errMsg = "Error sending data: " + result->getRes().second;
                }
            return true;
        },
        bytes,
        numBytes,
        databaseName,
        setName,
        "IntermediateData",
        false,
        true,
        true,
        true);
}


Handle<NodeDispatcherData> DispatcherServer::findNode(NodeID nodeId) {
    for (int i = 0; i < storageNodes->size(); i++) {
        auto storageNode = (*storageNodes)[i];
        if (storageNode->getNodeId() == nodeId) {
            return storageNode;
        }
    }
    return nullptr;
}
}

#endif
