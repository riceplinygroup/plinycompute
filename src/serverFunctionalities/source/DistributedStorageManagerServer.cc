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
#ifndef OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERSERVER_CC
#define OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERSERVER_CC

#include "DistributedStorageManagerServer.h"
#include "CatalogClient.h"
#include "CatalogServer.h"
#include "ResourceManagerServer.h"
#include "PDBCatalogMsgType.h"
#include "CatalogDatabaseMetadata.h"
#include "CatalogSetMetadata.h"
#include "SimpleRequestHandler.h"

#include "DistributedStorageAddDatabase.h"
#include "DistributedStorageAddSet.h"
#include "DistributedStorageRemoveDatabase.h"
#include "DistributedStorageRemoveSet.h"

#include "StorageAddDatabase.h"
#include "StorageAddSet.h"
#include "StorageRemoveDatabase.h"
#include "StorageRemoveUserSet.h"


namespace pdb {

DistributedStorageManagerServer::DistributedStorageManagerServer(PDBLoggerPtr logger) : BroadcastServer(logger) {
    // no-op
}

DistributedStorageManagerServer::~DistributedStorageManagerServer() {
    // no-op
}

void DistributedStorageManagerServer::registerHandlers (PDBServer &forMe) {

    /**
     * Handler that distributes an add database request
     */
    forMe.registerHandler(DistributedStorageAddDatabase_TYPEID, make_shared<SimpleRequestHandler<DistributedStorageAddDatabase>> (
            [&] (Handle <DistributedStorageAddDatabase> request, PDBCommunicatorPtr sendUsingMe) {

                std::string errMsg;
                std::string database = request->getDatabase();
                std::string value;
                int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;

                if (getFunctionality<CatalogServer>().getCatalog()->keyIsFound(catalogType, database, value)) {
                    std::cout << "Database " << database << " already exists " << std::endl;
                } else {
                    std::cout << "Database " << database << " does not exist" << std::endl;
                    if (!getFunctionality<CatalogClient>().createDatabase(database, errMsg)) {
                        std::cout << "Could not register db, because: " << errMsg << std::endl;
                        Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                        bool res = sendUsingMe->sendObject (response, errMsg);
                        return make_pair (res, errMsg);
                    }
                }

                mutex lock;
                auto successfulNodes = std::vector<std::string>();
                auto failureNodes = std::vector<std::string>();
                auto nodesToBroadcastTo = std::vector<std::string>();
                if (!findNodesForDatabase(database, nodesToBroadcastTo, errMsg)) {
                    std::cout << "Could not find nodes to broadcast database to: " << errMsg << std::endl;
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    bool res = sendUsingMe->sendObject (response, errMsg);
                    return make_pair (res, errMsg);
                }

                Handle<StorageAddDatabase> storageCmd = makeObject<StorageAddDatabase>(request->getDatabase());
                broadcast<StorageAddDatabase, Object, SimpleRequestResult>(storageCmd, nullptr, nodesToBroadcastTo,
                    generateAckHandler(successfulNodes, failureNodes, lock),
                    [&] (std::string errMsg, std::string serverName) {
                        lock.lock();
                        std::cout << "Server "<< serverName << " received an error: " << errMsg << std::endl;
                        failureNodes.push_back(serverName);
                        lock.unlock();
                    }
                );

                bool res = true;

                for (auto node : successfulNodes) {
                    if (!getFunctionality<CatalogClient>().addNodeToDB(node, request->getDatabase(), errMsg)) {
                        // TODO: Handle error
                        std::cout << "Failed to register node " << node << " for database "
                                  << request->getDatabase() << " in Catalog" << std::endl;
                    }
                }

                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
    ));

    forMe.registerHandler(DistributedStorageAddSet_TYPEID, make_shared<SimpleRequestHandler<DistributedStorageAddSet>> (
            [&] (Handle <DistributedStorageAddSet> request, PDBCommunicatorPtr sendUsingMe) {

                std::string errMsg;
                mutex lock;

                auto successfulNodes = std::vector<std::string>();
                auto failureNodes = std::vector<std::string>();
                auto nodesToBroadcast = std::vector<std::string>();

                std::string database = request->getDatabase();
                std::string set = request->getSetName();
                std::string fullSetName = database + "." + set;
                std::string value;
                int catalogType = PDBCatalogMsgType::CatalogPDBSet;

                if (getFunctionality<CatalogServer>().getCatalog()->keyIsFound(catalogType, fullSetName, value)) {
                    std::cout << "Set " << fullSetName << " already exists " << std::endl;
                } else {
                    std::cout << "Set " << fullSetName << " does not exist" << std::endl;

                    int16_t typeId = getFunctionality<CatalogClient>().searchForObjectTypeName(request->getTypeName());
                    // TODO: Check if type matches the known type
                    if (typeId == 0) {
                        return make_pair (false, "Could not identify type=" + request->getTypeName());
                    }
                    if (!getFunctionality<CatalogClient>().createSet(typeId, database, set, errMsg)) {
                        std::cout << "Could not register set, because: " << errMsg << std::endl;
                        Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                        bool res = sendUsingMe->sendObject (response, errMsg);
                        return make_pair (res, errMsg);
                    }
                }

                if (!findNodesForSet(database, set, nodesToBroadcast, errMsg)) {
                    std::cout << "Could not find nodes to broadcast set to: " << errMsg << std::endl;
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    bool res = sendUsingMe->sendObject (response, errMsg);
                    return make_pair (res, errMsg);
                }

                Handle<StorageAddSet> storageCmd = makeObject<StorageAddSet>(request->getDatabase(),
                                                                             request->getSetName(), request->getTypeName());


                broadcast<StorageAddSet, Object, SimpleRequestResult>(storageCmd, nullptr, nodesToBroadcast,
                                                                      generateAckHandler(successfulNodes, failureNodes, lock));

                for (auto node : successfulNodes) {
                    if (!getFunctionality<CatalogClient>().addNodeToSet(node, database, set, errMsg)) {
                        std::cout << "Failed to register node " << node << " for set "
                                  << fullSetName << " in Catalog" << std::endl;
                    }
                }

                bool res = true;
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
    ));

    forMe.registerHandler(DistributedStorageRemoveDatabase_TYPEID, make_shared<SimpleRequestHandler<DistributedStorageRemoveDatabase>> (
            [&] (Handle <DistributedStorageRemoveDatabase> request, PDBCommunicatorPtr sendUsingMe) {

                std::string errMsg;
                mutex lock;
                std::vector<std::string> successfulNodes = std::vector<std::string>();
                std::vector<std::string> failureNodes = std::vector<std::string>();

                std::string database = request->getDatabase();
                std::string value;
                int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;

                if (!getFunctionality<CatalogServer>().getCatalog()->keyIsFound(catalogType, database, value)) {
                    errMsg = "Cannot delete database, database " + database + " does not exist\n";
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    bool res = sendUsingMe->sendObject (response, errMsg);
                    return make_pair (res, errMsg);
                }

                auto nodesToBroadcastTo = std::vector<std::string>();
                if (!findNodesContainingDatabase(database, nodesToBroadcastTo, errMsg)) {
                    std::cout << "Could not find nodes to broadcast database delete to " << errMsg << std::endl;
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    bool res = sendUsingMe->sendObject (response, errMsg);
                    return make_pair (res, errMsg);
                }

                Handle<StorageRemoveDatabase> storageCmd = makeObject<StorageRemoveDatabase>(database);
                broadcast<StorageRemoveDatabase, Object, SimpleRequestResult>(storageCmd, nullptr,
                    nodesToBroadcastTo,
                    generateAckHandler(successfulNodes, failureNodes, lock));

                if (failureNodes.size() == 0) {
                    std::cout << "Successfully deleted database on " << successfulNodes.size() << " nodes" << std::endl;
                } else {
                    errMsg = "Failed to delete database on " + std::to_string(failureNodes.size()) +
                             " nodes. Skipping registering with catalog";
                    std::cout << errMsg;
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    bool res = sendUsingMe->sendObject (response, errMsg);
                    return make_pair (res, errMsg);
                }

                if (!getFunctionality<CatalogClient>().deleteDatabase(database, errMsg)) {
                    std::cout << "Could not delete database, because: " << errMsg << std::endl;
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    bool res = sendUsingMe->sendObject (response, errMsg);
                    return make_pair (res, errMsg);
                }

                bool res = true;

                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
    ));

    forMe.registerHandler(DistributedStorageRemoveSet_TYPEID, make_shared<SimpleRequestHandler<DistributedStorageRemoveSet>> (
            [&] (Handle <DistributedStorageRemoveSet> request, PDBCommunicatorPtr sendUsingMe) {

                std::string errMsg;
                mutex lock;
                auto successfulNodes = std::vector<std::string>();
                auto failureNodes = std::vector<std::string>();
                auto nodesToBroadcast = std::vector<std::string>();

                std::string database = request->getDatabase();
                std::string set = request->getSetName();
                std::string fullSetName = database + "." + set;
                int catalogType = PDBCatalogMsgType::CatalogPDBSet;

                Handle<pdb::Vector<CatalogSetMetadata>> returnValues = makeObject<pdb::Vector<CatalogSetMetadata>>();

                std::string typeName;

                if (getFunctionality<CatalogServer>().getCatalog()->getMetadataFromCatalog<CatalogSetMetadata>(false,
                    fullSetName, returnValues, errMsg, catalogType)) {
                    if (returnValues->size() == 0) {
                        std::cout << "Cannot remove set, Set " << fullSetName << " does not exist " << std::endl;
                        Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                        bool res = sendUsingMe->sendObject (response, errMsg);
                        return make_pair (res, errMsg);
                    } else {
                        typeName = (*returnValues)[0].getObjectTypeName();
                    }
                }

                if (!findNodesContainingSet(database, set, nodesToBroadcast, errMsg)) {
                    std::cout << "Could not find nodes to broadcast set to: " << errMsg << std::endl;
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    bool res = sendUsingMe->sendObject (response, errMsg);
                    return make_pair (res, errMsg);
                }

                Handle<StorageRemoveUserSet> storageCmd = makeObject<StorageRemoveUserSet>(request->getDatabase(),
                    request->getSetName(), typeName);
                broadcast<StorageRemoveUserSet, Object, SimpleRequestResult>(storageCmd, nullptr,
                    nodesToBroadcast,
                    generateAckHandler(successfulNodes, failureNodes, lock));

                if (failureNodes.size() == 0) {
                    std::cout << "Successfully deleted set on " << successfulNodes.size() << " nodes" << std::endl;
                } else {
                    errMsg = "Failed to delete set on " + std::to_string(failureNodes.size()) +
                             " nodes. Skipping registering with catalog";
                    std::cout << errMsg << std::endl;
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (false, errMsg);
                    bool res = sendUsingMe->sendObject (response, errMsg);
                    return make_pair (res, errMsg);
                }

                if (failureNodes.size() == 0) {
                    // If all the nodes succeeded in removing the set then we can simply delete the set from the
                    // catalog
                    if (!getFunctionality<CatalogClient>().deleteSet(database, set, errMsg)) {
                        std::cout << "Could not delete database, because: " << errMsg << std::endl;
                        Handle <SimpleRequestResult> response = makeObject<SimpleRequestResult>(false, errMsg);
                        bool res = sendUsingMe->sendObject(response, errMsg);
                        return make_pair(res, errMsg);
                    }
                } else {
                    // If some nodes failed in removing the set remove these nodes from the set maps in the catalog
                    // so that future calls to this function will only attempt to affect the unmodified storage nodes
                    errMsg = "Nodes failed to remove set " + fullSetName + ": ";
                    for (auto node : successfulNodes) {
                        if (!getFunctionality<CatalogClient>().removeNodeFromSet(node, database, set, errMsg)) {
                            errMsg += node + ", ";
                            std::cout << errMsg << std::endl;
                        }
                    }
                    bool res = false;
                    Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                    sendUsingMe->sendObject (response, errMsg);
                    return make_pair (false, errMsg);
                }

                bool res = true;
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
    ));

}

std::function<void (Handle<SimpleRequestResult>, std::string)> DistributedStorageManagerServer::generateAckHandler(
        std::vector<std::string>& success, std::vector<std::string>& failures, mutex& lock) {
    return [&](Handle<SimpleRequestResult> response, std::string server){
        lock.lock();
        if (!response->getRes().first) {
            failures.push_back(server);
        } else {
            success.push_back(server);
        }
        lock.unlock();
    };
}

bool DistributedStorageManagerServer::findNodesForDatabase(const std::string& databaseName,
                                                           std::vector<std::string>& nodesForDatabase,
                                                           std::string& errMsg) {

    auto takenNodes = std::vector<std::string>();
    if (!findNodesContainingDatabase(databaseName, takenNodes, errMsg)) {
        return false;
    }

    std::vector<std::string> allNodes = std::vector<std::string>();
    const auto nodes = getFunctionality<ResourceManagerServer>().getAllNodes();
    for (int i = 0; i < nodes->size(); i++) {
        std::string address = static_cast<std::string>((*nodes)[i]->getAddress());
        std::string port = std::to_string((*nodes)[i]->getPort());
        allNodes.push_back(address + ":" + port);
    }

    for (auto node : allNodes) {
        if (std::find(takenNodes.begin(), takenNodes.end(), node) == takenNodes.end()) {
            nodesForDatabase.push_back(node);
        }
    }
    return true;
}

bool DistributedStorageManagerServer::findNodesContainingDatabase(const std::string& databaseName,
                                                                  std::vector<std::string>& nodesForDatabase,
                                                                  std::string& errMsg) {

    int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
    Handle<Vector<CatalogDatabaseMetadata>> returnValues = makeObject<Vector<CatalogDatabaseMetadata>>();

    if (getFunctionality<CatalogServer>().getCatalog()->getMetadataFromCatalog<CatalogDatabaseMetadata>(false,
            databaseName, returnValues, errMsg, catalogType)) {
        if (returnValues->size() != 1) {
            errMsg = "Could not find metadata for database: " + databaseName;
            std::cout << errMsg;
            return false;
        } else {
            auto nodesInDB = (* returnValues)[0].getNodesInDB();
            for (auto const& node :  (* (* returnValues)[0].getNodesInDB())) {
                nodesForDatabase.push_back(node.key);
            }
            return true;
        }
    }
    return false;

}

bool DistributedStorageManagerServer::findNodesForSet(const std::string& databaseName,
                                                      const std::string& setName,
                                                      std::vector<std::string>& nodesForSet,
                                                      std::string& errMsg ) {
    auto nodesInDatabase = std::vector<std::string>();
    if (!findNodesContainingDatabase(databaseName, nodesInDatabase, errMsg)) {
        return false;
    }

    auto nodesContainingSet = std::vector<std::string>();
    if (!findNodesContainingSet(databaseName, setName, nodesContainingSet, errMsg)) {
        return false;
    }

    for (auto node : nodesInDatabase) {
        if (std::find(nodesContainingSet.begin(), nodesContainingSet.end(), node) == nodesContainingSet.end()) {
            nodesForSet.push_back(node);
        }
    }

    return true;
}

bool DistributedStorageManagerServer::findNodesContainingSet(const std::string& databaseName,
                                                             const std::string& setName,
                                                             std::vector<std::string>& nodesContainingSet,
                                                             std::string& errMsg) {

    std::string fullSetName = databaseName + "." + setName;
    int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
    Handle<Vector<CatalogDatabaseMetadata>> returnValues = makeObject<Vector<CatalogDatabaseMetadata>>();

    if (getFunctionality<CatalogServer>().getCatalog()->getMetadataFromCatalog<CatalogDatabaseMetadata>(false,
            databaseName, returnValues, errMsg, catalogType)) {
        if (returnValues->size() != 1) {
            errMsg = "Could not find metadata for database: " + databaseName;
            return false;
        } else {
            bool setFound = false;
            auto listOfSets = (* returnValues)[0].getListOfSets();
            for (int i = 0; i < listOfSets->size(); i++) {
                if ((* listOfSets)[i] == setName) {
                    setFound = true;
                    break;
                }
            }
            if (!setFound) {
                errMsg = "Set " + fullSetName + " does not exist in database " + databaseName;
                return false;
            }
            auto setsInDB = (* returnValues)[0].getSetsInDB();
            String pdbSetName = String(setName);
            if (setsInDB->count(pdbSetName) == 0) {
                // The set is currently contained in no nodes
                return true;
            }
            auto nodes = (* setsInDB)[setName];
            for (int i = 0; i < nodes.size(); i++) {
                nodesContainingSet.push_back(nodes[i]);
            }
            return true;
        }
    }
    errMsg = "Database not found " + databaseName;
    return false;
}

}

#endif
