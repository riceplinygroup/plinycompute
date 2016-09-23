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

#ifndef OBJECTQUERYMODEL_DISPATCHER_H
#define OBJECTQUERYMODEL_DISPATCHER_H

#include "ServerFunctionality.h"
#include "PDBLogger.h"
#include "PDBWork.h"
#include "PartitionPolicy.h"
#include "UseTemporaryAllocationBlock.h"
#include "PDBVector.h"

#include "NodeDispatcherData.h"
#include "StorageClient.h"

#include <string>
#include <queue>
#include <unordered_map>
#include <vector>

/**
 * The DispatcherServer partitions and then forwards a Vector of pdb::Objects received from a DispatcherClient
 * to the proper storage servers
 */

namespace pdb {

class DispatcherServer : public ServerFunctionality {

public:

    DispatcherServer (PDBLoggerPtr logger);

    ~DispatcherServer();

    void initialize();

    /**
     * Inherited function from ServerFunctionality
     * @param forMe
     */
    void registerHandlers (PDBServer &forMe) override;

    /**
     * Updates PartitionPolicy with a collection of all the available storage nodes in the cluster
     *
     * @param storageNodes a vector of the live storage nodes
     */
    void registerStorageNodes(Handle<Vector<Handle<NodeDispatcherData>>> storageNodes);

    /**
     * Register a new set for the dispatcher to handle.
     *
     * @param setAndDatabase name of the set and its corresponding database
     * @param partitionPolicy policy by which to partition data for this set
     */
    void registerSet(std::pair<std::string, std::string> setAndDatabase, PartitionPolicyPtr partitionPolicy);

    /**
     * Dispatch a Vector of pdb::Object's to the correct StorageNodes as defined by that particular set's ParitionPolicy
     *
     * @param setAndDatabase name of the set and its corresponding database
     * @param toDispatch vector of pdb::Object's to dispatch
     * @return true on success
     */
    bool dispatchData(std::pair<std::string, std::string> setAndDatabase, Handle<Vector<Handle<Object>>> toDispatch);

private:

    PDBLoggerPtr logger;
    Handle<Vector<Handle<NodeDispatcherData>>> storageNodes;
    std::map<std::pair<std::string, std::string>, PartitionPolicyPtr> partitionPolicies;

    bool sendData(std::pair<std::string, std::string> setAndDatabase, Handle<NodeDispatcherData> destination,
                  Handle<Vector<Handle<Object>>> toSend);
    Handle<NodeDispatcherData> findNode(NodeID nodeId);

};
}





#endif //OBJECTQUERYMODEL_DISPATCHER_H
