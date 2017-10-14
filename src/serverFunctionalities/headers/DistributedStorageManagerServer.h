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
#ifndef OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERSERVER_H
#define OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERSERVER_H

#include "PDBLogger.h"

#include "BroadcastServer.h"
#include "SimpleRequestResult.h"

namespace pdb {

class DistributedStorageManagerServer : public BroadcastServer {

public:
    // A new constructor added by Jia to take a Configuration parameter
    DistributedStorageManagerServer(PDBLoggerPtr logger, ConfigurationPtr conf);


    DistributedStorageManagerServer(PDBLoggerPtr logger);
    ~DistributedStorageManagerServer();

    void registerHandlers(PDBServer& forMe) override;

private:
    bool findNodesForDatabase(const std::string& databaseName,
                              std::vector<std::string>& nodesForDatabase,
                              std::string& errMsg);

    bool findNodesContainingDatabase(const std::string& databaseName,
                                     std::vector<std::string>& nodesContainingDatabase,
                                     std::string& errMsg);

    bool findNodesForSet(const std::string& databaseName,
                         const std::string& setName,
                         std::vector<std::string>& nodesContainingSet,
                         std::string& errMsg);

    bool findNodesContainingSet(const std::string& databaseName,
                                const std::string& setName,
                                std::vector<std::string>& nodesContainingSet,
                                std::string& errMsg);

    std::function<void(Handle<SimpleRequestResult>, std::string)> generateAckHandler(
        std::vector<std::string>& success, std::vector<std::string>& failures, mutex& lock);
};
}


#endif  // OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERSERVER_H
