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
#include "StatisticsDB.h"

namespace pdb {

// This ServerFunctionality runs on the Manager server, and talks with
// the PangeaStorageServer deployed on each Worker server to serve
// storage-related requests. Include following:
// -- DistributedStorageAddDatabase: to add a database over the cluster
// -- DistributedStorageClearSet: to remove data (both in-memory or on-disk data) from a set
// -- DistributedStorageAddTempSet: to add a temp set (invisible to catalog) over the cluster
// -- DistributedStorageAddSet: to add a user set over the cluster
// -- DistributedStorageRemoveDatabase: to remove a database from the cluster
// -- DistributedStorageRemoveTempSet: to remove a temp set (invisible to catalog) 
//    from the cluster
// -- DistributedStorageRemoveSet: to remove a user set over the cluster
// -- DistributedStorageCleanup: to flush buffered data to disk for all storage nodes
// -- DistributedStorageExportSet: to export a set to user specified format
// -- SetScan: to scan all pages in a set from the client (One thread)



class DistributedStorageManagerServer : public BroadcastServer {

public:
    DistributedStorageManagerServer(PDBLoggerPtr logger, ConfigurationPtr conf, std::shared_ptr<StatisticsDB> statisticsDB);


    DistributedStorageManagerServer(PDBLoggerPtr logger, std::shared_ptr<StatisticsDB> statisticsDB);

    ~DistributedStorageManagerServer();

    void registerHandlers(PDBServer& forMe) override;

private:

    std::shared_ptr<StatisticsDB> statisticsDB;

    std::function<void(Handle<SimpleRequestResult>, std::string)> generateAckHandler(std::vector<std::string>& success,
                                                                                     std::vector<std::string>& failures,
                                                                                     mutex& lock);
};
}


#endif  // OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERSERVER_H
