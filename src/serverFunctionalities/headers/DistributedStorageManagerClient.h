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
#ifndef OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERCLIENT_H
#define OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERCLIENT_H

#include "ServerFunctionality.h"
#include "PDBLogger.h"

#include "SimpleRequestResult.h"

namespace pdb {


// This functionality talks with the DistributedStorageManagerServer instance
// to execute storage-related user requests.


class DistributedStorageManagerClient : public ServerFunctionality {

public:
    DistributedStorageManagerClient();
    DistributedStorageManagerClient(int portIn, std::string addressIn, PDBLoggerPtr myLoggerIn);
    ~DistributedStorageManagerClient();

    void registerHandlers(PDBServer& forMe);  // no - op


    // TODO: Allow the user to specify what partitioning policy they want to use for this step in
    // the future
    bool createDatabase(const std::string& databaseName, std::string& errMsg);

    bool createSet(const std::string& databaseName,
                   const std::string& setName,
                   const std::string& typeName,
                   std::string& errMsg,
                   size_t pageSize = DEFAULT_PAGE_SIZE);

    // create a temp set that only goes through storage
    bool createTempSet(const std::string& databaseName,
                       const std::string& setName,
                       const std::string& typeName,
                       std::string& errMsg,
                       size_t pageSize = DEFAULT_PAGE_SIZE);

    // templated createSet
    template <class DataType>
    bool createSet(const std::string& databaseName,
                   const std::string& setName,
                   std::string& errMsg,
                   size_t pageSize = DEFAULT_PAGE_SIZE);

    // storage cleanup to flush buffered data to disk
    bool flushData(std::string& errMsg);

    bool removeDatabase(const std::string& databaseName, std::string& errMsg);

    bool removeSet(const std::string& databaseName,
                   const std::string& setName,
                   std::string& errMsg);

    // clean up all set by removing both in-memory and on-disk data
    bool clearSet(const std::string& databaseName,
                  const std::string& setName,
                  const std::string& typeName,
                  std::string& errMsg);

    // remove a temp set that only goes through storage
    bool removeTempSet(const std::string& databaseName,
                       const std::string& setName,
                       const std::string& typeName,
                       std::string& errMsg);

    // export set to files with user specified format
    // the exported set is partitioned over all storage nodes on the specified directory
    // Note that the objects in set must be instances of ExportableObject
    bool exportSet(const std::string& databaseName,
                   const std::string& setName,
                   const std::string& outputFilePath,
                   const std::string& format,
                   std::string& errMsg);


private:
    std::function<bool(Handle<SimpleRequestResult>)> generateResponseHandler(
        std::string description, std::string& errMsg);

    int port;
    std::string address;
    PDBLoggerPtr logger;
};
}

#include "DistributedStorageManagerClientTemplate.cc"
#endif  // OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERCLIENT_H
