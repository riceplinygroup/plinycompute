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
#ifndef OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERCLIENT_CC
#define OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERCLIENT_CC

#include "DistributedStorageManagerClient.h"

#include "SimpleRequest.h"
#include "DistributedStorageAddDatabase.h"
#include "DistributedStorageAddSet.h"
#include "DistributedStorageAddTempSet.h"
#include "DistributedStorageRemoveDatabase.h"
#include "DistributedStorageRemoveSet.h"
#include "DistributedStorageRemoveTempSet.h"
#include "DistributedStorageExportSet.h"
#include "DistributedStorageClearSet.h"
#include "DistributedStorageCleanup.h"

namespace pdb {

DistributedStorageManagerClient::DistributedStorageManagerClient(int portIn,
                                                                 std::string addressIn,
                                                                 PDBLoggerPtr myLoggerIn)
    : port(portIn), address(addressIn), logger(myLoggerIn) {
    // no-op
}

DistributedStorageManagerClient::~DistributedStorageManagerClient() {
    // no-op
}

void DistributedStorageManagerClient::registerHandlers(PDBServer& forMe) {
    // no-op
}

bool DistributedStorageManagerClient::createDatabase(const std::string& databaseName,
                                                     std::string& errMsg) {
    return simpleRequest<DistributedStorageAddDatabase, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,
        generateResponseHandler("Could not add database to distributed storage manager", errMsg),
        databaseName);
}

bool DistributedStorageManagerClient::createSet(const std::string& databaseName,
                                                const std::string& setName,
                                                const std::string& typeName,
                                                std::string& errMsg,
                                                size_t pageSize) {
    return simpleRequest<DistributedStorageAddSet, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,
        generateResponseHandler("Could not add set to distributed storage manager:", errMsg),
        databaseName,
        setName,
        typeName,
        pageSize);
}

bool DistributedStorageManagerClient::createTempSet(const std::string& databaseName,
                                                    const std::string& setName,
                                                    const std::string& typeName,
                                                    std::string& errMsg,
                                                    size_t pageSize) {
    return simpleRequest<DistributedStorageAddTempSet, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,

        generateResponseHandler("Could not add temp set to distributed storage manager:", errMsg),
        databaseName,
        setName,
        typeName,
        pageSize);
}

bool DistributedStorageManagerClient::removeDatabase(const std::string& databaseName,
                                                     std::string& errMsg) {
    return simpleRequest<DistributedStorageRemoveDatabase, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,
        generateResponseHandler("Could not remove database from distributed storage manager",
                                errMsg),
        databaseName);
}

bool DistributedStorageManagerClient::removeSet(const std::string& databaseName,
                                                const std::string& setName,
                                                std::string& errMsg) {
    return simpleRequest<DistributedStorageRemoveSet, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,
        generateResponseHandler("Could not remove set to distributed storage manager", errMsg),
        databaseName,
        setName);
}

bool DistributedStorageManagerClient::removeTempSet(const std::string& databaseName,
                                                    const std::string& setName,
                                                    const std::string& typeName,
                                                    std::string& errMsg) {
    return simpleRequest<DistributedStorageRemoveTempSet, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,
        generateResponseHandler("Could not remove temp set to distributed storage manager", errMsg),
        databaseName,
        setName,
        typeName);
}


bool DistributedStorageManagerClient::exportSet(const std::string& databaseName,
                                                const std::string& setName,
                                                const std::string& outputFilePath,
                                                const std::string& format,
                                                std::string& errMsg) {
    return simpleRequest<DistributedStorageExportSet, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,
        generateResponseHandler("Could not export set", errMsg),
        databaseName,
        setName,
        outputFilePath,
        format);
}


bool DistributedStorageManagerClient::clearSet(const std::string& databaseName,
                                               const std::string& setName,
                                               const std::string& typeName,
                                               std::string& errMsg) {
    return simpleRequest<DistributedStorageClearSet, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,
        generateResponseHandler("Could not clear set to distributed storage manager", errMsg),
        databaseName,
        setName,
        typeName);
}


bool DistributedStorageManagerClient::flushData(std::string& errMsg) {
    return simpleRequest<DistributedStorageCleanup, SimpleRequestResult, bool>(
        logger,
        port,
        address,
        false,
        1024,
        generateResponseHandler("Could not cleanup buffered records in distributed storage servers",
                                errMsg));
}


std::function<bool(Handle<SimpleRequestResult>)>
DistributedStorageManagerClient::generateResponseHandler(std::string description,
                                                         std::string& errMsg) {
    return [&](Handle<SimpleRequestResult> result) {
        if (result != nullptr) {
            if (!result->getRes().first) {
                errMsg = description + result->getRes().second;
                logger->error(description + ": " + result->getRes().second);
                return false;
            }
            return true;
        }
        errMsg = "Received nullptr as response";
        logger->error(description + ": received nullptr as response");
        return false;
    };
}
}

#endif
