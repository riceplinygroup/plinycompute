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
#ifndef LA_RUNING_ENVIRONMENT_H
#define LA_RUNING_ENVIRONMENT_H

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>
#include <set>
#include <map>

#include "PDBDebug.h"
#include "PDBString.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "Set.h"
#include "DataTypes.h"
#include "LADimension.h"

// by Binhang, June 2017

class LAPDBInstance {

private:
    bool printResult;
    bool clusterMode;
    size_t blockSize;
    std::string managerIP;
    int port;
    pdb::PDBLoggerPtr clientLogger;
    pdb::DistributedStorageManagerClient storageClient;
    pdb::CatalogClient catalogClient;
    pdb::DispatcherClient dispatcherClient;
    pdb::QueryClient queryClient;

    std::string errMsg;

    int dispatchCount = 0;

    std::set<std::string> cachedSet;
    std::map<std::string, std::string> identifierPDBSetNameMap;
    std::map<std::string, LADimension> identifierDimensionMap;

public:
    LAPDBInstance(bool printResultIn,
                  bool clusterModeIn,
                  size_t blockSizeIn,
                  std::string managerIPIn,
                  int portIn,
                  pdb::PDBLoggerPtr loggerIn)
        : printResult(printResultIn),
          clusterMode(clusterModeIn),
          blockSize(blockSizeIn),
          managerIP(managerIPIn),
          port(portIn),
          clientLogger(loggerIn),
          storageClient(port, managerIP, clientLogger),
          catalogClient(port, managerIP, clientLogger),
          dispatcherClient(port, managerIP, clientLogger),
          queryClient(port, managerIP, clientLogger, true) {
        // Register libraries;
        catalogClient.registerType("libraries/libLADuplicateColMultiSelection.so", errMsg);
        catalogClient.registerType("libraries/libLADuplicateRowMultiSelection.so", errMsg);
        catalogClient.registerType("libraries/libLAInverse1Aggregate.so", errMsg);
        catalogClient.registerType("libraries/libLAInverse2Selection.so", errMsg);
        catalogClient.registerType("libraries/libLAInverse3MultiSelection.so", errMsg);
        catalogClient.registerType("libraries/libLAMaxElementOutputType.so", errMsg);
        catalogClient.registerType("libraries/libLAMaxElementValueType.so", errMsg);
        catalogClient.registerType("libraries/libLAMinElementOutputType.so", errMsg);
        catalogClient.registerType("libraries/libLAMinElementValueType.so", errMsg);
        catalogClient.registerType("libraries/libLAScanMatrixBlockSet.so", errMsg);
        catalogClient.registerType("libraries/libLAAddJoin.so", errMsg);
        catalogClient.registerType("libraries/libLARowMaxAggregate.so", errMsg);
        catalogClient.registerType("libraries/libLARowMinAggregate.so", errMsg);
        catalogClient.registerType("libraries/libLARowSumAggregate.so", errMsg);
        catalogClient.registerType("libraries/libLAColMaxAggregate.so", errMsg);
        catalogClient.registerType("libraries/libLAColMinAggregate.so", errMsg);
        catalogClient.registerType("libraries/libLAColSumAggregate.so", errMsg);
        catalogClient.registerType("libraries/libLASubstractJoin.so", errMsg);
        catalogClient.registerType("libraries/libLAScaleMultiplyJoin.so", errMsg);
        catalogClient.registerType("libraries/libLAMaxElementAggregate.so", errMsg);
        catalogClient.registerType("libraries/libLAMinElementAggregate.so", errMsg);
        catalogClient.registerType("libraries/libLAMultiply1Join.so", errMsg);
        catalogClient.registerType("libraries/libLAMultiply2Aggregate.so", errMsg);
        catalogClient.registerType("libraries/libLATransposeMultiply1Join.so", errMsg);
        catalogClient.registerType("libraries/libLATransposeSelection.so", errMsg);
        catalogClient.registerType("libraries/libLAWriteMatrixBlockSet.so", errMsg);
        catalogClient.registerType("libraries/libLAWriteMaxElementSet.so", errMsg);
        catalogClient.registerType("libraries/libLAWriteMinElementSet.so", errMsg);
        catalogClient.registerType("libraries/libMatrixMeta.so", errMsg);
        catalogClient.registerType("libraries/libMatrixData.so", errMsg);
        catalogClient.registerType("libraries/libMatrixBlock.so", errMsg);
        catalogClient.registerType("libraries/libLASingleMatrix.so", errMsg);

        if (!storageClient.createDatabase("LA_db", errMsg)) {
            std::cout << "Not able to create database: " + errMsg;
            exit(-1);
        } else {
            std::cout << "Created database <LA_db>.\n";
        }
    }


    pdb::DistributedStorageManagerClient& getStorageClient() {
        return this->storageClient;
    }

    pdb::DispatcherClient& getDispatchClient() {
        return this->dispatcherClient;
    }

    pdb::QueryClient& getQueryClient() {
        return this->queryClient;
    }

    void increaseDispatchCount() {
        this->dispatchCount += 1;
    }

    int getDispatchCount() {
        return dispatchCount;
    }

    size_t getBlockSize() {
        return blockSize;
    }

    std::string& instanceErrMsg() {
        return errMsg;
    }

    void addToCachedSet(std::string setName) {
        cachedSet.insert(setName);
    }

    void deleteFromCachedSet(std::string setName) {
        cachedSet.erase(setName);
    }

    bool existsPDBSet(std::string setName) {
        return cachedSet.find(setName) != cachedSet.end();
    }

    void addToIdentifierPDBSetNameMap(std::string identiferName, std::string scanSetName) {
        if (identifierPDBSetNameMap.find(identiferName) != identifierPDBSetNameMap.end()) {
            identifierPDBSetNameMap.erase(identiferName);
        }
        identifierPDBSetNameMap.insert(
            std::pair<std::string, std::string>(identiferName, scanSetName));
    }

    bool existsPDBSetForIdentifier(std::string identiferName) {
        return identifierPDBSetNameMap.find(identiferName) != identifierPDBSetNameMap.end();
    }

    std::string getPDBSetNameForIdentifier(std::string identiferName) {
        return identifierPDBSetNameMap[identiferName];
    }

    void addToIdentifierDimensionMap(std::string identiferName, LADimension dim) {
        if (identifierDimensionMap.find(identiferName) != identifierDimensionMap.end()) {
            identifierDimensionMap.erase(identiferName);
        }
        identifierDimensionMap.insert(std::pair<std::string, LADimension>(identiferName, dim));
    }

    bool existsDimension(std::string identiferName) {
        return identifierDimensionMap.find(identiferName) != identifierDimensionMap.end();
    }

    LADimension findDimension(std::string identiferName) {
        return identifierDimensionMap[identiferName];
    }

    void clearCachedSets() {
        std::cout << "Clear all the set in LA_db." << std::endl;
        for (auto const& setName : cachedSet) {
            if (!storageClient.removeSet("LA_db", setName, errMsg)) {
                std::cout << "Not able to remove set: " + errMsg << std::endl;
                exit(-1);
            } else {
                std::cout << "Set " << setName << " removed" << std::endl;
            }
        }
        cachedSet.clear();
    }
};

#endif
