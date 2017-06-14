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

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "Set.h"
#include "DataTypes.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>


const int portNo = 8108;

class LAPDBInstance{
/*
private:
	bool printResult = true;
    bool clusterMode = false;
    int blockSize = 64;
    std :: string masterIp = "localhost";
    int port = portNo;
    pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("clientLog");;
    pdb :: DistributedStorageManagerClient storageClient (portNo, masterIp, clientLogger);
    pdb :: CatalogClient catalogClient (portNo, masterIp, clientLogger);

    std::string errMsg;

public:

	LAPDBInstance(){
		//Register libraries;
		catalogClient.registerType ("libraries/libMatrixBlock.so", errMsg);
		catalogClient.registerType('libraries/libLAMaxElementOutputType.so', errMsg);
		catalogClient.registerType('libraries/libLAMaxElementValueType.so', errMsg);
		catalogClient.registerType('libraries/libLAMinElementOutputType.so', errMsg);
		catalogClient.registerType('libraries/libLAMinElementValueType.so', errMsg);
		catalogClient.registerType('libraries/libLAScanMatrixBlockSet.so', errMsg);
		catalogClient.registerType('libraries/libLASillyAddJoin.so', errMsg);
		catalogClient.registerType('libraries/libLASillyRowMaxAggregate.so', errMsg);
		catalogClient.registerType('libraries/libLASillyRowMinAggregate.so', errMsg);
		catalogClient.registerType('libraries/libLASillyColMaxAggregate.so', errMsg);
		catalogClient.registerType('libraries/libLASillyColMinAggregate.so', errMsg);
		catalogClient.registerType('libraries/libLASillySubstractJoin.so', errMsg);
		catalogClient.registerType('libraries/libLASillyMaxElementAggregate.so', errMsg);
		catalogClient.registerType('libraries/libLASillyMinElementAggregate.so', errMsg);
		catalogClient.registerType('libraries/libLASillyMultiply1Join.so', errMsg);
		catalogClient.registerType('libraries/libLASillyMultiply2Aggregate.so', errMsg);
		catalogClient.registerType('libraries/libLASillyTransposeMultiply1Join.so', errMsg);
		catalogClient.registerType('libraries/libLASillyTransposeSelection.so', errMsg);
		catalogClient.registerType('libraries/libLAWriteMatrixBlockSet.so', errMsg);
		catalogClient.registerType('libraries/libLAWriteMaxElementSet.so', errMsg);
		catalogClient.registerType('libraries/libLAWriteMinElementSet.so', errMsg);
		catalogClient.registerType('libraries/libMatrixMeta.so', errMsg);
		catalogClient.registerType('libraries/libMatrixData.so', errMsg);
		catalogClient.registerType('libraries/libMatrixBlock.so', errMsg);

		if (!storageClient.createDatabase ("LA_db", errMsg)) {
            std::cout << "Not able to create database: " + errMsg;
            exit (-1);
        } else {
            std::cout << "Created database.\n";
        }
	}


	void addSet(std::string setName){
		
	}
*/
};

#endif
