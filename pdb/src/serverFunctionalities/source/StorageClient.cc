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

#ifndef STORAGE_CLIENT_CC
#define STORAGE_CLIENT_CC

#include "StorageClient.h"
#include "StorageAddDatabase.h"
#include "StorageCleanup.h"
namespace pdb {

StorageClient::StorageClient(int portIn,
                             std::string addressIn,
                             PDBLoggerPtr myLoggerIn,
                             bool usePangeaIn)
    : myHelper(portIn, addressIn, myLoggerIn) {

    // get the communication information
    port = portIn;
    address = addressIn;
    myLogger = myLoggerIn;
    usePangea = usePangeaIn;
}

void StorageClient::registerHandlers(PDBServer& forMe) { /* no handlers for a storage client!! */
}

bool StorageClient::registerType(std::string regMe, std::string& errMsg) {
    return myHelper.registerType(regMe, errMsg);
}

bool StorageClient::shutDownServer(std::string& errMsg) {

    return myHelper.shutDownServer(errMsg);
}

bool StorageClient::flushData(std::string& errMsg) {
    return simpleRequest<StorageCleanup, SimpleRequestResult, bool>(
        myLogger, port, address, false, 1024, [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error cleanup buffered records in storage server: " +
                        result->getRes().second;
                    myLogger->error("Error cleanup buffered records in storage server: " +
                                    result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error cleanup buffered records in storage server";
            return false;
        });
}


bool StorageClient::createDatabase(std::string databaseName, std::string& errMsg) {
    if (usePangea == false) {
        return myHelper.createDatabase(databaseName, errMsg);
    } else {
        return simpleRequest<StorageAddDatabase, SimpleRequestResult, bool>(
            myLogger,
            port,
            address,
            false,
            1024,
            [&](Handle<SimpleRequestResult> result) {
                if (result != nullptr) {
                    if (!result->getRes().first) {
                        errMsg = "Error creating database: " + result->getRes().second;
                        myLogger->error("Error creating database: " + result->getRes().second);
                        return false;
                    }
                    return true;
                }
                errMsg = "Error getting type name, nothing is back from storage";
                return false;
            },
            databaseName);
    }
}

bool StorageClient::storeData(Handle<Vector<Handle<Object>>> data,
                              std::string databaseName,
                              std::string setName,
                              std::string typeName,
                              std::string& errMsg) {
    return simpleSendDataRequest<StorageAddData, Handle<Object>, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr)
                if (!result->getRes().first) {
                    myLogger->error("Error sending data: " + result->getRes().second);
                    errMsg = "Error sending data: " + result->getRes().second;
                }
            return true;
        },
        data,
        databaseName,
        setName,
        typeName,
        true);
}


bool StorageClient::storeData(Handle<Vector<Handle<Object>>> data,
                              std::string databaseName,
                              std::string setName,
                              std::string typeName,
                              std::string& errMsg,
                              PDBCommunicator temp) {
    return simpleSendDataRequest<StorageAddData, Handle<Object>, SimpleRequestResult, bool>(
        temp,
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr)
                if (!result->getRes().first) {
                    myLogger->error("Error sending data: " + result->getRes().second);
                    errMsg = "Error sending data: " + result->getRes().second;
                }
            return true;
        },
        data,
        databaseName,
        setName,
        typeName,
        true);
}

std::string StorageClient::getObjectType(std::string databaseName,
                                         std::string setName,
                                         std::string& errMsg) {
    return myHelper.getObjectType(databaseName, setName, errMsg);
}
}
#endif
