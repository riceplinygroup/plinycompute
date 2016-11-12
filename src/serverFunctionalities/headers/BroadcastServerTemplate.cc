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
#ifndef OBJECTQUERYMODEL_BROADCASTSERVERTEMPLATE_CC
#define OBJECTQUERYMODEL_BROADCASTSERVERTEMPLATE_CC

#include "BroadcastServer.h"
#include "ResourceManagerServer.h"
#include "PDBWorker.h"
#include "PDBWork.h"
#include "GenericWork.h"
#include "UseTemporaryAllocationBlock.h"

namespace pdb {

template <class MsgType, class PayloadType, class ResponseType>
void BroadcastServer::broadcast(Handle<MsgType> broadcastMsg, Handle<Vector<Handle<PayloadType>>> broadcastData,
        std::vector<std::string> receivers,
        std::function<void(Handle<ResponseType>, std::string)> callBack,
        std::function<void(std::string, std::string)> errorCallBack) {
    int errors = 0;
    int success = 0;
    auto failures = make_shared<std::vector<std::pair<std::string, std::string>>>();

    PDBBuzzerPtr buzzer = make_shared<PDBBuzzer> (
        [&] (PDBAlarm myAlarm, std::string serverName) {
            lock.lock();
            // Handle the error cases here
            if (myAlarm == PDBAlarm::GenericError) {
                errors ++;
                std::cout << "Error broadcast " << errors << " to " << serverName << std::endl;
                lock.unlock();
            } else {
                success++;
                std::cout << "Successful broadcast " << success << " to " << serverName << std::endl;
                lock.unlock();
            }
        }
    );

    for (int i = 0; i < receivers.size(); i++) {
        PDBWorkerPtr myWorker = getWorker();

        std::string serverName = receivers[i];
        int port;
        std::string address;

        std::cout << "Broadcasting to " << serverName << std::endl;

        size_t pos = serverName.find(":");
        if (pos != string::npos) {
            port = stoi(serverName.substr(pos + 1, serverName.size()));
            address = serverName.substr(0, pos);
        } else {
            port = 8108;
            address = serverName;
        }

        PDBWorkPtr myWork = make_shared <GenericWork> ([&, i, serverName, port, address] (PDBBuzzerPtr callerBuzzer) {
            std :: cout << "the " << i << "-th thread is started" << std :: endl;
            std::string errMsg;
            PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();
            std :: cout << i << ":port = " << port << std :: endl;
            std :: cout << i << ":address = " << address << std :: endl; 
            if(communicator->connectToInternetServer(this->logger, port, address, errMsg)) {
                std :: cout << i << ":connectToInternetServer: " << errMsg << std :: endl;
                errorCallBack(errMsg, serverName);
                callerBuzzer->buzz (PDBAlarm :: GenericError, serverName);
                return;
            }
            Handle<MsgType> broadcastMsgCopy = deepCopy<MsgType>(broadcastMsg);
            if (!communicator->sendObject<MsgType>(broadcastMsgCopy, errMsg)) {
                std :: cout << i << ":sendObject: " << errMsg << std :: endl;
                errorCallBack(errMsg, serverName);
                callerBuzzer->buzz (PDBAlarm :: GenericError, serverName);
                return;
            }
            if (broadcastData != nullptr) {
                Handle<Vector<Handle<PayloadType>>> payloadCopy = deepCopy<Vector<Handle<PayloadType>>> (broadcastData);
                if (!communicator->sendObject(payloadCopy, errMsg)) {
                    std :: cout << i << ":sendBytes: " << errMsg << std :: endl;
                    errorCallBack(errMsg, serverName);
                    callerBuzzer->buzz (PDBAlarm :: GenericError, serverName);
                    return;
                }
            }
            const UseTemporaryAllocationBlock myBlock{communicator->getSizeOfNextObject()};
            bool err;
            Handle<ResponseType> result = communicator->getNextObject<ResponseType>(err, errMsg);
            callBack(result, serverName);
            std :: cout << "the " << i << "-th thread finished" << std :: endl;
            callerBuzzer->buzz(PDBAlarm :: WorkAllDone, serverName);
        });
        myWorker->execute(myWork, buzzer);
    }
    while(errors + success < receivers.size()) {
        buzzer->wait();
    }
    std :: cout << "all broadcasting thread returns" << std :: endl;
}

template<class DataType>
Handle<DataType> BroadcastServer::deepCopy(const Handle<DataType> & original) {
    Handle<DataType> newObject = makeObject<DataType>();
    (* newObject) = (* original);
    return newObject;
}

}


#endif
