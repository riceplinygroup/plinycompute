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

#include "PDBDebug.h"
#include "BroadcastServer.h"
#include "ResourceManagerServer.h"
#include "PDBWorker.h"
#include "PDBWork.h"
#include "GenericWork.h"
#include "UseTemporaryAllocationBlock.h"


#ifndef MAX_RETRIES
    #define MAX_RETRIES 5
#endif
#ifndef HEADER_SIZE
    #define HEADER_SIZE 20
#endif

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
                PDB_COUT << "Successful broadcast " << success << " to " << serverName << std::endl;
                lock.unlock();
            }
        }
    );

    for (int i = 0; i < receivers.size(); i++) {
        PDBWorkerPtr myWorker = getWorker();

        std::string serverName = receivers[i];
        int port;
        std::string address;

        PDB_COUT << "Broadcasting to " << serverName << std::endl;

        size_t pos = serverName.find(":");
        if (pos != string::npos) {
            port = stoi(serverName.substr(pos + 1, serverName.size()));
            address = serverName.substr(0, pos);
        } else {
            if (conf != nullptr) {
                port = conf->getPort();
            } else {
                port = 8108;
            }
            address = serverName;
        }

        PDBWorkPtr myWork = make_shared <GenericWork> ([i, serverName, port, address, this, &errorCallBack, &broadcastMsg, &broadcastData, &callBack] (PDBBuzzerPtr callerBuzzer) {
            PDB_COUT << "the " << i << "-th thread is started" << std :: endl;

            int retries = 0;
            
            while (retries <= MAX_RETRIES) {

                std::string errMsg;
                int portNumber = port;
                std :: string serverAddress = address;

                //socket() is not thread-safe, so we need synchronize here
                pthread_mutex_lock(&connection_mutex);
                PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();
                PDB_COUT << i << ":port = " << portNumber << std :: endl;
                PDB_COUT << i << ":address = " << serverAddress << std :: endl; 
                if(communicator->connectToInternetServer(this->logger, portNumber, serverAddress, errMsg)) {
                     PDB_COUT << i << ":connectToInternetServer: " << errMsg << std :: endl;
                    if (retries < MAX_RETRIES) {
                        retries ++;
                        PDB_COUT << "to retry to resend message" << std :: endl;
                        pthread_mutex_unlock(&connection_mutex);
                        continue;
                    } else {
                        pthread_mutex_unlock(&connection_mutex);
                        errorCallBack(errMsg, serverName);
                        callerBuzzer->buzz (PDBAlarm :: GenericError, serverName);
                        return;
                    }
                }
                pthread_mutex_unlock(&connection_mutex);
                PDB_COUT << i << ":connected to server: " << serverAddress << std :: endl;
                Handle<MsgType> broadcastMsgCopy = deepCopyToCurrentAllocationBlock<MsgType>(broadcastMsg);
                if (!communicator->sendObject<MsgType>(broadcastMsgCopy, errMsg)) {
                    PDB_COUT << i << ":sendObject: " << errMsg << std :: endl;
                    if (retries < MAX_RETRIES) {
                        retries ++;
                        PDB_COUT << "to retry to resend message" << std :: endl;
                        continue;
                    } else {
                        errorCallBack(errMsg, serverName);
                        callerBuzzer->buzz (PDBAlarm :: GenericError, serverName);
                        return;
                    }
                }
                PDB_COUT << i << ":send object to server: " << serverAddress << std :: endl;
                if (broadcastData != nullptr) {
                    Handle<Vector<Handle<PayloadType>>> payloadCopy = deepCopyToCurrentAllocationBlock<Vector<Handle<PayloadType>>> (broadcastData);
                    if (!communicator->sendObject(payloadCopy, errMsg)) {
                        PDB_COUT << i << ":sendBytes: " << errMsg << std :: endl;
                        if (retries < MAX_RETRIES) {
                            retries ++;
                            PDB_COUT << "to retry to resend message" << std :: endl;
                            continue;
                        } else {
                            errorCallBack(errMsg, serverName);
                            callerBuzzer->buzz (PDBAlarm :: GenericError, serverName);
                            return;
                        }
                    }
                }

                size_t objectSize = communicator->getSizeOfNextObject();  
                if (objectSize <  HEADER_SIZE) {
                    std :: cout << "received size is too small for an object" << std :: endl;
                    if (retries < MAX_RETRIES) {
                        retries ++;
                        PDB_COUT << "to retry to resend message" << std :: endl;
                        continue;
                    } else {
                        errorCallBack(errMsg, serverName);
                        callerBuzzer->buzz (PDBAlarm :: GenericError, serverName);
                        return;
                    }
                }       
                const UseTemporaryAllocationBlock myBlock{objectSize};
                bool err;
                Handle<ResponseType> result = communicator->getNextObject<ResponseType>(err, errMsg);
                if (result == nullptr) {
                     PDB_COUT << "the " << i << "-th thread connection closed unexpectedly" << std :: endl;
                    if (retries < MAX_RETRIES) {
                        retries ++;
                        PDB_COUT << "to retry to resend message" << std :: endl;
                        continue;
                    } else {
                        errorCallBack(errMsg, serverName);
                        callerBuzzer->buzz(PDBAlarm::GenericError, serverName);
                        return;
                    }
                }
                callBack(result, serverName);
                PDB_COUT << "the " << i << "-th thread finished" << std :: endl;
                callerBuzzer->buzz(PDBAlarm :: WorkAllDone, serverName);
                return;
            }
        });
        myWorker->execute(myWork, buzzer);
    }
    while(errors + success < receivers.size()) {
        buzzer->wait();
    }
    PDB_COUT << "all broadcasting thread returns" << std :: endl;
}


template<class DataType>
Handle<DataType> BroadcastServer::deepCopy(const Handle<DataType> & original) {
    Handle<DataType> newObject = makeObject<DataType>();
    (* newObject) = (* original);
    return newObject;
}

}


#endif
