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
#ifndef OBJECTQUERYMODEL_BROADCASTSERVER_H
#define OBJECTQUERYMODEL_BROADCASTSERVER_H

#include "ServerFunctionality.h"

#include <mutex>

namespace pdb {

/**
 * An abstract class for ServerFunctionality that allows for the functionality to broadcast messages asynchronously.
 */
    class BroadcastServer : public ServerFunctionality {

    public:

        BroadcastServer(PDBLoggerPtr logger);
        ~BroadcastServer();

        void registerHandlers (PDBServer &forMe) override; // no-op

        /**
         * A broadcast sends out a PDBObject along with an optional Vector<PDBObject> of data to a group of nodes. It will
         * dispatch each of these messages in a seperate thread. Each thread will then call the proper callback, on either
         * success or failure, when it receives a response. This function will block until all responses are received,
         * either as successes or failures.
         *
         * @param broadcastMsg Initial PDBObject to send
         * @param broadCastData Additional PDBObject data to send
         * @param receivers Vector of "ip:host" of the nodes to receive the broadcast.
         * @param successCallBack Callback to be called on receiving a successful response from a receiver. Takes a response
         *                        and "ip:host" as arguments
         * @param errorCallBack Callback to be called on receiving a erroneous response from a receiver. Takes an error msg
         *                      and "ip:host" as arguments
         */
        template <class MsgType, class PayloadType, class ResponseType>
        void broadcast(Handle<MsgType> broadcastMsg, Handle<Vector<Handle<PayloadType>>> broadCastData,
                std::vector<std::string> receivers,
                std::function<void(Handle<ResponseType>, std::string)> successCallBack,
                std::function<void(std::string, std::string)> errorCallBack = [] (std::string errMsg, std::string serverName) {/* no-op */ });

    private:

        /**
         * Performs a deep copy on a PDBObject
         *
         * @param original the original PDBObject
         * @return a deep copy of the PDBOBject
         */
        template<class DataType>
        Handle<DataType> deepCopy(const Handle<DataType> & original);

        PDBLoggerPtr logger;
        std::mutex lock;
    };
}

#include "BroadcastServerTemplate.cc"

#endif //OBJECTQUERYMODEL_BROADCASTSERVER_H
