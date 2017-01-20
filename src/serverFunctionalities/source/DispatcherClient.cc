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
#ifndef OBJECTQUERYMODEL_DISPATCHERCLIENT_CC
#define OBJECTQUERYMODEL_DISPATCHERCLIENT_CC

#include "DispatcherClient.h"
#include "SimpleRequest.h"
#include "DispatcherRegisterPartitionPolicy.h"

namespace pdb {

    DispatcherClient::DispatcherClient(int portIn, std :: string addressIn, PDBLoggerPtr myLoggerIn) : myHelper (portIn, addressIn, myLoggerIn)  {
   this->logger = myLoggerIn;
   this->port = portIn;
   this->address = addressIn;
}

    DispatcherClient::~DispatcherClient() {}

    void DispatcherClient::registerHandlers (PDBServer &forMe) {} // no-op

    bool DispatcherClient::registerSet(std::pair<std::string, std::string> setAndDatabase, PartitionPolicy::Policy policy, std::string& errMsg) {

        return simpleRequest <DispatcherRegisterPartitionPolicy, SimpleRequestResult, bool> (logger, port, address, false, 1024,
            [&] (Handle <SimpleRequestResult> result) {
                if (result != nullptr) {
                    if (!result->getRes ().first) {
                        errMsg = "Error registering partition policy for " + setAndDatabase.first + ":" +
                                setAndDatabase.second + ": " + result->getRes ().second;
                        logger->error(errMsg);
                        return false;
                    }
                    return true;
                }
                errMsg = "Error registering partition policy: got nothing back from the DispatcherServer";
                return false;}, setAndDatabase.first, setAndDatabase.second, policy);
    }
}

#include "StorageClientTemplate.cc"

#endif

