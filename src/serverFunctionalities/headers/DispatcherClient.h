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
//
// Created by Joseph Hwang on 9/22/16.
//

#ifndef OBJECTQUERYMODEL_DISPATCHERCLIENT_H
#define OBJECTQUERYMODEL_DISPATCHERCLIENT_H

#include "ServerFunctionality.h"
#include "Handle.h"
#include "PDBVector.h"
#include "PDBObject.h"
#include "PartitionPolicy.h"
#include "CatalogClient.h"

namespace pdb {

class DispatcherClient : public ServerFunctionality {

public:

    DispatcherClient(int portIn, std :: string addressIn, PDBLoggerPtr myLoggerIn);
    ~DispatcherClient();

    /**
     *
     * @param forMe
     */
    void registerHandlers (PDBServer &forMe) override; // no-op

    /**
     *
     * @param setAndDatabase
     * @return
     */
    bool registerSet(std::pair<std::string, std::string> setAndDatabase, PartitionPolicy::Policy policy, std::string& errMsg);

    /**
     *
     * @param setAndDatabase
     * @return
     */
    template <class DataType>
    bool sendData(std::pair<std::string, std::string> setAndDatabase, Handle<Vector<Handle<DataType>>> dataToSend, std::string& errMsg);

    template <class DataType>
    bool sendBytes(std::pair<std::string, std::string> setAndDatabase, char * bytes, size_t numBytes, std::string& errMsg);

private:
    CatalogClient myHelper;
    int port;
    std :: string address;
    PDBLoggerPtr logger;

};
}

#include "DispatcherClientTemplate.cc"

#endif //OBJECTQUERYMODEL_DISPATCHERCLIENT_H
