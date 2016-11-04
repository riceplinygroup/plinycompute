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

    class DistributedStorageManagerClient : public ServerFunctionality {

    public:
        DistributedStorageManagerClient(int portIn, std :: string addressIn, PDBLoggerPtr myLoggerIn);
        ~DistributedStorageManagerClient();

        void registerHandlers (PDBServer &forMe); // no - op


        // TODO: Allow the user to specify what partitioning policy they want to use for this step in the future
        bool createDatabase(const std::string& databaseName, std::string& errMsg);

        bool createSet(const std::string& databaseName, const std::string& setName, const std::string& typeName,
                       std::string& errMsg);

        bool removeDatabase(const std::string& databaseName, std::string & errMsg);

        bool removeSet(const std::string& databaseName, const std::string& setName, std::string & errMsg);


    private:
        std::function<bool (Handle<SimpleRequestResult>)> generateResponseHandler(std::string description, std::string & errMsg);

        int port;
        std :: string address;
        PDBLoggerPtr logger;

    };

}


#endif //OBJECTQUERYMODEL_DISTRIBUTEDSTORAGEMANAGERCLIENT_H
