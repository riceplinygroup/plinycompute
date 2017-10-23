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
#ifndef PDB_CLIENT_TEMPLATE_CC
#define PDB_CLIENT_TEMPLATE_CC

#include "PDBClient.h"

namespace pdb {

    template <class DataType>
    bool PDBClient::createSet(
            const std::string& databaseName,
            const std::string& setName,
            std::string& errMsg) {

        return distributedStorageClient.createSet<DataType>(databaseName, setName,
                                  errMsg, DEFAULT_PAGE_SIZE);
    }

    template <class DataType>
    bool PDBClient::sendData(
            std::pair<std::string, std::string> setAndDatabase,
            Handle<Vector<Handle<DataType>>> dataToSend,
            std::string& errMsg) {

        return dispatcherClient.sendData<DataType>(setAndDatabase, dataToSend, errMsg);
    }


    template <class... Types>
    bool PDBClient::executeComputations(std::string& errMsg,
                             Handle<Computation> firstParam,
                             Handle<Types>... args) {
        return queryClient.executeComputations(errMsg, firstParam, args...);
    }

    template <class Type>
    SetIterator<Type> PDBClient::getSetIterator(std::string databaseName, std::string setName){
        return queryClient.getSetIterator<Type>(databaseName, setName);
    }



}
#endif
