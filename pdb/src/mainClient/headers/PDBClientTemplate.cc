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
    void PDBClient::createSet(const std::string &databaseName,
                              const std::string &setName, size_t pageSize) {

      string errMsg;

      bool result = distributedStorageClient.createSet<DataType>(
            databaseName, setName, errMsg, pageSize);

      if (result==false) {
          std::cout << "Not able to create set: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Created set.\n";
      }
    }

    template <class DataType>
    void PDBClient::createSet(const std::string &databaseName,
                              const std::string &setName) {

      string errMsg;

      bool result = distributedStorageClient.createSet<DataType>(
          databaseName, setName, errMsg, DEFAULT_PAGE_SIZE);

      if (result==false) {
          std::cout << "Not able to create set: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Created set.\n";
      }
    }

    template <class DataType>
    void PDBClient::sendData(std::pair<std::string, std::string> setAndDatabase,
                             Handle<Vector<Handle<DataType>>> dataToSend) {

      string errMsg;

      bool result = dispatcherClient.sendData<DataType>(setAndDatabase, dataToSend,
                                                 errMsg);

      if (result==false) {
          std::cout << "Not able to send data: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Data sent.\n";
      }
    }

    template <class DataType>
    void PDBClient::sendBytes(std::pair<std::string, std::string> setAndDatabase,
                   char* bytes,
                   size_t numBytes){

      string errMsg;

      bool result = dispatcherClient.sendBytes<DataType>(
                setAndDatabase,
                bytes,
                numBytes,
                errMsg);

        if (result==false) {
            std::cout << "Not able to send bytes: " + errMsg;
            exit(-1);
        } else {
            std::cout << "Bytes sent.\n";
        }
    }

    template <class... Types>
    void PDBClient::executeComputations(Handle<Computation> firstParam,
                                        Handle<Types>... args) {
      string errMsg;

      bool result = queryClient.executeComputations(errMsg, firstParam, args...);

      if (result==false) {
          std::cout << "Not able to execute computations: " + errMsg;
          exit(-1);
      }
    }

    template <class Type>
    SetIterator<Type> PDBClient::getSetIterator(std::string databaseName,
                                                std::string setName) {

      return queryClient.getSetIterator<Type>(databaseName, setName);
    }
}
#endif
